// Package wabridge wires the whatsmeow client to the store + media uploader.
// On every inbound message it upserts the chat row, optionally downloads and
// SFTPs media, and inserts a wa_messages row.
package wabridge

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/enviadores/wabridge/internal/config"
	"github.com/enviadores/wabridge/internal/media"
	"github.com/enviadores/wabridge/internal/pairing"
	"github.com/enviadores/wabridge/internal/store"

	_ "github.com/mattn/go-sqlite3"
	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type Bridge struct {
	cfg      *config.Config
	store    *store.Store
	pairing  *pairing.Writer
	uploader *media.Uploader
	client   *whatsmeow.Client
	logger   waLog.Logger
	fatalCh  chan error
}

func New(ctx context.Context, cfg *config.Config, st *store.Store, up *media.Uploader) (*Bridge, error) {
	level := strings.ToUpper(cfg.Whatsmeow.LogLevel)
	dbLog := waLog.Stdout("wa-db", level, true)
	clientLog := waLog.Stdout("wa-client", level, true)

	container, err := sqlstore.New(ctx, "sqlite3",
		fmt.Sprintf("file:%s?_foreign_keys=on", cfg.Whatsmeow.StorePath),
		dbLog,
	)
	if err != nil {
		return nil, fmt.Errorf("open whatsmeow sqlite store: %w", err)
	}

	deviceStore, err := container.GetFirstDevice(ctx)
	if err != nil {
		return nil, fmt.Errorf("get device: %w", err)
	}

	client := whatsmeow.NewClient(deviceStore, clientLog)

	b := &Bridge{
		cfg:      cfg,
		store:    st,
		pairing:  pairing.New(st.DB()),
		uploader: up,
		client:   client,
		logger:   clientLog,
		fatalCh:  make(chan error, 1),
	}
	client.AddEventHandler(b.handleEvent)
	return b, nil
}

// Start connects to WhatsApp. If no device session exists, it streams QR
// codes to both stdout (terminal fallback) and the wa_pairing MySQL row so
// the React app at /whatsapp can render them.
func (b *Bridge) Start(ctx context.Context) error {
	if b.client.Store.ID == nil {
		qrChan, _ := b.client.GetQRChannel(ctx)
		if err := b.client.Connect(); err != nil {
			return err
		}
		go b.runQRLoop(ctx, qrChan)
		return nil
	}
	// Already paired — mark the bus accordingly so the UI doesn't show stale
	// QR codes from a previous pairing attempt.
	if err := b.pairing.SetPaired(ctx, b.client.Store.ID.String()); err != nil {
		b.logger.Warnf("pairing.SetPaired: %v", err)
	}
	return b.client.Connect()
}

// runQRLoop forwards QR / status events from whatsmeow into wa_pairing and
// the terminal until the channel closes (either by pairing success or
// timeout).
func (b *Bridge) runQRLoop(ctx context.Context, qrChan <-chan whatsmeow.QRChannelItem) {
	for evt := range qrChan {
		switch evt.Event {
		case "code":
			b.logger.Infof("QR code emitted (valid ~20s) — visible at app.enviadores.com.mx/whatsapp")
			b.logger.Infof("Terminal fallback: %s", evt.Code)
			if err := b.pairing.SetQRCode(ctx, evt.Code, 20*time.Second); err != nil {
				b.logger.Warnf("pairing.SetQRCode: %v", err)
			}
		case "success":
			b.logger.Infof("Paired successfully")
			if b.client.Store.ID != nil {
				if err := b.pairing.SetPaired(ctx, b.client.Store.ID.String()); err != nil {
					b.logger.Warnf("pairing.SetPaired: %v", err)
				}
			}
		case "timeout":
			b.logger.Warnf("QR pairing timed out — restart wabridge or click 'Reset' in the web UI")
			_ = b.pairing.SetEvent(ctx, pairing.StatusError, "qr timeout")
		default:
			_ = b.pairing.SetEvent(ctx, pairing.StatusError, "qr event: "+evt.Event)
		}
	}
}

// WatchResetRequests polls wa_pairing for a non-NULL reset_requested_at
// every few seconds. When the web app sets that field, the bridge logs out
// the current WhatsApp session — supervise() then restarts runOnce(), which
// triggers a fresh QR pairing flow.
func (b *Bridge) WatchResetRequests(ctx context.Context) error {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			reset, err := b.pairing.ConsumeResetRequest(ctx)
			if err != nil {
				b.logger.Warnf("pairing.ConsumeResetRequest: %v", err)
				continue
			}
			if !reset {
				continue
			}
			b.logger.Infof("Reset requested from web UI — logging out current session")
			if err := b.client.Logout(ctx); err != nil {
				b.logger.Warnf("Logout: %v", err)
			}
			// Returning here propagates up to supervise(), which will
			// re-open the tunnel + bridge with a fresh QR flow.
			return fmt.Errorf("reset requested")
		}
	}
}

func (b *Bridge) Stop() {
	if b.client.IsConnected() {
		b.client.Disconnect()
	}
}

func (b *Bridge) handleEvent(evt interface{}) {
	switch v := evt.(type) {
	case *events.Connected:
		b.logger.Infof("Connected to WhatsApp")
		// Mark online so the server starts forwarding ChatPresence
		// (typing) events. This makes the session visible as
		// "online"/last-seen to all contacts — a known trade-off of
		// enabling the typing feature.
		go b.markOnline(context.Background())
		// Flush any peer_typing_until rows left over from a previous
		// session: we have no way to know if those contacts are
		// still composing.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := b.store.ClearAllPeerTyping(ctx); err != nil {
			b.logger.Warnf("ClearAllPeerTyping: %v", err)
		}
	case *events.ChatPresence:
		b.handleChatPresence(v)
	case *events.Disconnected:
		// whatsmeow auto-reconnects on transient drops; the watchdog in
		// WatchConnection() escalates to a full restart if the gap
		// outlasts whatsmeow's own retries.
		b.logger.Warnf("Disconnected from WhatsApp (whatsmeow will retry)")
	case *events.LoggedOut:
		// Terminal: the WhatsApp server has revoked our session (user
		// removed the device, security flag, etc.). whatsmeow has
		// already cleared the local device store; bounce supervise()
		// so the next runOnce() starts a fresh QR flow.
		b.logger.Errorf("Logged out: %s — triggering bridge restart", v.Reason)
		b.signalFatal(fmt.Errorf("logged out: %s", v.Reason))
	case *events.Receipt:
		b.handleReceipt(v)
	case *events.Message:
		b.handleMessage(v)
	case *events.HistorySync:
		// History sync can carry thousands of messages; offload to a
		// goroutine so we don't block subsequent live events.
		go b.handleHistorySync(v)
	}
}

// Fatal returns a channel that receives an error when the bridge needs to
// be torn down and re-started from scratch (LoggedOut, or a prolonged
// disconnect that whatsmeow's own backoff didn't recover from).
func (b *Bridge) Fatal() <-chan error { return b.fatalCh }

func (b *Bridge) signalFatal(err error) {
	select {
	case b.fatalCh <- err:
	default: // already signalled, drop
	}
}

// WatchConnection escalates a stuck disconnect to a full bridge restart.
// whatsmeow has internal reconnect logic but it can wedge after long
// network outages or remote socket resets; if IsConnected() stays false
// for more than the threshold, we return an error so supervise() can
// rebuild the tunnel + client from scratch.
func (b *Bridge) WatchConnection(ctx context.Context) error {
	const checkInterval = 30 * time.Second
	const disconnectThreshold = 5 * time.Minute
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()
	var disconnectedSince time.Time
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if b.client.IsConnected() {
				disconnectedSince = time.Time{}
				continue
			}
			if disconnectedSince.IsZero() {
				disconnectedSince = time.Now()
				continue
			}
			if time.Since(disconnectedSince) >= disconnectThreshold {
				return fmt.Errorf("disconnected for >%s — restarting", disconnectThreshold)
			}
		}
	}
}

// handleHistorySync walks the conversations + messages whatsmeow delivers
// after pair (and periodically during normal operation) and writes them
// into wa_chats / wa_messages. Media is NOT re-downloaded here — the URLs
// in history-sync envelopes are usually expired; we just persist metadata
// (mime, filename) so the UI shows "documento" etc. instead of nothing.
func (b *Bridge) handleHistorySync(evt *events.HistorySync) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	data := evt.Data
	convs := data.GetConversations()
	b.logger.Infof("History sync: type=%s progress=%d%% conversations=%d",
		data.GetSyncType().String(), data.GetProgress(), len(convs))

	totalInserted := 0
	for _, conv := range convs {
		select {
		case <-ctx.Done():
			b.logger.Warnf("History sync cancelled: %v", ctx.Err())
			return
		default:
		}

		rawChatJID, err := types.ParseJID(conv.GetID())
		if err != nil {
			b.logger.Warnf("History sync: bad JID %s: %v", conv.GetID(), err)
			continue
		}
		// Canonicalize PN → LID for the storage keys so history-sync rows
		// land on the same JID the real-time path will subsequently use.
		// Keep the raw JID for ParseWebMessage so any sender lookups inside
		// the parser see the form WhatsApp actually delivered.
		chatJID := b.canonicalChatJID(ctx, rawChatJID)
		chatJIDStr := chatJID.String()
		isGroup := chatJID.Server == types.GroupServer

		displayName := b.resolveDisplayNameForJID(ctx, chatJID, conv.GetName())
		phoneE164 := sql.NullString{}
		if chatJID.Server == types.DefaultUserServer && chatJID.User != "" {
			phoneE164 = nullString("+" + chatJID.User)
		} else if chatJID.Server == types.HiddenUserServer {
			if pn := b.resolvePNForLID(ctx, chatJID); !pn.IsEmpty() && pn.User != "" {
				phoneE164 = nullString("+" + pn.User)
			}
		}

		// Initial upsert with no last_message_* — we'll fill those after
		// inserting the conversation's messages.
		chatRow := store.Chat{
			JID:             chatJIDStr,
			DisplayName:     displayName,
			PhoneE164:       phoneE164,
			IsGroup:         isGroup,
			ProfilePicURL:   sql.NullString{},
			IncrementUnread: false, // history backfill does not bump unread
		}
		if err := b.store.UpsertChat(ctx, chatRow); err != nil {
			b.logger.Warnf("History sync UpsertChat %s: %v", chatJIDStr, err)
			continue
		}

		inserted := 0
		for _, histMsg := range conv.GetMessages() {
			webMsg := histMsg.GetMessage()
			if webMsg == nil {
				continue
			}
			parsed, err := b.client.ParseWebMessage(rawChatJID, webMsg)
			if err != nil {
				continue
			}

			msgType, body, mediaInfo := classifyMessage(parsed.Message)
			if msgType == "system" {
				continue
			}

			var mediaName, mediaMime sql.NullString
			if mediaInfo != nil {
				mediaName = nullString(mediaInfo.filename)
				mediaMime = nullString(mediaInfo.mime)
			}

			senderJID := sql.NullString{}
			if s := parsed.Info.Sender.String(); s != "" {
				senderJID = nullString(s)
			}

			m := store.Message{
				ID:            parsed.Info.ID,
				ChatJID:       chatJIDStr,
				SenderJID:     senderJID,
				FromMe:        parsed.Info.IsFromMe,
				MessageType:   msgType,
				Body:          nullString(body),
				MediaMime:     mediaMime,
				MediaFilename: mediaName,
				Timestamp:     parsed.Info.Timestamp,
				Status:        sql.NullString{String: defaultStatus(parsed.Info.IsFromMe), Valid: true},
			}
			if err := b.store.InsertMessage(ctx, m); err != nil {
				// transient row error — keep going on the next message
				continue
			}
			inserted++
		}

		if inserted > 0 {
			if err := b.store.RecomputeChatTail(ctx, chatJIDStr); err != nil {
				b.logger.Warnf("RecomputeChatTail %s: %v", chatJIDStr, err)
			}
		}
		totalInserted += inserted
	}

	b.logger.Infof("History sync: inserted %d messages across %d conversations",
		totalInserted, len(convs))
}

// resolveDisplayNameForJID is the equivalent of resolveDisplayName but for
// the history-sync path, where we have a JID + a hint name from the
// conversation envelope instead of an *events.Message.
func (b *Bridge) resolveDisplayNameForJID(ctx context.Context, jid types.JID, convName string) sql.NullString {
	if name := strings.TrimSpace(convName); name != "" {
		return nullString(name)
	}
	if jid.Server == types.GroupServer {
		return sql.NullString{}
	}
	if name := b.contactNameForJID(ctx, jid); name != "" {
		return nullString(name)
	}
	if pn := b.resolvePNForLID(ctx, jid); !pn.IsEmpty() {
		if name := b.contactNameForJID(ctx, pn); name != "" {
			return nullString(name)
		}
	}
	return sql.NullString{}
}

func (b *Bridge) handleMessage(evt *events.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Normalize the chat JID to its canonical form before any DB write so
	// LID/PN dupes can never accumulate in wa_chats again.
	canonicalChat := b.canonicalChatJID(ctx, evt.Info.Chat)
	chatJID := canonicalChat.String()
	senderJID := evt.Info.Sender.String()
	fromMe := evt.Info.IsFromMe
	timestamp := evt.Info.Timestamp

	msgType, body, mediaInfo := classifyMessage(evt.Message)
	// 'system' rows are E2E protocol envelopes (SenderKeyDistribution
	// etc.) — never user-facing. Skipping them here keeps wa_chats
	// tail metadata (last_message_at / preview / from_me) honest
	// instead of advancing to a row the UI hides anyway.
	if msgType == "system" {
		return
	}

	// Download & upload media if present.
	var (
		mediaURL       sql.NullString
		mediaMime      sql.NullString
		mediaSize      sql.NullInt64
		mediaName      sql.NullString
		mediaSHA256    sql.NullString
		mediaThumbURL  sql.NullString
	)
	if mediaInfo != nil {
		data, err := b.client.Download(ctx, mediaInfo.downloadable)
		if err != nil {
			b.logger.Warnf("Download media for %s: %v", evt.Info.ID, err)
		} else {
			publicURL, sha, err := b.uploader.Upload(data, mediaInfo.mime, mediaInfo.filename)
			if err != nil {
				b.logger.Warnf("SFTP upload for %s: %v", evt.Info.ID, err)
			} else {
				mediaURL = nullString(publicURL)
				mediaMime = nullString(mediaInfo.mime)
				mediaSize = sql.NullInt64{Int64: int64(len(data)), Valid: true}
				mediaName = nullString(mediaInfo.filename)
				mediaSHA256 = nullString(sha)

				// Inline thumbnail for the chat bubble — keeps the
				// React inbox from rendering a generic file-icon card
				// for PDFs and lets long-tail formats (HEIC, video
				// frame 0) show a preview without per-format decoders.
				if thumb, terr := b.deriveInboundThumbnail(ctx, msgType, mediaInfo, data); terr == nil && len(thumb) > 0 {
					if turl, uerr := b.uploader.UploadThumbnail(sha, thumb); uerr == nil {
						mediaThumbURL = nullString(turl)
					} else {
						b.logger.Warnf("SFTP upload thumb for %s: %v", evt.Info.ID, uerr)
					}
				}
			}
		}
	}

	// Upsert chat row first (FK target for the message).
	preview := previewFor(msgType, body, mediaInfo)
	chat := store.Chat{
		JID:                chatJID,
		DisplayName:        b.resolveDisplayName(ctx, evt),
		PhoneE164:          b.resolvePhone(ctx, evt),
		IsGroup:            evt.Info.IsGroup,
		ProfilePicURL:      b.fetchProfilePicURL(ctx, canonicalChat),
		LastMessageAt:      sql.NullTime{Time: timestamp, Valid: true},
		LastMessagePreview: nullString(preview),
		LastMessageFromMe:  fromMe,
		IncrementUnread:    !fromMe,
	}
	if err := b.store.UpsertChat(ctx, chat); err != nil {
		b.logger.Errorf("UpsertChat %s: %v", chatJID, err)
		return
	}

	msg := store.Message{
		ID:              evt.Info.ID,
		ChatJID:         chatJID,
		SenderJID:       nullString(senderJID),
		FromMe:          fromMe,
		MessageType:     msgType,
		Body:            nullString(body),
		MediaURL:        mediaURL,
		MediaMime:       mediaMime,
		MediaSize:       mediaSize,
		MediaFilename:   mediaName,
		MediaSHA256:       mediaSHA256,
		MediaThumbnailURL: mediaThumbURL,
		QuotedMessageID: sql.NullString{}, // wired in a later iteration if needed
		Timestamp:       timestamp,
		Status:          sql.NullString{String: defaultStatus(fromMe), Valid: true},
	}
	if err := b.store.InsertMessage(ctx, msg); err != nil {
		b.logger.Errorf("InsertMessage %s: %v", evt.Info.ID, err)
	}
}

// markOnline tells WhatsApp we're available. Without it the server
// won't push ChatPresence events to us. Refreshes every 5 minutes —
// whatsmeow doesn't expose the exact timeout but the protocol's
// available state is treated as ephemeral and benefits from periodic
// reassertion.
func (b *Bridge) markOnline(ctx context.Context) {
	send := func() {
		if !b.client.IsConnected() {
			return
		}
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := b.client.SendPresence(cctx, types.PresenceAvailable); err != nil {
			b.logger.Warnf("SendPresence(Available): %v", err)
		}
	}
	send()
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			send()
		}
	}
}

// handleChatPresence persists peer typing state. Composing stamps a
// future expiry on wa_chats; paused (or any other state) clears it.
// The window is short — whatsmeow refreshes composing every few seconds
// for as long as the contact is typing, so we don't need a long TTL.
func (b *Bridge) handleChatPresence(evt *events.ChatPresence) {
	const composingTTL = 15 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Canonicalize so the typing flag lands on the same wa_chats row that
	// inbound/outbound messages use.
	chatJID := b.canonicalChatJID(ctx, evt.Chat).String()
	switch evt.State {
	case types.ChatPresenceComposing:
		until := time.Now().UTC().Add(composingTTL)
		if err := b.store.SetPeerTyping(ctx, chatJID, &until); err != nil {
			b.logger.Warnf("SetPeerTyping composing %s: %v", chatJID, err)
		}
	default:
		if err := b.store.SetPeerTyping(ctx, chatJID, nil); err != nil {
			b.logger.Warnf("SetPeerTyping paused %s: %v", chatJID, err)
		}
	}
}

// handleReceipt processes delivery/read receipts for outbound messages. A
// single receipt may carry multiple message IDs; whatsmeow's Type field
// distinguishes delivered vs read (empty Type is delivered).
func (b *Bridge) handleReceipt(evt *events.Receipt) {
	if len(evt.MessageIDs) == 0 {
		return
	}
	var newStatus string
	switch evt.Type {
	case types.ReceiptTypeRead, types.ReceiptTypeReadSelf, types.ReceiptTypePlayed:
		newStatus = "read"
	case types.ReceiptTypeDelivered:
		// ReceiptTypeDelivered is the empty string in whatsmeow; this
		// case also catches receipts where the field was omitted.
		newStatus = "delivered"
	default:
		// Sender / Retry / others — not user-facing status transitions.
		return
	}
	ids := make([]string, 0, len(evt.MessageIDs))
	for _, id := range evt.MessageIDs {
		ids = append(ids, string(id))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := b.store.MarkMessagesStatus(ctx, ids, newStatus); err != nil {
		b.logger.Warnf("MarkMessagesStatus (%s, %d ids): %v", newStatus, len(ids), err)
	}
}

// resolvePNForLID returns the phone-number JID associated with an @lid
// JID, if whatsmeow has the mapping cached. Empty/zero JID means we
// don't know it yet; callers should fall back to the LID-only path.
func (b *Bridge) resolvePNForLID(ctx context.Context, lid types.JID) types.JID {
	if lid.Server != types.HiddenUserServer {
		return types.EmptyJID
	}
	pn, err := b.client.Store.LIDs.GetPNForLID(ctx, lid)
	if err != nil {
		return types.EmptyJID
	}
	return pn
}

// canonicalChatJID picks one JID per chat, so wa_chats never sprouts a
// duplicate when WhatsApp routes the same conversation through both its
// PN (<phone>@s.whatsapp.net) and LID (<id>@lid) forms.
//
// LID is WhatsApp's long-term identifier — phone-hiding by design and
// stable even if the contact changes number. We rewrite PN → LID when
// whatsmeow's LID store knows the mapping, and otherwise fall through to
// the raw JID (first-contact case where the mapping hasn't been learned
// yet). Groups and other server types are passed through unchanged.
//
// See migrations/2026_05_13_wa_chats_merge_lid_pn_dupes.sql for the
// one-shot backfill that cleaned up dupes created before this normalization
// existed.
func (b *Bridge) canonicalChatJID(ctx context.Context, raw types.JID) types.JID {
	if raw.Server != types.DefaultUserServer || raw.User == "" {
		return raw
	}
	lid, err := b.client.Store.LIDs.GetLIDForPN(ctx, raw)
	if err != nil || lid.IsEmpty() || lid.User == "" {
		return raw
	}
	return lid
}

// contactNameForJID consults whatsmeow's contact store for the best
// human-readable label, in the order WhatsApp itself uses: full name →
// first name → business name → push name. Returns "" when nothing is
// known.
func (b *Bridge) contactNameForJID(ctx context.Context, jid types.JID) string {
	c, err := b.client.Store.Contacts.GetContact(ctx, jid)
	if err != nil || !c.Found {
		return ""
	}
	if s := strings.TrimSpace(c.FullName); s != "" {
		return s
	}
	if s := strings.TrimSpace(c.FirstName); s != "" {
		return s
	}
	if s := strings.TrimSpace(c.BusinessName); s != "" {
		return s
	}
	if s := strings.TrimSpace(c.PushName); s != "" {
		return s
	}
	return ""
}

// resolveDisplayName picks the best human-readable name for the chat.
// Priority: contact-book name on the chat JID → contact-book name on
// the LID-mapped PN JID (so LID-only chats inherit the address-book
// entry) → envelope PushName for inbound messages.
//
// The phone's address book is synced into whatsmeow_contacts on pair, so
// for known contacts FullName is what shows up — same as what WhatsApp
// itself displays in the chat list.
func (b *Bridge) resolveDisplayName(ctx context.Context, evt *events.Message) sql.NullString {
	if !evt.Info.IsGroup {
		if name := b.contactNameForJID(ctx, evt.Info.Chat); name != "" {
			return nullString(name)
		}
		// LID chats hide the phone, so contact lookup on the LID JID
		// usually misses. Fall through to the PN mapping if we know it.
		if pn := b.resolvePNForLID(ctx, evt.Info.Chat); !pn.IsEmpty() {
			if name := b.contactNameForJID(ctx, pn); name != "" {
				return nullString(name)
			}
		}
	}
	// Fall back to the PushName on the message envelope — but ONLY for
	// inbound messages. On outbound (from_me) messages PushName is the
	// sender (us), not the recipient, so it would mislabel the chat with
	// our own name.
	if !evt.Info.IsFromMe {
		if name := strings.TrimSpace(evt.Info.PushName); name != "" {
			return nullString(name)
		}
	}
	return sql.NullString{}
}

// resolvePhone returns the +E.164 number for the chat. For phone-number
// JIDs that's trivial; for @lid JIDs we ask whatsmeow's LID store for
// the mapped PN so the chat list can show the phone (and the name
// fallback chain can lookup the address book).
func (b *Bridge) resolvePhone(ctx context.Context, evt *events.Message) sql.NullString {
	chat := evt.Info.Chat
	if chat.Server == types.DefaultUserServer && chat.User != "" {
		return nullString("+" + chat.User)
	}
	if chat.Server == types.HiddenUserServer {
		if pn := b.resolvePNForLID(ctx, chat); !pn.IsEmpty() && pn.User != "" {
			return nullString("+" + pn.User)
		}
	}
	return sql.NullString{}
}

func (b *Bridge) fetchProfilePicURL(ctx context.Context, jid types.JID) sql.NullString {
	pic, err := b.client.GetProfilePictureInfo(ctx, jid, nil)
	if err != nil || pic == nil || pic.URL == "" {
		return sql.NullString{}
	}
	// Profile picture URLs from WhatsApp are temporary signed URLs. We could
	// download + SFTP them like media. For v1 we just store the URL; the
	// front-end falls back to initials when it 404s.
	return nullString(pic.URL)
}

// classifyMessage returns (type, text body, optional media descriptor).
// Recursively unwraps the container proto types whatsmeow surfaces:
//   - DeviceSentMessage: messages sent from another linked device (e.g.
//     when you send from your phone, your linked clients receive it
//     wrapped in this).
//   - EphemeralMessage: disappearing messages.
//   - ViewOnceMessage / ViewOnceMessageV2: "view once" media.
//   - DocumentWithCaptionMessage: documents that carry a caption.
// Without unwrapping, the outer envelope has none of the leaf accessors
// set and we'd misclassify the whole thing as "other".
func classifyMessage(msg *waProto.Message) (string, string, *mediaDescriptor) {
	if dsm := msg.GetDeviceSentMessage(); dsm != nil && dsm.GetMessage() != nil {
		return classifyMessage(dsm.GetMessage())
	}
	if eph := msg.GetEphemeralMessage(); eph != nil && eph.GetMessage() != nil {
		return classifyMessage(eph.GetMessage())
	}
	if vom := msg.GetViewOnceMessage(); vom != nil && vom.GetMessage() != nil {
		return classifyMessage(vom.GetMessage())
	}
	if vom := msg.GetViewOnceMessageV2(); vom != nil && vom.GetMessage() != nil {
		return classifyMessage(vom.GetMessage())
	}
	if dwc := msg.GetDocumentWithCaptionMessage(); dwc != nil && dwc.GetMessage() != nil {
		return classifyMessage(dwc.GetMessage())
	}

	switch {
	case msg.GetConversation() != "":
		return "text", msg.GetConversation(), nil
	case msg.GetExtendedTextMessage() != nil:
		return "text", msg.GetExtendedTextMessage().GetText(), nil
	case msg.GetImageMessage() != nil:
		m := msg.GetImageMessage()
		return "image", m.GetCaption(), &mediaDescriptor{
			downloadable:      m,
			mime:              m.GetMimetype(),
			embeddedThumbnail: m.GetJPEGThumbnail(),
		}
	case msg.GetVideoMessage() != nil:
		m := msg.GetVideoMessage()
		return "video", m.GetCaption(), &mediaDescriptor{
			downloadable:      m,
			mime:              m.GetMimetype(),
			embeddedThumbnail: m.GetJPEGThumbnail(),
		}
	case msg.GetAudioMessage() != nil:
		m := msg.GetAudioMessage()
		return "audio", "", &mediaDescriptor{downloadable: m, mime: m.GetMimetype()}
	case msg.GetDocumentMessage() != nil:
		m := msg.GetDocumentMessage()
		return "document", m.GetCaption(), &mediaDescriptor{
			downloadable:      m,
			mime:              m.GetMimetype(),
			filename:          m.GetFileName(),
			embeddedThumbnail: m.GetJPEGThumbnail(),
		}
	case msg.GetStickerMessage() != nil:
		m := msg.GetStickerMessage()
		return "sticker", "", &mediaDescriptor{downloadable: m, mime: m.GetMimetype()}
	case msg.GetLocationMessage() != nil:
		l := msg.GetLocationMessage()
		return "location", fmt.Sprintf("%.6f, %.6f", l.GetDegreesLatitude(), l.GetDegreesLongitude()), nil
	case msg.GetContactMessage() != nil:
		return "contact", msg.GetContactMessage().GetDisplayName(), nil
	case msg.GetReactionMessage() != nil:
		return "reaction", msg.GetReactionMessage().GetText(), nil
	case msg.GetProtocolMessage() != nil &&
		msg.GetProtocolMessage().GetType() == waProto.ProtocolMessage_REVOKE:
		return "revoked", "", nil

	// Rich / interactive types that aren't strictly media — keep
	// message_type='other' but populate body with an informative label so
	// the bubble + chat-list preview both render something useful.
	case msg.GetPollCreationMessage() != nil:
		return "other", pollPreview(msg.GetPollCreationMessage().GetName(), pollOptionNames(msg.GetPollCreationMessage().GetOptions())), nil
	case msg.GetPollCreationMessageV2() != nil:
		return "other", pollPreview(msg.GetPollCreationMessageV2().GetName(), pollOptionNames(msg.GetPollCreationMessageV2().GetOptions())), nil
	case msg.GetPollCreationMessageV3() != nil:
		return "other", pollPreview(msg.GetPollCreationMessageV3().GetName(), pollOptionNames(msg.GetPollCreationMessageV3().GetOptions())), nil
	case msg.GetPollUpdateMessage() != nil:
		return "other", "🗳️ Voto en encuesta", nil

	// Encryption-protocol envelopes — silently classified as 'system' so
	// the UI hides them (bodylessPlaceholder returns null for system).
	// Not user-facing content, just a side effect of group / sender-key
	// setup that whatsmeow surfaces alongside real messages.
	case msg.GetSenderKeyDistributionMessage() != nil:
		return "system", "", nil
	case msg.GetButtonsMessage() != nil:
		content := strings.TrimSpace(msg.GetButtonsMessage().GetContentText())
		if content == "" {
			content = "Mensaje con botones"
		}
		return "other", "🔘 " + content, nil
	case msg.GetButtonsResponseMessage() != nil:
		return "other", "▶ " + msg.GetButtonsResponseMessage().GetSelectedDisplayText(), nil
	case msg.GetListMessage() != nil:
		title := strings.TrimSpace(msg.GetListMessage().GetTitle())
		if title == "" {
			title = strings.TrimSpace(msg.GetListMessage().GetDescription())
		}
		if title == "" {
			title = "Lista interactiva"
		}
		return "other", "📋 " + title, nil
	case msg.GetListResponseMessage() != nil:
		return "other", "▶ " + msg.GetListResponseMessage().GetTitle(), nil
	case msg.GetTemplateMessage() != nil:
		return "other", "📋 Plantilla", nil

	default:
		// Diagnostic: list the proto field names actually populated on
		// this Message so we can extend the classifier next time. The
		// body is shown in the bubble + chat preview so any uncovered
		// type becomes visible at a glance instead of a generic
		// "Mensaje no compatible".
		fields := setProtoFields(msg)
		if len(fields) > 0 {
			return "other", "[proto: " + strings.Join(fields, ", ") + "]", nil
		}
		return "other", "", nil
	}
}

// setProtoFields returns the names of fields populated on a proto Message
// using reflection. Filters out a couple of envelope fields that show up
// on nearly every message and aren't useful for diagnostics.
func setProtoFields(msg *waProto.Message) []string {
	if msg == nil {
		return nil
	}
	skip := map[string]bool{
		"messageContextInfo": true,
	}
	var out []string
	msg.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		name := string(fd.Name())
		if !skip[name] {
			out = append(out, name)
		}
		return true
	})
	return out
}

func pollOptionNames(opts []*waProto.PollCreationMessage_Option) []string {
	out := make([]string, 0, len(opts))
	for _, o := range opts {
		if name := strings.TrimSpace(o.GetOptionName()); name != "" {
			out = append(out, name)
		}
	}
	return out
}

func pollPreview(name string, options []string) string {
	parts := []string{"📊 Encuesta"}
	if n := strings.TrimSpace(name); n != "" {
		parts[0] = "📊 " + n
	}
	for _, o := range options {
		parts = append(parts, "• "+o)
	}
	return strings.Join(parts, "\n")
}

type mediaDescriptor struct {
	downloadable whatsmeow.DownloadableMessage
	mime         string
	filename     string
	// Sender-embedded inline preview, if the proto carried one. We keep
	// these as the option-1 source for media_thumbnail_url — much cheaper
	// than rendering our own and works for the long tail of formats
	// (HEIC, video first frame, doc cover) we can't generate locally.
	embeddedThumbnail []byte
}

// deriveInboundThumbnail picks the JPEG bytes we'll persist as the
// chat-bubble preview for an incoming media message.
//
// Priority:
//  1. Sender-embedded thumbnail from the proto (DocumentMessage.JPEGThumbnail,
//     etc.) — free, already encoded, works for any sender-rendered format.
//  2. For PDFs only, fall back to rendering the first page via pdftoppm so
//     senders whose clients don't pre-render get one anyway.
//
// Returns (nil, nil) when neither option produces bytes — that's an
// expected outcome (e.g. an mp3 attachment, or a PDF on a host without
// poppler) and not an error worth bubbling up.
func (b *Bridge) deriveInboundThumbnail(ctx context.Context, msgType string, info *mediaDescriptor, data []byte) ([]byte, error) {
	if info == nil {
		return nil, nil
	}
	if len(info.embeddedThumbnail) > 0 {
		return info.embeddedThumbnail, nil
	}
	// Only PDFs justify the subprocess cost; other doc types
	// (Office files, archives) would need format-specific renderers
	// we're not pulling in.
	if msgType == "document" && strings.Contains(info.mime, "pdf") {
		prev, err := media.ExtractPDFPreview(ctx, data)
		if err != nil {
			return nil, err
		}
		return prev.JPEGThumbnail, nil
	}
	return nil, nil
}

func previewFor(msgType, body string, m *mediaDescriptor) string {
	if body != "" {
		return truncate(body, 160)
	}
	switch msgType {
	case "image":
		return "📷 Imagen"
	case "video":
		return "🎥 Video"
	case "audio":
		return "🎤 Audio"
	case "document":
		if m != nil && m.filename != "" {
			return "📄 " + m.filename
		}
		return "📄 Documento"
	case "sticker":
		return "Sticker"
	case "location":
		return "📍 Ubicación"
	case "contact":
		return "👤 Contacto"
	case "reaction":
		return body
	case "revoked":
		return "Mensaje eliminado"
	}
	return ""
}

func defaultStatus(fromMe bool) string {
	if fromMe {
		return "sent"
	}
	return "received"
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func nullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

// WatchProfilePics periodically refreshes wa_chats.profile_pic_url for
// non-group chats whose stored URL is approaching its 24-48h WhatsApp
// signed-URL expiry. The frontend falls back to initials on 404 so a
// stale URL doesn't break the UI, but a fresh one keeps avatars visible
// in quiet chats that no longer trigger UpsertChat on each message.
func (b *Bridge) WatchProfilePics(ctx context.Context) error {
	// First sweep ~5min after start (lets the initial history sync settle
	// without us contesting CPU/network), then every 30min thereafter.
	// Refresh anything older than ~18h, well under WhatsApp's 24h floor.
	const staleAfter = 18 * time.Hour
	const batchLimit = 30
	timer := time.NewTimer(5 * time.Minute)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			if b.client.IsConnected() {
				b.refreshStaleProfilePics(ctx, staleAfter, batchLimit)
			}
			timer.Reset(30 * time.Minute)
		}
	}
}

func (b *Bridge) refreshStaleProfilePics(ctx context.Context, staleAfter time.Duration, limit int) {
	jids, err := b.store.FindStaleProfilePics(ctx, staleAfter, limit)
	if err != nil {
		b.logger.Warnf("FindStaleProfilePics: %v", err)
		return
	}
	if len(jids) == 0 {
		return
	}
	b.logger.Infof("Refreshing %d stale profile pic URL(s)", len(jids))
	refreshed := 0
	for _, jidStr := range jids {
		if ctx.Err() != nil {
			return
		}
		jid, err := types.ParseJID(jidStr)
		if err != nil {
			continue
		}
		pic, err := b.client.GetProfilePictureInfo(ctx, jid, nil)
		newURL := ""
		if err == nil && pic != nil {
			newURL = pic.URL
		}
		if err := b.store.UpdateProfilePic(ctx, jidStr, newURL); err != nil {
			b.logger.Warnf("UpdateProfilePic %s: %v", jidStr, err)
			continue
		}
		if newURL != "" {
			refreshed++
		}
		// Tiny pause to avoid hammering WhatsApp's profile-pic endpoint
		// when there's a big backlog (first run after the column lands
		// will sweep every chat).
		select {
		case <-ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
	b.logger.Infof("Profile pic refresh: %d/%d entries got new URLs", refreshed, len(jids))
}

// WatchTypingOutbound flushes queued ChatPresence requests written by
// the React inbox into WhatsApp. Atomic drain-and-delete: each tick
// reads whatever rows are in wa_typing_outbound, deletes them, then
// calls SendChatPresence outside the DB transaction. Lossy on purpose —
// presence is ephemeral; a dropped state will be superseded by the
// next keystroke or by the natural TTL.
func (b *Bridge) WatchTypingOutbound(ctx context.Context) error {
	// 500ms is fast enough that "starts typing" feels live without
	// pegging MySQL. We also short-circuit when not connected.
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if !b.client.IsConnected() {
				continue
			}
			b.flushTypingOutbound(ctx)
		}
	}
}

func (b *Bridge) flushTypingOutbound(ctx context.Context) {
	batch, err := b.store.DrainTypingOutbound(ctx, 25)
	if err != nil {
		b.logger.Warnf("DrainTypingOutbound: %v", err)
		return
	}
	for _, r := range batch {
		jid, err := types.ParseJID(r.ChatJID)
		if err != nil {
			b.logger.Warnf("Typing outbound: bad JID %s: %v", r.ChatJID, err)
			continue
		}
		var state types.ChatPresence
		switch r.State {
		case "composing":
			state = types.ChatPresenceComposing
		case "paused":
			state = types.ChatPresencePaused
		default:
			continue
		}
		if err := b.client.SendChatPresence(ctx, jid, state, types.ChatPresenceMediaText); err != nil {
			// Not retried — the next keystroke will refresh.
			b.logger.Warnf("SendChatPresence %s %s: %v", r.ChatJID, state, err)
		}
	}
}

// WatchOutbound polls wa_outbound for pending rows and sends them via
// whatsmeow. On success it stamps status='sent' + whatsapp_message_id and
// also inserts a row into wa_messages so the UI sees the outbound message
// on the next listMessages poll without waiting for an event echo (which
// whatsmeow doesn't always deliver back to the originating device).
func (b *Bridge) WatchOutbound(ctx context.Context) error {
	// 500ms keeps perceived send latency close to network RTT without
	// hammering MySQL — even at one outbox check per 500ms across a quiet
	// queue, this is ~120 cheap indexed reads per minute.
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if !b.client.IsConnected() {
				continue
			}
			if err := b.processOutboundBatch(ctx); err != nil {
				b.logger.Warnf("processOutboundBatch: %v", err)
			}
		}
	}
}

type outboundRow struct {
	ID              int64
	ClientRequestID string
	ChatJID         string
	Body            string
	Kind            string // "text", "image", "document", "audio", "video"
	MediaURL        sql.NullString
	MediaMime       sql.NullString
	MediaFilename   sql.NullString
	MediaSize       sql.NullInt64
	MediaSHA256     sql.NullString
	Attempts        int
}

// Cap on retries before we permanently mark a row 'failed'. Tuned with
// the backoff schedule below to ~9 minutes of total elapsed time before
// giving up (10 + 30 + 60 + 120 + 300 ≈ 520s).
const outboundMaxAttempts = 5

func (b *Bridge) processOutboundBatch(ctx context.Context) error {
	// The CASE expression mirrors the per-attempt backoff: a row that's
	// failed N times waits backoffForAttempt(N) seconds before its next
	// SELECT picks it up. Bare 0 for attempts=0 makes brand-new rows
	// process immediately.
	rows, err := b.store.DB().QueryContext(ctx, `
		SELECT id, client_request_id, chat_jid, body, kind,
		       media_url, media_mime, media_filename, media_size, media_sha256,
		       attempts
		FROM wa_outbound
		WHERE status = 'pending'
		  AND attempts < ?
		  AND (
		    last_attempt_at IS NULL
		    OR last_attempt_at < (UTC_TIMESTAMP() - INTERVAL (CASE attempts
		        WHEN 0 THEN 0
		        WHEN 1 THEN 10
		        WHEN 2 THEN 30
		        WHEN 3 THEN 60
		        WHEN 4 THEN 120
		        ELSE 300 END) SECOND)
		  )
		ORDER BY id
		LIMIT 10
	`, outboundMaxAttempts)
	if err != nil {
		return err
	}
	var batch []outboundRow
	for rows.Next() {
		var r outboundRow
		if err := rows.Scan(
			&r.ID, &r.ClientRequestID, &r.ChatJID, &r.Body, &r.Kind,
			&r.MediaURL, &r.MediaMime, &r.MediaFilename, &r.MediaSize, &r.MediaSHA256,
			&r.Attempts,
		); err != nil {
			rows.Close()
			return err
		}
		batch = append(batch, r)
	}
	rows.Close()

	for _, r := range batch {
		b.sendOutboundRow(ctx, r)
	}
	return nil
}

func (b *Bridge) sendOutboundRow(ctx context.Context, r outboundRow) {
	chatJID, err := types.ParseJID(r.ChatJID)
	if err != nil {
		// Bad JID is not a transient error — no point retrying.
		b.markOutboundPermanentlyFailed(ctx, r.ID, "invalid chat_jid: "+err.Error())
		return
	}

	// Storage key for wa_chats / wa_messages: canonicalize so an outbound
	// row queued under PN doesn't recreate a PN twin if WhatsApp routes
	// the matching inbound through LID.
	storeChatJID := b.canonicalChatJID(ctx, chatJID).String()

	msg, msgType, thumbURL, sendErr := b.buildOutboundMessage(ctx, r)
	if sendErr != nil {
		// buildOutboundMessage marks permanent failures itself for bad
		// configuration; anything that reaches here is transient (SFTP,
		// upload to WhatsApp CDN) and gets the normal backoff treatment.
		b.markOutboundAttempt(ctx, r, sendErr.Error())
		return
	}

	resp, err := b.client.SendMessage(ctx, chatJID, msg)
	if err != nil {
		b.markOutboundAttempt(ctx, r, err.Error())
		return
	}

	// Stamp the outbound row.
	if _, err := b.store.DB().ExecContext(ctx, `
		UPDATE wa_outbound
		SET status = 'sent',
		    whatsapp_message_id = ?,
		    sent_at = NOW(),
		    error_message = NULL
		WHERE id = ?
	`, resp.ID, r.ID); err != nil {
		b.logger.Warnf("Failed to mark outbound %d sent: %v", r.ID, err)
	}

	// Insert into wa_messages so the UI sees the message right away.
	// Carry the client_request_id forward so the React inbox can match
	// its optimistic bubble against this real row deterministically,
	// regardless of whether send.php has already responded with the
	// whatsapp_message_id (the two race).
	//
	// Media metadata is the public URL the PHP gateway already stored —
	// not the WhatsApp CDN URL, which is encrypted and useless to the UI.
	msgRow := store.Message{
		ID:              resp.ID,
		ChatJID:         storeChatJID,
		SenderJID:       nullString(b.selfJID()),
		FromMe:          true,
		MessageType:     msgType,
		Body:            nullString(r.Body),
		MediaURL:        r.MediaURL,
		MediaMime:       r.MediaMime,
		MediaSize:       r.MediaSize,
		MediaFilename:   r.MediaFilename,
		MediaSHA256:     r.MediaSHA256,
		MediaThumbnailURL: thumbURL,
		ClientRequestID: nullString(r.ClientRequestID),
		Timestamp:       resp.Timestamp,
		Status:          sql.NullString{String: "sent", Valid: true},
	}
	if err := b.store.InsertMessage(ctx, msgRow); err != nil {
		b.logger.Warnf("Insert outbound wa_messages %s: %v", resp.ID, err)
	}

	// Reflect on chat tail so the chat list re-sorts.
	preview := r.Body
	if preview == "" {
		preview = previewForKind(msgType, r.MediaFilename.String)
	}
	chatRow := store.Chat{
		JID:                storeChatJID,
		LastMessageAt:      sql.NullTime{Time: resp.Timestamp, Valid: true},
		LastMessagePreview: nullString(truncate(preview, 160)),
		LastMessageFromMe:  true,
		IncrementUnread:    false,
	}
	if err := b.store.UpsertChat(ctx, chatRow); err != nil {
		b.logger.Warnf("Outbound UpsertChat %s: %v", storeChatJID, err)
	}

	b.logger.Infof("Sent outbound %d (%s, %s) → %s", r.ID, resp.ID, msgType, storeChatJID)
}

// buildOutboundMessage materialises the proto message to send for a row,
// reading any media bytes from the shared SFTP host and uploading them to
// WhatsApp's CDN. Returns the message, the wa_messages.message_type
// string to record, an optional thumbnail public URL to persist on the
// outbound wa_messages row (empty if no thumbnail was generated /
// uploaded), and a non-nil error on retryable failure.
func (b *Bridge) buildOutboundMessage(ctx context.Context, r outboundRow) (*waProto.Message, string, sql.NullString, error) {
	var thumbURL sql.NullString
	kind := r.Kind
	if kind == "" {
		kind = "text"
	}

	if kind == "text" {
		return &waProto.Message{
			Conversation: proto.String(r.Body),
		}, "text", thumbURL, nil
	}

	mediaType, ok := mediaTypeForKind(kind)
	if !ok {
		b.markOutboundPermanentlyFailed(ctx, r.ID, "unsupported kind: "+kind)
		return nil, "", thumbURL, fmt.Errorf("unsupported kind %q (already marked failed)", kind)
	}
	if !r.MediaURL.Valid || r.MediaURL.String == "" {
		b.markOutboundPermanentlyFailed(ctx, r.ID, "media row missing media_url")
		return nil, "", thumbURL, fmt.Errorf("media_url missing (already marked failed)")
	}

	basename := b.uploader.BasenameFromPublicURL(r.MediaURL.String)
	if basename == "" {
		b.markOutboundPermanentlyFailed(ctx, r.ID, "media_url not under configured public base")
		return nil, "", thumbURL, fmt.Errorf("media_url outside public base (already marked failed)")
	}
	data, err := b.uploader.Read(basename)
	if err != nil {
		return nil, "", thumbURL, fmt.Errorf("read media: %w", err)
	}

	up, err := b.client.Upload(ctx, data, mediaType)
	if err != nil {
		return nil, "", thumbURL, fmt.Errorf("upload to WA CDN: %w", err)
	}

	mime := r.MediaMime.String
	switch kind {
	case "image":
		img := &waProto.ImageMessage{
			Caption:       protoStringPtrOrNil(r.Body),
			Mimetype:      proto.String(mime),
			URL:           &up.URL,
			DirectPath:    &up.DirectPath,
			MediaKey:      up.MediaKey,
			FileEncSHA256: up.FileEncSHA256,
			FileSHA256:    up.FileSHA256,
			FileLength:    &up.FileLength,
		}
		// Inline JPEG thumbnail + dimensions are what makes WhatsApp
		// render an actual preview in chat instead of a gray placeholder
		// that the recipient has to tap to download. Thumbnail
		// generation is best-effort — an unknown image format (e.g.
		// HEIC, which Go's stdlib can't decode) just sends without a
		// preview rather than failing the whole message.
		//
		// The image bubble in our React UI uses media_url directly
		// (full-res image), so we don't bother persisting the thumbnail
		// for outbound images — the sender already sees the file inline.
		if thumb, w, h, terr := media.MakeImageThumbnail(data); terr == nil {
			img.JPEGThumbnail = thumb
			wu, hu := uint32(w), uint32(h)
			img.Width, img.Height = &wu, &hu
		} else {
			b.logger.Warnf("Outbound %d: thumbnail skipped (%s): %v", r.ID, mime, terr)
		}
		return &waProto.Message{ImageMessage: img}, "image", thumbURL, nil

	case "video":
		return &waProto.Message{
			VideoMessage: &waProto.VideoMessage{
				Caption:       protoStringPtrOrNil(r.Body),
				Mimetype:      proto.String(mime),
				URL:           &up.URL,
				DirectPath:    &up.DirectPath,
				MediaKey:      up.MediaKey,
				FileEncSHA256: up.FileEncSHA256,
				FileSHA256:    up.FileSHA256,
				FileLength:    &up.FileLength,
			},
		}, "video", thumbURL, nil

	case "audio":
		return &waProto.Message{
			AudioMessage: &waProto.AudioMessage{
				Mimetype:      proto.String(mime),
				URL:           &up.URL,
				DirectPath:    &up.DirectPath,
				MediaKey:      up.MediaKey,
				FileEncSHA256: up.FileEncSHA256,
				FileSHA256:    up.FileSHA256,
				FileLength:    &up.FileLength,
			},
		}, "audio", thumbURL, nil

	case "document":
		doc := &waProto.DocumentMessage{
			FileName:      protoStringPtrOrNil(r.MediaFilename.String),
			Mimetype:      proto.String(mime),
			URL:           &up.URL,
			DirectPath:    &up.DirectPath,
			MediaKey:      up.MediaKey,
			FileEncSHA256: up.FileEncSHA256,
			FileSHA256:    up.FileSHA256,
			FileLength:    &up.FileLength,
		}
		// Title is the human-readable name WhatsApp displays on the
		// document card; default it to the original filename so the
		// recipient sees something sensible even when pdfinfo doesn't
		// surface a Title metadata field.
		if r.MediaFilename.String != "" {
			doc.Title = proto.String(r.MediaFilename.String)
		}
		// PDF cover thumbnail + page count is what turns the document
		// card from a generic paper-clip blob into a real first-page
		// preview. Best-effort — when poppler isn't installed we just
		// send the document without a preview.
		//
		// We also persist the rendered thumbnail to SFTP so the sender's
		// own chat (rendered by the React UI from wa_messages) shows
		// the same cover the recipient will see.
		if strings.Contains(mime, "pdf") {
			if prev, err := media.ExtractPDFPreview(ctx, data); err == nil {
				if prev.PageCount > 0 {
					pc := uint32(prev.PageCount)
					doc.PageCount = &pc
				}
				if prev.Title != "" {
					doc.Title = proto.String(prev.Title)
				}
				if len(prev.JPEGThumbnail) > 0 {
					doc.JPEGThumbnail = prev.JPEGThumbnail
					tw, th := uint32(prev.ThumbnailWidth), uint32(prev.ThumbnailHeight)
					doc.ThumbnailWidth, doc.ThumbnailHeight = &tw, &th
					if r.MediaSHA256.Valid && r.MediaSHA256.String != "" {
						if turl, uerr := b.uploader.UploadThumbnail(r.MediaSHA256.String, prev.JPEGThumbnail); uerr == nil {
							thumbURL = nullString(turl)
						} else {
							b.logger.Warnf("Outbound %d: SFTP upload thumb: %v", r.ID, uerr)
						}
					}
				}
			} else {
				b.logger.Warnf("Outbound %d: PDF preview skipped: %v", r.ID, err)
			}
		}
		// DocumentMessage's own Caption field is ignored by WhatsApp
		// clients; the supported way to caption a document is to wrap
		// it in DocumentWithCaptionMessage.
		if r.Body != "" {
			return &waProto.Message{
				DocumentWithCaptionMessage: &waProto.FutureProofMessage{
					Message: &waProto.Message{
						DocumentMessage: doc,
					},
				},
			}, "document", thumbURL, nil
		}
		return &waProto.Message{DocumentMessage: doc}, "document", thumbURL, nil
	}

	// Unreachable — mediaTypeForKind already validated kind above.
	return nil, "", thumbURL, fmt.Errorf("internal: unhandled kind %q", kind)
}

func mediaTypeForKind(kind string) (whatsmeow.MediaType, bool) {
	switch kind {
	case "image":
		return whatsmeow.MediaImage, true
	case "video":
		return whatsmeow.MediaVideo, true
	case "audio":
		return whatsmeow.MediaAudio, true
	case "document":
		return whatsmeow.MediaDocument, true
	}
	return "", false
}

func protoStringPtrOrNil(s string) *string {
	if s == "" {
		return nil
	}
	return proto.String(s)
}

// previewForKind returns a chat-list preview when a media message has no
// caption to use. Mirrors the labels used by the React inbox so the chat
// list stays consistent across refreshes.
func previewForKind(kind, filename string) string {
	switch kind {
	case "image":
		return "📷 Imagen"
	case "video":
		return "🎬 Video"
	case "audio":
		return "🎵 Audio"
	case "document":
		if filename != "" {
			return "📄 " + filename
		}
		return "📄 Documento"
	}
	return ""
}

// markOutboundAttempt records a send failure and, if the attempts cap has
// been reached, transitions the row to status='failed'. Otherwise the row
// stays 'pending' and the next processOutboundBatch tick (after the
// per-attempt backoff window) will retry it.
func (b *Bridge) markOutboundAttempt(ctx context.Context, r outboundRow, msg string) {
	if len(msg) > 500 {
		msg = msg[:500]
	}
	nextAttempts := r.Attempts + 1
	if nextAttempts >= outboundMaxAttempts {
		if _, err := b.store.DB().ExecContext(ctx, `
			UPDATE wa_outbound
			SET status = 'failed',
			    attempts = ?,
			    last_attempt_at = UTC_TIMESTAMP(),
			    error_message = ?
			WHERE id = ?
		`, nextAttempts, msg, r.ID); err != nil {
			b.logger.Warnf("Failed to mark outbound %d failed: %v", r.ID, err)
			return
		}
		b.logger.Warnf("Outbound %d permanently failed after %d attempts: %s", r.ID, nextAttempts, msg)
		return
	}
	if _, err := b.store.DB().ExecContext(ctx, `
		UPDATE wa_outbound
		SET attempts = ?,
		    last_attempt_at = UTC_TIMESTAMP(),
		    error_message = ?
		WHERE id = ?
	`, nextAttempts, msg, r.ID); err != nil {
		b.logger.Warnf("Failed to bump attempts on outbound %d: %v", r.ID, err)
		return
	}
	b.logger.Infof("Outbound %d attempt %d/%d failed: %s — will retry", r.ID, nextAttempts, outboundMaxAttempts, msg)
}

// markOutboundPermanentlyFailed terminates retries immediately. Use for
// errors that won't fix themselves on the next try (bad JID, etc.).
func (b *Bridge) markOutboundPermanentlyFailed(ctx context.Context, id int64, msg string) {
	if len(msg) > 500 {
		msg = msg[:500]
	}
	if _, err := b.store.DB().ExecContext(ctx, `
		UPDATE wa_outbound
		SET status = 'failed',
		    attempts = ?,
		    last_attempt_at = UTC_TIMESTAMP(),
		    error_message = ?
		WHERE id = ?
	`, outboundMaxAttempts, msg, id); err != nil {
		b.logger.Warnf("Failed to mark outbound %d permanently failed: %v", id, err)
	}
}

func (b *Bridge) selfJID() string {
	if b.client.Store.ID == nil {
		return ""
	}
	return b.client.Store.ID.String()
}

