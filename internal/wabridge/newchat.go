package wabridge

import (
	"context"
	"database/sql"
	"regexp"
	"strings"
	"time"

	"github.com/enviadores/wabridge/internal/store"

	waProto "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

// New chats by phone number.
//
// The web app queues a message to a customer who never wrote to the office
// number as a wa_outbound row with status='resolve', chat_jid=NULL and
// to_phone='+52XXXXXXXXXX'. The bridge asks WhatsApp whether the number has
// an account (IsOnWhatsApp, which also returns the canonical JID and stores
// the LID mapping), sends to that JID, and only then writes wa_chats,
// wa_messages and stamps the row 'sent' with its chat_jid. A number without
// WhatsApp fails with error_code NOT_ON_WHATSAPP and leaves no chat behind:
// wa_chats is written only after a send to a JID WhatsApp confirmed.
//
// Older bridges select only status='pending', so they never see these rows,
// and the web app queues them only while the heartbeat announces "new_chat"
// (store.BridgeFeatures).

// featureNewChat names the Fallback feature for wa_outbound.to_phone. Without
// the column the new-chat path is off for the life of the process.
const featureNewChat = "wa_outbound.to_phone"

// newChatMinGap is the least time between two new-chat WhatsApp lookups (each
// one that finds an account is followed by its send). The owner keeps the
// volume slow and natural; this is a cheap guard on top. Existing-chat sends
// are not paced.
const newChatMinGap = 20 * time.Second

// newChatCallTimeout bounds each WhatsApp call of the new-chat path, so a
// stuck usync or send cannot stall the outbound loop.
const newChatCallTimeout = 45 * time.Second

// Machine reasons written to wa_outbound.error_code.
const (
	codeNotOnWhatsApp   = "NOT_ON_WHATSAPP"
	codeInvalidPhone    = "INVALID_PHONE"
	codeSendFailed      = "SEND_FAILED"
	codeLookupFailed    = "LOOKUP_FAILED"
	codeUnsupportedKind = "UNSUPPORTED_KIND"
)

// mxPhoneRE is the only to_phone form the web app writes: +52 and the 10
// national digits. WhatsApp's lookup decides between the 52 and 521 forms.
var mxPhoneRE = regexp.MustCompile(`^\+52[0-9]{10}$`)

// newChatRow is one status='resolve' row of wa_outbound.
type newChatRow struct {
	ID              int64
	ClientRequestID sql.NullString
	ToPhone         sql.NullString
	ToName          sql.NullString
	Body            sql.NullString
	Kind            sql.NullString
	Attempts        int
}

// newChatWrites is what a successful send still has to store. It is kept
// until the wa_outbound row is stamped, so a stamp that fails is retried
// without sending the message a second time.
type newChatWrites struct {
	chat  store.Chat
	msg   store.Message
	waID  string
	label string // for logs: the store JID
}

type newChatState struct {
	lastLookup time.Time
	sent       map[int64]newChatWrites
}

func (b *Bridge) clock() time.Time {
	if b.now != nil {
		return b.now()
	}
	return time.Now()
}

// processNewChats sends at most one new-chat row per call, and none while
// the previous lookup is less than newChatMinGap ago. Errors are logged: the
// existing-chat path must keep working whatever happens here.
func (b *Bridge) processNewChats(ctx context.Context) {
	if b.store.FeatureOff(featureNewChat) {
		return
	}
	if !b.newChat.lastLookup.IsZero() && b.clock().Sub(b.newChat.lastLookup) < newChatMinGap {
		return
	}
	var row *newChatRow
	err := b.store.Fallback(featureNewChat, func() (err error) {
		row, err = b.queryResolveOutbound(ctx)
		return err
	}, nil)
	if err != nil {
		b.logger.Warnf("new chat query: %v", err)
		return
	}
	if row != nil {
		b.sendNewChatRow(ctx, *row)
	}
}

func (b *Bridge) queryResolveOutbound(ctx context.Context) (*newChatRow, error) {
	rows, err := b.store.DB().QueryContext(ctx, `
		SELECT id, client_request_id, to_phone, to_name, body, kind, attempts
		FROM wa_outbound
		WHERE status = 'resolve'
		  AND attempts < ?
		  AND `+outboundBackoffSQL+`
		ORDER BY id
		LIMIT 1
	`, outboundMaxAttempts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, rows.Err()
	}
	var r newChatRow
	if err := rows.Scan(&r.ID, &r.ClientRequestID, &r.ToPhone, &r.ToName, &r.Body, &r.Kind, &r.Attempts); err != nil {
		return nil, err
	}
	return &r, rows.Err()
}

// sendNewChatRow resolves the row's phone to a WhatsApp account, sends the
// text and stores the result. See the package comment above for the order.
func (b *Bridge) sendNewChatRow(ctx context.Context, r newChatRow) {
	if b.newChat.sent == nil {
		b.newChat.sent = map[int64]newChatWrites{}
	}
	// Already sent, only the stamp is missing: never send again.
	if w, ok := b.newChat.sent[r.ID]; ok {
		b.finishNewChat(r.ID, w)
		return
	}
	if r.Kind.String != "text" || strings.TrimSpace(r.Body.String) == "" {
		b.failNewChat(ctx, r.ID, codeUnsupportedKind, "a new chat takes a text message, got kind "+r.Kind.String)
		return
	}
	phone := r.ToPhone.String
	if !mxPhoneRE.MatchString(phone) {
		b.failNewChat(ctx, r.ID, codeInvalidPhone, "to_phone must be +52 and 10 digits, got "+quoteForLog(phone))
		return
	}

	b.newChat.lastLookup = b.clock()
	lookupCtx, cancel := context.WithTimeout(ctx, newChatCallTimeout)
	res, err := b.isOnWhatsApp(lookupCtx, []string{phone})
	cancel()
	if err != nil && len(res) == 0 {
		b.markAttempt(ctx, r.ID, r.Attempts, codeLookupFailed, "whatsapp lookup: "+err.Error())
		return
	}
	if err != nil {
		// The fork returns the answer together with an error when only
		// caching the LID mapping failed; the answer itself is good.
		b.logger.Warnf("Outbound %d: whatsapp lookup answered with a warning: %v", r.ID, err)
	}
	if len(res) == 0 || !res[0].IsIn {
		b.failNewChat(ctx, r.ID, codeNotOnWhatsApp, "number is not on WhatsApp")
		return
	}
	hit := res[0]
	target := hit.JID
	if target.User == "" || (target.Server != types.DefaultUserServer && target.Server != types.HiddenUserServer) {
		b.failNewChat(ctx, r.ID, codeInvalidPhone, "lookup returned a non-user JID "+target.String())
		return
	}
	if target.Server == types.DefaultUserServer {
		target = b.canonicalChatJID(ctx, target)
	}
	storeJID := target.String()

	sendCtx, cancel := context.WithTimeout(ctx, newChatCallTimeout)
	resp, err := b.sendMessage(sendCtx, target, &waProto.Message{Conversation: proto.String(r.Body.String)})
	cancel()
	if err != nil {
		// The next attempt looks the number up again.
		b.markAttempt(ctx, r.ID, r.Attempts, codeSendFailed, err.Error())
		return
	}

	phoneE164 := r.ToPhone
	if hit.PhoneNumber.Server == types.DefaultUserServer && hit.PhoneNumber.User != "" {
		phoneE164 = nullString("+" + hit.PhoneNumber.User)
	}
	w := newChatWrites{
		chat: store.Chat{
			JID:                storeJID,
			DisplayName:        b.newChatDisplayName(ctx, target, hit, r.ToName),
			PhoneE164:          phoneE164,
			LastMessageAt:      sql.NullTime{Time: resp.Timestamp, Valid: true},
			LastMessagePreview: nullString(truncate(r.Body.String, 160)),
			LastMessageFromMe:  true,
		},
		msg: store.Message{
			ID:              resp.ID,
			ChatJID:         storeJID,
			SenderJID:       nullString(b.selfJID()),
			FromMe:          true,
			MessageType:     "text",
			Body:            nullString(r.Body.String),
			ClientRequestID: r.ClientRequestID,
			Timestamp:       resp.Timestamp,
			Status:          sql.NullString{String: "sent", Valid: true},
		},
		waID:  resp.ID,
		label: storeJID,
	}
	b.newChat.sent[r.ID] = w
	b.finishNewChat(r.ID, w)
}

// finishNewChat stores a sent new-chat message: the chat row first, then the
// message (its FK needs the chat), then the outbound stamp. A chat row that
// cannot be written leaves chat_jid NULL on the stamp; the customer's reply
// or a receipt creates the chat later. If the stamp itself fails the row
// stays 'resolve' and the next tick retries the writes, never the send.
func (b *Bridge) finishNewChat(id int64, w newChatWrites) {
	chatJID := nullString(w.chat.JID)
	if err := storeRetry(func(ctx context.Context) error { return b.store.UpsertChat(ctx, w.chat) }); err != nil {
		b.logger.Errorf("Outbound %d sent as %s but the chat row could not be written: %v", id, w.waID, err)
		chatJID = sql.NullString{}
	} else if err := storeRetry(func(ctx context.Context) error { return b.store.InsertMessage(ctx, w.msg) }); err != nil {
		b.logger.Warnf("Insert outbound wa_messages %s: %v", w.waID, err)
	}
	if err := storeRetry(func(ctx context.Context) error {
		return b.store.MarkNewChatSent(ctx, id, chatJID, w.waID)
	}); err != nil {
		b.logger.Errorf("Outbound %d sent as %s but could not be marked sent (will retry the stamp, not the send): %v", id, w.waID, err)
		return
	}
	delete(b.newChat.sent, id)
	b.logger.Infof("Sent outbound %d (%s, new chat) -> %s", id, w.waID, w.label)
}

// failNewChat ends a new-chat row. These are expected outcomes (a customer
// without WhatsApp), so they log at info, not error.
func (b *Bridge) failNewChat(ctx context.Context, id int64, code, detail string) {
	b.logger.Infof("Outbound %d new chat not sent (%s): %s", id, code, detail)
	if err := b.store.FailOutbound(ctx, id, outboundMaxAttempts, code, detail); err != nil {
		b.logger.Warnf("Failed to mark outbound %d failed: %v", id, err)
	}
}

// newChatDisplayName: the address-book name (on the LID, then the phone
// JID), the verified business name, the name the web app passed, or NULL.
func (b *Bridge) newChatDisplayName(ctx context.Context, target types.JID, hit types.IsOnWhatsAppResponse, toName sql.NullString) sql.NullString {
	if name := b.contactNameForJID(ctx, target); name != "" {
		return nullString(name)
	}
	if name := b.contactNameForJID(ctx, hit.PhoneNumber); name != "" {
		return nullString(name)
	}
	if hit.VerifiedName != nil && hit.VerifiedName.Details != nil {
		if name := strings.TrimSpace(hit.VerifiedName.Details.GetVerifiedName()); name != "" {
			return nullString(name)
		}
	}
	return nullString(strings.TrimSpace(toName.String))
}

// quoteForLog keeps a malformed phone readable (and short) in logs.
func quoteForLog(s string) string {
	if len(s) > 24 {
		s = s[:24] + "..."
	}
	return `"` + s + `"`
}
