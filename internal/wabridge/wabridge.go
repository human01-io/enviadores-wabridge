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
	"github.com/enviadores/wabridge/internal/store"

	_ "github.com/mattn/go-sqlite3"
	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

type Bridge struct {
	cfg      *config.Config
	store    *store.Store
	uploader *media.Uploader
	client   *whatsmeow.Client
	logger   waLog.Logger
}

func New(cfg *config.Config, st *store.Store, up *media.Uploader) (*Bridge, error) {
	level := strings.ToUpper(cfg.Whatsmeow.LogLevel)
	dbLog := waLog.Stdout("wa-db", level, true)
	clientLog := waLog.Stdout("wa-client", level, true)

	container, err := sqlstore.New("sqlite3",
		fmt.Sprintf("file:%s?_foreign_keys=on", cfg.Whatsmeow.StorePath),
		dbLog,
	)
	if err != nil {
		return nil, fmt.Errorf("open whatsmeow sqlite store: %w", err)
	}

	deviceStore, err := container.GetFirstDevice()
	if err != nil {
		return nil, fmt.Errorf("get device: %w", err)
	}

	client := whatsmeow.NewClient(deviceStore, clientLog)

	b := &Bridge{
		cfg:      cfg,
		store:    st,
		uploader: up,
		client:   client,
		logger:   clientLog,
	}
	client.AddEventHandler(b.handleEvent)
	return b, nil
}

// Start connects to WhatsApp. If no device session exists, it prints a QR
// code to stdout for pairing on first run.
func (b *Bridge) Start(ctx context.Context) error {
	if b.client.Store.ID == nil {
		qrChan, _ := b.client.GetQRChannel(ctx)
		if err := b.client.Connect(); err != nil {
			return err
		}
		go func() {
			for evt := range qrChan {
				switch evt.Event {
				case "code":
					b.logger.Infof("=== SCAN THIS QR CODE WITH WHATSAPP > LINKED DEVICES ===")
					b.logger.Infof("%s", evt.Code)
					b.logger.Infof("======================================================")
				case "success":
					b.logger.Infof("Paired successfully")
				case "timeout":
					b.logger.Warnf("QR pairing timed out — restart wabridge to retry")
				}
			}
		}()
		return nil
	}
	return b.client.Connect()
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
	case *events.Disconnected:
		b.logger.Warnf("Disconnected from WhatsApp")
	case *events.LoggedOut:
		b.logger.Errorf("Logged out: %s — re-pair required", v.Reason)
	case *events.Message:
		b.handleMessage(v)
	}
}

func (b *Bridge) handleMessage(evt *events.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	chatJID := evt.Info.Chat.String()
	senderJID := evt.Info.Sender.String()
	fromMe := evt.Info.IsFromMe
	timestamp := evt.Info.Timestamp

	msgType, body, mediaInfo := classifyMessage(evt.Message)

	// Download & upload media if present.
	var (
		mediaURL    sql.NullString
		mediaMime   sql.NullString
		mediaSize   sql.NullInt64
		mediaName   sql.NullString
		mediaSHA256 sql.NullString
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
			}
		}
	}

	// Upsert chat row first (FK target for the message).
	preview := previewFor(msgType, body, mediaInfo)
	chat := store.Chat{
		JID:                chatJID,
		DisplayName:        b.resolveDisplayName(evt),
		PhoneE164:          b.resolvePhone(evt),
		IsGroup:            evt.Info.IsGroup,
		ProfilePicURL:      b.fetchProfilePicURL(ctx, evt.Info.Chat),
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
		MediaSHA256:     mediaSHA256,
		QuotedMessageID: sql.NullString{}, // wired in a later iteration if needed
		Timestamp:       timestamp,
		Status:          sql.NullString{String: defaultStatus(fromMe), Valid: true},
	}
	if err := b.store.InsertMessage(ctx, msg); err != nil {
		b.logger.Errorf("InsertMessage %s: %v", evt.Info.ID, err)
	}
}

func (b *Bridge) resolveDisplayName(evt *events.Message) sql.NullString {
	if name := strings.TrimSpace(evt.Info.PushName); name != "" {
		return nullString(name)
	}
	// For groups the chat has its own subject; getting it requires GetGroupInfo
	// — skipped here for simplicity. The phone number from JID is a usable
	// fallback.
	return sql.NullString{}
}

func (b *Bridge) resolvePhone(evt *events.Message) sql.NullString {
	chat := evt.Info.Chat
	if chat.Server == types.DefaultUserServer && chat.User != "" {
		return nullString("+" + chat.User)
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
func classifyMessage(msg *waProto.Message) (string, string, *mediaDescriptor) {
	switch {
	case msg.GetConversation() != "":
		return "text", msg.GetConversation(), nil
	case msg.GetExtendedTextMessage() != nil:
		return "text", msg.GetExtendedTextMessage().GetText(), nil
	case msg.GetImageMessage() != nil:
		m := msg.GetImageMessage()
		return "image", m.GetCaption(), &mediaDescriptor{downloadable: m, mime: m.GetMimetype()}
	case msg.GetVideoMessage() != nil:
		m := msg.GetVideoMessage()
		return "video", m.GetCaption(), &mediaDescriptor{downloadable: m, mime: m.GetMimetype()}
	case msg.GetAudioMessage() != nil:
		m := msg.GetAudioMessage()
		return "audio", "", &mediaDescriptor{downloadable: m, mime: m.GetMimetype()}
	case msg.GetDocumentMessage() != nil:
		m := msg.GetDocumentMessage()
		return "document", m.GetCaption(), &mediaDescriptor{
			downloadable: m,
			mime:         m.GetMimetype(),
			filename:     m.GetFileName(),
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
	default:
		return "other", "", nil
	}
}

type mediaDescriptor struct {
	downloadable whatsmeow.DownloadableMessage
	mime         string
	filename     string
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

