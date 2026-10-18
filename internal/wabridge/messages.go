package wabridge

// Pure message helpers: unwrapping, protocol actions (edits / deletes),
// quote (reply) extraction and construction. Nothing here touches the network
// or the database, so it is all unit-tested in messages_test.go.
//
// Log text in this package is ASCII only: the production bridge runs on a
// Windows box whose console and Event Log mangle anything else.

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/enviadores/wabridge/internal/store"
	waProto "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

// isSkippedChat reports chats the inbox never shows: WhatsApp Status
// (status@broadcast) and the bot server (Meta AI). Processing them only
// burns round trips over the SSH tunnel.
func isSkippedChat(jid types.JID) bool {
	if jid.Server == types.BroadcastServer && jid.User == "status" {
		return true
	}
	return jid.Server == types.BotServer
}

// unwrapMessage peels the container protos WhatsApp wraps real content in
// (messages from another linked device, disappearing messages, view once,
// documents with caption, album children, edits) and returns the innermost
// Message. A nil input gives nil.
func unwrapMessage(msg *waProto.Message) *waProto.Message {
	for i := 0; i < 8 && msg != nil; i++ {
		var inner *waProto.Message
		switch {
		case msg.GetDeviceSentMessage().GetMessage() != nil:
			inner = msg.GetDeviceSentMessage().GetMessage()
		case msg.GetEphemeralMessage().GetMessage() != nil:
			inner = msg.GetEphemeralMessage().GetMessage()
		case msg.GetViewOnceMessage().GetMessage() != nil:
			inner = msg.GetViewOnceMessage().GetMessage()
		case msg.GetViewOnceMessageV2().GetMessage() != nil:
			inner = msg.GetViewOnceMessageV2().GetMessage()
		case msg.GetViewOnceMessageV2Extension().GetMessage() != nil:
			inner = msg.GetViewOnceMessageV2Extension().GetMessage()
		case msg.GetDocumentWithCaptionMessage().GetMessage() != nil:
			inner = msg.GetDocumentWithCaptionMessage().GetMessage()
		case msg.GetAssociatedChildMessage().GetMessage() != nil:
			// Album: each photo / video of the album arrives wrapped here.
			inner = msg.GetAssociatedChildMessage().GetMessage()
		case msg.GetEditedMessage().GetMessage() != nil:
			inner = msg.GetEditedMessage().GetMessage()
		}
		if inner == nil {
			return msg
		}
		msg = inner
	}
	return msg
}

// ---------------------------------------------------------------------------
// Edits and deletes

type actionKind int

const (
	actionNone   actionKind = iota // an ordinary message: store it
	actionEdit                     // rewrite the body of TargetID
	actionRevoke                   // TargetID was deleted for everyone
	actionDrop                     // not storable and not an error (diagnostic only)
)

// protoAction is what a protocol-level message asks us to do.
type protoAction struct {
	Kind     actionKind
	TargetID string
	Edited   *waProto.Message // the new content (actionEdit)
	Reason   string           // why a message was dropped (actionDrop)
}

// secretDecryptor decrypts a SecretEncryptedMessage (client method in
// production, a stub in tests).
type secretDecryptor func(ctx context.Context, evt *events.Message) (*waProto.Message, error)

// resolveAction classifies a message that may be an edit or a delete.
//
// Edits reach us in three shapes:
//  1. ProtocolMessage{MESSAGE_EDIT, Key, EditedMessage}: the normal case,
//     either bare or after whatsmeow unwrapped an EditedMessage.
//  2. SecretEncryptedMessage{MESSAGE_EDIT}: decrypted with the original
//     message's secret. The plaintext is either a Message holding shape 1
//     (the target is then in its Key), or the bare new content (the target is
//     then the SecretEncryptedMessage's TargetMessageKey).
//
// A message whose secret we no longer have cannot be decrypted
// (ErrOriginalMessageSecretNotFound and friends): it is dropped, not stored.
func resolveAction(ctx context.Context, evt *events.Message, decrypt secretDecryptor) protoAction {
	m := unwrapMessage(evt.Message)
	if m == nil {
		return protoAction{}
	}
	if pm := m.GetProtocolMessage(); pm != nil {
		switch pm.GetType() {
		case waProto.ProtocolMessage_MESSAGE_EDIT:
			return editAction(pm.GetKey().GetID(), pm.GetEditedMessage())
		case waProto.ProtocolMessage_REVOKE:
			if id := pm.GetKey().GetID(); id != "" {
				return protoAction{Kind: actionRevoke, TargetID: id}
			}
		}
		return protoAction{}
	}
	enc := m.GetSecretEncryptedMessage()
	if enc == nil {
		return protoAction{}
	}
	if enc.GetSecretEncType() != waProto.SecretEncryptedMessage_MESSAGE_EDIT {
		return protoAction{Kind: actionDrop, Reason: "secret message type " + enc.GetSecretEncType().String()}
	}
	if decrypt == nil {
		return protoAction{Kind: actionDrop, Reason: "no decryptor"}
	}
	plain, err := decrypt(ctx, evt)
	if err != nil {
		return protoAction{Kind: actionDrop, Reason: "edit not decrypted: " + asciiOnly(err.Error())}
	}
	plain = unwrapMessage(plain)
	if pm := plain.GetProtocolMessage(); pm != nil && pm.GetType() == waProto.ProtocolMessage_MESSAGE_EDIT {
		id := pm.GetKey().GetID()
		if id == "" {
			id = enc.GetTargetMessageKey().GetID()
		}
		return editAction(id, pm.GetEditedMessage())
	}
	return editAction(enc.GetTargetMessageKey().GetID(), plain)
}

func editAction(targetID string, edited *waProto.Message) protoAction {
	if targetID == "" || edited == nil {
		return protoAction{Kind: actionDrop, Reason: "edit without target or content"}
	}
	return protoAction{Kind: actionEdit, TargetID: targetID, Edited: edited}
}

// ---------------------------------------------------------------------------
// Reactions

// reactionOf returns the reaction carried by a message, decrypting an
// EncReactionMessage when needed. ok is false for non-reactions; err is set
// when a reaction exists but cannot be read (the caller drops it).
func reactionOf(ctx context.Context, evt *events.Message,
	decrypt func(context.Context, *events.Message) (*waProto.ReactionMessage, error),
) (rm *waProto.ReactionMessage, ok bool, err error) {
	m := unwrapMessage(evt.Message)
	if r := m.GetReactionMessage(); r != nil {
		return r, true, nil
	}
	if m.GetEncReactionMessage() == nil {
		return nil, false, nil
	}
	if decrypt == nil {
		return nil, true, fmt.Errorf("no reaction decryptor")
	}
	r, err := decrypt(ctx, evt)
	if err != nil {
		return nil, true, err
	}
	// The decrypted payload usually carries only the emoji: the reacted-to
	// message is named by the envelope's TargetMessageKey.
	if r != nil && r.GetKey().GetID() == "" {
		r.Key = m.GetEncReactionMessage().GetTargetMessageKey()
	}
	return r, true, nil
}

// reactionRecord turns a reaction event into the row-level description the
// store understands. Text "" is a removal.
func reactionRecord(info types.MessageInfo, chatJID string, rm *waProto.ReactionMessage) store.Reaction {
	return store.Reaction{
		ID:        info.ID,
		ChatJID:   chatJID,
		SenderJID: nullString(senderNonAD(info.Sender)),
		FromMe:    info.IsFromMe,
		IsGroup:   info.IsGroup,
		TargetID:  rm.GetKey().GetID(),
		Emoji:     rm.GetText(),
		Timestamp: info.Timestamp,
	}
}

// senderNonAD drops the device suffix: the same member reacts from the phone
// (device 0) and WhatsApp Web (device 12) and must match one reaction row.
func senderNonAD(j types.JID) string {
	if j.IsEmpty() {
		return ""
	}
	return j.ToNonAD().String()
}

// ---------------------------------------------------------------------------
// Quotes (replies)

// maxJIDLen is the width of the quoted_sender_jid / sender_jid columns.
const maxJIDLen = 64

// contextInfoGetter is implemented by every content type that can carry a
// reply (they all expose the same ContextInfo field).
type contextInfoGetter interface {
	GetContextInfo() *waProto.ContextInfo
}

// contextInfoOf returns the ContextInfo of whatever content the message
// holds (text, image, video, audio, document, sticker, location, contact,
// buttons, lists, ...), or nil.
func contextInfoOf(msg *waProto.Message) *waProto.ContextInfo {
	m := unwrapMessage(msg)
	if m == nil {
		return nil
	}
	for _, c := range []contextInfoGetter{
		m.GetExtendedTextMessage(),
		m.GetImageMessage(),
		m.GetVideoMessage(),
		m.GetAudioMessage(),
		m.GetDocumentMessage(),
		m.GetStickerMessage(),
		m.GetLocationMessage(),
		m.GetLiveLocationMessage(),
		m.GetContactMessage(),
		m.GetContactsArrayMessage(),
		m.GetButtonsMessage(),
		m.GetButtonsResponseMessage(),
		m.GetListMessage(),
		m.GetListResponseMessage(),
		m.GetTemplateMessage(),
		m.GetTemplateButtonReplyMessage(),
		m.GetInteractiveMessage(),
		m.GetInteractiveResponseMessage(),
		m.GetPollCreationMessage(),
		m.GetGroupInviteMessage(),
		m.GetProductMessage(),
		m.GetOrderMessage(),
	} {
		if ci := c.GetContextInfo(); ci != nil {
			return ci
		}
	}
	return nil
}

// quoteInfo is the quoted_* part of a wa_messages row.
type quoteInfo struct {
	MessageID sql.NullString
	Preview   sql.NullString
	SenderJID sql.NullString
}

// quoteFromContext reads reply data from a ContextInfo. Only a StanzaID makes
// it a reply (ContextInfo also carries forwarding flags, mentions, ...).
func quoteFromContext(ci *waProto.ContextInfo) quoteInfo {
	id := ci.GetStanzaID()
	if id == "" || len(id) > 64 {
		return quoteInfo{}
	}
	q := quoteInfo{
		MessageID: nullString(id),
		Preview:   nullString(quotedPreview(ci.GetQuotedMessage())),
	}
	if p := ci.GetParticipant(); p != "" && len(p) <= maxJIDLen {
		q.SenderJID = nullString(p)
	}
	return q
}

// quotedPreviewMax is the width of wa_messages.quoted_preview.
const quotedPreviewMax = 255

// quotedPreview is the short text shown for a quoted message: its text or
// caption, or a type label when it has none.
func quotedPreview(qm *waProto.Message) string {
	m := unwrapMessage(qm)
	if m == nil {
		return ""
	}
	msgType, body, media := classifyMessage(m)
	if msgType == "system" || (msgType == "other" && strings.HasPrefix(body, "[proto:")) {
		return ""
	}
	filename := ""
	if media != nil {
		filename = media.filename
		if msgType == "location" && media.locationName != "" {
			return truncateRunes(locationLabel(media.locationName), quotedPreviewMax)
		}
	}
	return truncateRunes(quoteLabel(msgType, body, filename), quotedPreviewMax)
}

// quoteLabel names a message for a quote block: its text, else a type label.
func quoteLabel(msgType, body, filename string) string {
	switch msgType {
	case "location":
		return "\U0001F4CD Ubicación"
	case "contact":
		if body != "" {
			return "\U0001F464 " + body
		}
		return "\U0001F464 Contacto"
	}
	if strings.TrimSpace(body) != "" {
		return body
	}
	switch msgType {
	case "image":
		return "\U0001F4F7 Foto"
	case "video":
		return "\U0001F3A5 Video"
	case "audio":
		return "\U0001F3A4 Audio"
	case "document":
		if filename != "" {
			return "\U0001F4C4 " + filename
		}
		return "\U0001F4C4 Documento"
	case "sticker":
		return "Sticker"
	case "revoked":
		return "Mensaje eliminado"
	}
	return ""
}

// participantFor picks the JID a quote or reaction names as the author of the
// target message:
//   - our own message: our own JID (LID form in a LID chat, and in a group
//     where the stored row shows we were addressed by LID);
//   - 1:1 chat: the chat's user;
//   - group: the stored sender, empty when unknown.
func participantFor(row store.QuotedRow, chat, ownPN, ownLID types.JID) types.JID {
	if row.FromMe {
		if chat.Server == types.HiddenUserServer && !ownLID.IsEmpty() {
			return ownLID.ToNonAD()
		}
		if chat.Server == types.GroupServer && !ownLID.IsEmpty() && row.SenderJID.Valid {
			if j, err := types.ParseJID(row.SenderJID.String); err == nil && j.Server == types.HiddenUserServer {
				return ownLID.ToNonAD()
			}
		}
		return ownPN.ToNonAD()
	}
	if chat.Server == types.GroupServer {
		if !row.SenderJID.Valid {
			return types.EmptyJID
		}
		j, err := types.ParseJID(row.SenderJID.String)
		if err != nil {
			return types.EmptyJID
		}
		return j.ToNonAD()
	}
	return chat.ToNonAD()
}

// buildQuoteContext builds the ContextInfo of an outbound reply. The quoted
// content is synthesized from what we stored (WhatsApp shows it in the quote
// block): the text, or a type label for media.
func buildQuoteContext(stanzaID string, participant types.JID, row store.QuotedRow) *waProto.ContextInfo {
	text := quoteLabel(row.MessageType, row.Body.String, row.MediaFilename.String)
	ci := &waProto.ContextInfo{
		StanzaID:      proto.String(stanzaID),
		QuotedMessage: &waProto.Message{Conversation: proto.String(text)},
	}
	if !participant.IsEmpty() {
		ci.Participant = proto.String(participant.String())
	}
	return ci
}

// attachQuote puts a reply ContextInfo on an outbound message. A plain text
// Conversation becomes an ExtendedTextMessage (the only text form that can
// carry a context). Unknown shapes are returned unchanged.
func attachQuote(msg *waProto.Message, ci *waProto.ContextInfo) *waProto.Message {
	if msg == nil || ci == nil {
		return msg
	}
	switch {
	case msg.Conversation != nil:
		return &waProto.Message{ExtendedTextMessage: &waProto.ExtendedTextMessage{
			Text:        msg.Conversation,
			ContextInfo: ci,
		}}
	case msg.ExtendedTextMessage != nil:
		msg.ExtendedTextMessage.ContextInfo = ci
	case msg.ImageMessage != nil:
		msg.ImageMessage.ContextInfo = ci
	case msg.VideoMessage != nil:
		msg.VideoMessage.ContextInfo = ci
	case msg.AudioMessage != nil:
		msg.AudioMessage.ContextInfo = ci
	case msg.DocumentMessage != nil:
		msg.DocumentMessage.ContextInfo = ci
	case msg.LocationMessage != nil:
		msg.LocationMessage.ContextInfo = ci
	case msg.DocumentWithCaptionMessage.GetMessage().GetDocumentMessage() != nil:
		msg.DocumentWithCaptionMessage.GetMessage().GetDocumentMessage().ContextInfo = ci
	}
	return msg
}

// ---------------------------------------------------------------------------
// Small text helpers

// truncateRunes cuts s to at most n characters without splitting a UTF-8
// sequence (MySQL rejects invalid utf8mb4).
func truncateRunes(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n])
}

// asciiOnly replaces every non-ASCII byte so log lines stay readable on the
// Windows console.
func asciiOnly(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r < 0x20 || r > 0x7e {
			b.WriteByte('?')
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}
