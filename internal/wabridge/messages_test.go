package wabridge

import (
	"context"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/enviadores/wabridge/internal/store"
	"go.mau.fi/whatsmeow/proto/waCommon"
	waProto "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func key(id string) *waCommon.MessageKey { return &waCommon.MessageKey{ID: proto.String(id)} }

func editProto(target, text string) *waProto.Message {
	return &waProto.Message{ProtocolMessage: &waProto.ProtocolMessage{
		Type:          waProto.ProtocolMessage_MESSAGE_EDIT.Enum(),
		Key:           key(target),
		EditedMessage: &waProto.Message{Conversation: proto.String(text)},
	}}
}

func evtOf(m *waProto.Message) *events.Message {
	return &events.Message{Message: m, RawMessage: m}
}

// ---------------------------------------------------------------------------
// Edits

func TestResolveActionPlainProtocolEdit(t *testing.T) {
	act := resolveAction(context.Background(), evtOf(editProto("ORIG", "Calle 5 #12")), nil)
	if act.Kind != actionEdit || act.TargetID != "ORIG" {
		t.Fatalf("got %+v", act)
	}
	_, body, _ := classifyMessage(act.Edited)
	if body != "Calle 5 #12" {
		t.Fatalf("edited body %q", body)
	}
}

func TestResolveActionEditStillWrappedInEditedMessage(t *testing.T) {
	wrapped := &waProto.Message{EditedMessage: &waProto.FutureProofMessage{Message: editProto("ORIG", "nuevo")}}
	act := resolveAction(context.Background(), evtOf(wrapped), nil)
	if act.Kind != actionEdit || act.TargetID != "ORIG" {
		t.Fatalf("got %+v", act)
	}
}

func secretEvt(target string) *events.Message {
	return evtOf(&waProto.Message{SecretEncryptedMessage: &waProto.SecretEncryptedMessage{
		TargetMessageKey: key(target),
		SecretEncType:    waProto.SecretEncryptedMessage_MESSAGE_EDIT.Enum(),
	}})
}

func TestResolveActionSecretEditDecryptedAsProtocolMessage(t *testing.T) {
	// Shape 1: the plaintext is a Message holding ProtocolMessage{MESSAGE_EDIT}.
	dec := func(context.Context, *events.Message) (*waProto.Message, error) {
		return editProto("ORIG", "texto nuevo"), nil
	}
	act := resolveAction(context.Background(), secretEvt("IGNORED-FALLBACK"), dec)
	if act.Kind != actionEdit || act.TargetID != "ORIG" {
		t.Fatalf("got %+v", act)
	}
	if _, body, _ := classifyMessage(act.Edited); body != "texto nuevo" {
		t.Fatalf("body %q", body)
	}
}

func TestResolveActionSecretEditProtocolMessageWithoutKeyUsesTargetKey(t *testing.T) {
	dec := func(context.Context, *events.Message) (*waProto.Message, error) {
		m := editProto("", "x")
		m.ProtocolMessage.Key = nil
		return m, nil
	}
	act := resolveAction(context.Background(), secretEvt("FROM-ENVELOPE"), dec)
	if act.Kind != actionEdit || act.TargetID != "FROM-ENVELOPE" {
		t.Fatalf("got %+v", act)
	}
}

func TestResolveActionSecretEditDecryptedAsBareContent(t *testing.T) {
	// Shape 2: the plaintext is the bare new content; the target is the
	// envelope's TargetMessageKey.
	dec := func(context.Context, *events.Message) (*waProto.Message, error) {
		return &waProto.Message{ExtendedTextMessage: &waProto.ExtendedTextMessage{Text: proto.String("bare")}}, nil
	}
	act := resolveAction(context.Background(), secretEvt("ORIG"), dec)
	if act.Kind != actionEdit || act.TargetID != "ORIG" {
		t.Fatalf("got %+v", act)
	}
	if _, body, _ := classifyMessage(act.Edited); body != "bare" {
		t.Fatalf("body %q", body)
	}
}

func TestResolveActionSecretEditDecryptFailureIsDropped(t *testing.T) {
	dec := func(context.Context, *events.Message) (*waProto.Message, error) {
		return nil, errors.New("original message secret key not found")
	}
	act := resolveAction(context.Background(), secretEvt("ORIG"), dec)
	if act.Kind != actionDrop || !strings.Contains(act.Reason, "not decrypted") {
		t.Fatalf("got %+v", act)
	}
	// And it is ASCII, so it is safe for the Windows log.
	for _, r := range act.Reason {
		if r > 0x7e {
			t.Fatalf("reason not ASCII: %q", act.Reason)
		}
	}
}

func TestResolveActionOtherSecretTypesAreDroppedWithoutDecrypting(t *testing.T) {
	called := false
	dec := func(context.Context, *events.Message) (*waProto.Message, error) { called = true; return nil, nil }
	evt := evtOf(&waProto.Message{SecretEncryptedMessage: &waProto.SecretEncryptedMessage{
		TargetMessageKey: key("P"),
		SecretEncType:    waProto.SecretEncryptedMessage_POLL_EDIT.Enum(),
	}})
	if act := resolveAction(context.Background(), evt, dec); act.Kind != actionDrop || called {
		t.Fatalf("got %+v, decrypt called=%v", act, called)
	}
}

func TestResolveActionEditWithoutContentOrTargetIsDropped(t *testing.T) {
	m := editProto("ORIG", "x")
	m.ProtocolMessage.EditedMessage = nil
	if act := resolveAction(context.Background(), evtOf(m), nil); act.Kind != actionDrop {
		t.Fatalf("no content: %+v", act)
	}
	m = editProto("", "x")
	m.ProtocolMessage.Key = nil
	if act := resolveAction(context.Background(), evtOf(m), nil); act.Kind != actionDrop {
		t.Fatalf("no target: %+v", act)
	}
}

// ---------------------------------------------------------------------------
// Revokes

func TestResolveActionRevokeTargetsKeyID(t *testing.T) {
	evt := evtOf(&waProto.Message{ProtocolMessage: &waProto.ProtocolMessage{
		Type: waProto.ProtocolMessage_REVOKE.Enum(),
		Key:  key("DELETED-ONE"),
	}})
	act := resolveAction(context.Background(), evt, nil)
	if act.Kind != actionRevoke || act.TargetID != "DELETED-ONE" {
		t.Fatalf("got %+v", act)
	}
	// A revoke without a key cannot be targeted: keep the old 'revoked' row.
	evt.Message.ProtocolMessage.Key = nil
	if act := resolveAction(context.Background(), evt, nil); act.Kind != actionNone {
		t.Fatalf("keyless revoke: %+v", act)
	}
}

func TestResolveActionOrdinaryMessagesAreNone(t *testing.T) {
	for name, m := range map[string]*waProto.Message{
		"text":     {Conversation: proto.String("hola")},
		"reaction": {ReactionMessage: &waProto.ReactionMessage{Key: key("X"), Text: proto.String("x")}},
		"ephemeral setting": {ProtocolMessage: &waProto.ProtocolMessage{
			Type: waProto.ProtocolMessage_EPHEMERAL_SETTING.Enum()}},
	} {
		if act := resolveAction(context.Background(), evtOf(m), nil); act.Kind != actionNone {
			t.Errorf("%s: %+v", name, act)
		}
	}
	if act := resolveAction(context.Background(), &events.Message{}, nil); act.Kind != actionNone {
		t.Errorf("nil message: %+v", act)
	}
}

// ---------------------------------------------------------------------------
// Reactions

func TestReactionOfPlainAndEncrypted(t *testing.T) {
	plain := evtOf(&waProto.Message{ReactionMessage: &waProto.ReactionMessage{Key: key("T"), Text: proto.String("x")}})
	rm, ok, err := reactionOf(context.Background(), plain, nil)
	if !ok || err != nil || rm.GetText() != "x" {
		t.Fatalf("plain: %v %v %v", rm, ok, err)
	}

	enc := evtOf(&waProto.Message{EncReactionMessage: &waProto.EncReactionMessage{TargetMessageKey: key("T")}})
	good := func(context.Context, *events.Message) (*waProto.ReactionMessage, error) {
		return &waProto.ReactionMessage{Key: key("T"), Text: proto.String("y")}, nil
	}
	rm, ok, err = reactionOf(context.Background(), enc, good)
	if !ok || err != nil || rm.GetText() != "y" {
		t.Fatalf("encrypted: %v %v %v", rm, ok, err)
	}

	// The decrypted payload usually has no Key: the envelope names the target.
	keyless := func(context.Context, *events.Message) (*waProto.ReactionMessage, error) {
		return &waProto.ReactionMessage{Text: proto.String("z")}, nil
	}
	rm, ok, err = reactionOf(context.Background(), enc, keyless)
	if !ok || err != nil || rm.GetKey().GetID() != "T" || rm.GetText() != "z" {
		t.Fatalf("keyless encrypted: %v %v %v", rm, ok, err)
	}

	bad := func(context.Context, *events.Message) (*waProto.ReactionMessage, error) {
		return nil, errors.New("nope")
	}
	if _, ok, err = reactionOf(context.Background(), enc, bad); !ok || err == nil {
		t.Fatalf("decrypt failure must be reported: ok=%v err=%v", ok, err)
	}

	if _, ok, _ := reactionOf(context.Background(), evtOf(&waProto.Message{Conversation: proto.String("hi")}), nil); ok {
		t.Fatal("text is not a reaction")
	}
}

func TestReactionRecordCarriesTargetAndEmptyMeansRemoval(t *testing.T) {
	info := types.MessageInfo{ID: "R1"}
	info.IsFromMe = false
	info.IsGroup = true
	info.Sender = types.JID{User: "5215511111111", Server: types.DefaultUserServer, Device: 12}
	rec := reactionRecord(info, "chat@g.us", &waProto.ReactionMessage{Key: key("TARGET"), Text: proto.String("")})
	if rec.TargetID != "TARGET" || rec.Emoji != "" || rec.ChatJID != "chat@g.us" || !rec.IsGroup ||
		rec.SenderJID.String != "5215511111111@s.whatsapp.net" || rec.ID != "R1" {
		t.Fatalf("got %+v", rec)
	}
}

// ---------------------------------------------------------------------------
// ContextInfo extraction per content type

func ctxWith(id, part string, quoted *waProto.Message) *waProto.ContextInfo {
	return &waProto.ContextInfo{StanzaID: proto.String(id), Participant: proto.String(part), QuotedMessage: quoted}
}

func TestContextInfoExtractedFromEveryContentType(t *testing.T) {
	q := &waProto.Message{Conversation: proto.String("citado")}
	ci := func() *waProto.ContextInfo { return ctxWith("QID", "5215500000000@s.whatsapp.net", q) }
	cases := map[string]*waProto.Message{
		"extended text":    {ExtendedTextMessage: &waProto.ExtendedTextMessage{Text: proto.String("r"), ContextInfo: ci()}},
		"image":            {ImageMessage: &waProto.ImageMessage{ContextInfo: ci()}},
		"video":            {VideoMessage: &waProto.VideoMessage{ContextInfo: ci()}},
		"audio":            {AudioMessage: &waProto.AudioMessage{ContextInfo: ci()}},
		"document":         {DocumentMessage: &waProto.DocumentMessage{ContextInfo: ci()}},
		"sticker":          {StickerMessage: &waProto.StickerMessage{ContextInfo: ci()}},
		"location":         {LocationMessage: &waProto.LocationMessage{ContextInfo: ci()}},
		"contact":          {ContactMessage: &waProto.ContactMessage{ContextInfo: ci()}},
		"contacts array":   {ContactsArrayMessage: &waProto.ContactsArrayMessage{ContextInfo: ci()}},
		"buttons response": {ButtonsResponseMessage: &waProto.ButtonsResponseMessage{ContextInfo: ci()}},
		"list response":    {ListResponseMessage: &waProto.ListResponseMessage{ContextInfo: ci()}},
		"document with caption": {DocumentWithCaptionMessage: &waProto.FutureProofMessage{Message: &waProto.Message{
			DocumentMessage: &waProto.DocumentMessage{ContextInfo: ci()}}}},
		"view once image": {ViewOnceMessageV2: &waProto.FutureProofMessage{Message: &waProto.Message{
			ImageMessage: &waProto.ImageMessage{ContextInfo: ci()}}}},
		"ephemeral text": {EphemeralMessage: &waProto.FutureProofMessage{Message: &waProto.Message{
			ExtendedTextMessage: &waProto.ExtendedTextMessage{ContextInfo: ci()}}}},
		"device sent": {DeviceSentMessage: &waProto.DeviceSentMessage{Message: &waProto.Message{
			ExtendedTextMessage: &waProto.ExtendedTextMessage{ContextInfo: ci()}}}},
		"album child image": {AssociatedChildMessage: &waProto.FutureProofMessage{Message: &waProto.Message{
			ImageMessage: &waProto.ImageMessage{ContextInfo: ci()}}}},
	}
	for name, m := range cases {
		got := quoteFromContext(contextInfoOf(m))
		if got.MessageID.String != "QID" || got.SenderJID.String != "5215500000000@s.whatsapp.net" || got.Preview.String != "citado" {
			t.Errorf("%s: got %+v", name, got)
		}
	}
}

func TestQuoteFromContextIgnoresContextWithoutStanzaID(t *testing.T) {
	// ContextInfo also carries mentions / forwarding flags: not a reply.
	m := &waProto.Message{ExtendedTextMessage: &waProto.ExtendedTextMessage{
		ContextInfo: &waProto.ContextInfo{IsForwarded: proto.Bool(true)}}}
	if q := quoteFromContext(contextInfoOf(m)); q.MessageID.Valid || q.Preview.Valid || q.SenderJID.Valid {
		t.Fatalf("got %+v", q)
	}
	if contextInfoOf(&waProto.Message{Conversation: proto.String("plain")}) != nil {
		t.Fatal("a plain conversation has no context")
	}
	if contextInfoOf(nil) != nil {
		t.Fatal("nil message")
	}
}

func TestQuotedPreviewTextCaptionAndTypeLabels(t *testing.T) {
	cases := []struct {
		name string
		msg  *waProto.Message
		want string
	}{
		{"text", &waProto.Message{Conversation: proto.String("hola")}, "hola"},
		{"extended text", &waProto.Message{ExtendedTextMessage: &waProto.ExtendedTextMessage{Text: proto.String("link")}}, "link"},
		{"image caption wins", &waProto.Message{ImageMessage: &waProto.ImageMessage{Caption: proto.String("mi guia")}}, "mi guia"},
		{"image", &waProto.Message{ImageMessage: &waProto.ImageMessage{}}, "\U0001F4F7 Foto"},
		{"video", &waProto.Message{VideoMessage: &waProto.VideoMessage{}}, "\U0001F3A5 Video"},
		{"audio", &waProto.Message{AudioMessage: &waProto.AudioMessage{}}, "\U0001F3A4 Audio"},
		{"document with filename", &waProto.Message{DocumentMessage: &waProto.DocumentMessage{FileName: proto.String("guia.pdf")}}, "\U0001F4C4 guia.pdf"},
		{"document without filename", &waProto.Message{DocumentMessage: &waProto.DocumentMessage{}}, "\U0001F4C4 Documento"},
		{"document with caption wrapper", &waProto.Message{DocumentWithCaptionMessage: &waProto.FutureProofMessage{Message: &waProto.Message{
			DocumentMessage: &waProto.DocumentMessage{FileName: proto.String("x.pdf"), Caption: proto.String("recibo")}}}}, "recibo"},
		{"sticker", &waProto.Message{StickerMessage: &waProto.StickerMessage{}}, "Sticker"},
		{"location", &waProto.Message{LocationMessage: &waProto.LocationMessage{DegreesLatitude: proto.Float64(19.4), DegreesLongitude: proto.Float64(-99.1)}}, "\U0001F4CD Ubicación"},
		{"contact", &waProto.Message{ContactMessage: &waProto.ContactMessage{DisplayName: proto.String("Ana")}}, "\U0001F464 Ana"},
		{"nothing", nil, ""},
		{"unknown protocol", &waProto.Message{ProtocolMessage: &waProto.ProtocolMessage{Type: waProto.ProtocolMessage_BOT_FEEDBACK_MESSAGE.Enum()}}, ""},
	}
	for _, c := range cases {
		if got := quotedPreview(c.msg); got != c.want {
			t.Errorf("%s: got %q want %q", c.name, got, c.want)
		}
	}
}

func TestQuotedPreviewTruncatesToColumnWidthWithoutBreakingUTF8(t *testing.T) {
	long := strings.Repeat("á\U0001F4E6", 200) // 400 runes, 1000+ bytes
	got := quotedPreview(&waProto.Message{Conversation: proto.String(long)})
	if n := utf8.RuneCountInString(got); n != 255 {
		t.Fatalf("got %d runes, want 255", n)
	}
	if !utf8.ValidString(got) {
		t.Fatal("truncation produced invalid UTF-8")
	}
}

func TestQuoteFromContextDropsOversizedFields(t *testing.T) {
	ci := ctxWith(strings.Repeat("A", 65), "x", nil)
	if q := quoteFromContext(ci); q.MessageID.Valid {
		t.Fatal("stanza id wider than the column must be dropped")
	}
	ci = ctxWith("ID", strings.Repeat("9", 70), nil)
	if q := quoteFromContext(ci); q.SenderJID.Valid || q.MessageID.String != "ID" {
		t.Fatalf("got %+v", q)
	}
}

// ---------------------------------------------------------------------------
// Outbound quote building

var (
	ownPN  = types.NewJID("5215599999999", types.DefaultUserServer)
	ownLID = types.NewJID("11111111111111", types.HiddenUserServer)
)

func TestParticipantFor(t *testing.T) {
	dm := types.NewJID("5215511111111", types.DefaultUserServer)
	lidDM := types.NewJID("22222222222222", types.HiddenUserServer)
	group := types.NewJID("120363000000000000", types.GroupServer)

	// Our own message: our JID (device suffix stripped), LID form in a LID chat.
	ownAD := types.JID{User: "5215599999999", Server: types.DefaultUserServer, Device: 12}
	if got := participantFor(store.QuotedRow{FromMe: true}, dm, ownAD, ownLID); got.String() != ownPN.String() {
		t.Errorf("own in PN chat: %s", got)
	}
	if got := participantFor(store.QuotedRow{FromMe: true}, lidDM, ownPN, ownLID); got.String() != ownLID.String() {
		t.Errorf("own in LID chat: %s", got)
	}
	if got := participantFor(store.QuotedRow{FromMe: true}, lidDM, ownPN, types.EmptyJID); got.String() != ownPN.String() {
		t.Errorf("own in LID chat without LID: %s", got)
	}
	// Inbound 1:1: the chat's user.
	if got := participantFor(store.QuotedRow{}, dm, ownPN, ownLID); got.String() != dm.String() {
		t.Errorf("inbound 1:1: %s", got)
	}
	// Inbound group: the stored sender, no device; unknown when missing or bad.
	sender := store.QuotedRow{SenderJID: nullString("5215522222222:7@s.whatsapp.net")}
	if got := participantFor(sender, group, ownPN, ownLID); got.String() != "5215522222222@s.whatsapp.net" {
		t.Errorf("group: %s", got)
	}
	if got := participantFor(store.QuotedRow{}, group, ownPN, ownLID); !got.IsEmpty() {
		t.Errorf("group without sender: %s", got)
	}
	if got := participantFor(store.QuotedRow{SenderJID: nullString("not a jid@@@")}, group, ownPN, ownLID); !got.IsEmpty() {
		t.Errorf("group with bad sender: %s", got)
	}
	// Our own message in a group: LID when the stored row shows LID addressing.
	if got := participantFor(store.QuotedRow{FromMe: true, SenderJID: nullString("11111111111111:3@lid")}, group, ownPN, ownLID); got.String() != ownLID.String() {
		t.Errorf("own in LID group: %s", got)
	}
	if got := participantFor(store.QuotedRow{FromMe: true, SenderJID: nullString("5215599999999:3@s.whatsapp.net")}, group, ownPN, ownLID); got.String() != ownPN.String() {
		t.Errorf("own in PN group: %s", got)
	}
}

func TestBuildQuoteContextSynthesizesQuotedMessage(t *testing.T) {
	p := types.NewJID("5215511111111", types.DefaultUserServer)
	ci := buildQuoteContext("QID", p, store.QuotedRow{MessageType: "text", Body: nullString("hola, tu guia sale hoy")})
	if ci.GetStanzaID() != "QID" || ci.GetParticipant() != p.String() ||
		ci.GetQuotedMessage().GetConversation() != "hola, tu guia sale hoy" {
		t.Fatalf("text: %v", ci)
	}
	ci = buildQuoteContext("QID", p, store.QuotedRow{MessageType: "image"})
	if ci.GetQuotedMessage().GetConversation() != "\U0001F4F7 Foto" {
		t.Fatalf("image fallback label: %v", ci)
	}
	ci = buildQuoteContext("QID", p, store.QuotedRow{MessageType: "document", MediaFilename: nullString("guia.pdf")})
	if ci.GetQuotedMessage().GetConversation() != "\U0001F4C4 guia.pdf" {
		t.Fatalf("document label: %v", ci)
	}
	if ci := buildQuoteContext("QID", types.EmptyJID, store.QuotedRow{}); ci.Participant != nil {
		t.Fatal("no participant must stay unset")
	}
}

func TestAttachQuoteTextBecomesExtendedText(t *testing.T) {
	ci := ctxWith("QID", "p@s.whatsapp.net", nil)
	got := attachQuote(&waProto.Message{Conversation: proto.String("respuesta")}, ci)
	if got.Conversation != nil || got.GetExtendedTextMessage().GetText() != "respuesta" ||
		got.GetExtendedTextMessage().GetContextInfo().GetStanzaID() != "QID" {
		t.Fatalf("got %v", got)
	}
}

func TestAttachQuoteMediaKinds(t *testing.T) {
	ci := ctxWith("QID", "p@s.whatsapp.net", nil)
	cases := map[string]*waProto.Message{
		"image":    {ImageMessage: &waProto.ImageMessage{}},
		"video":    {VideoMessage: &waProto.VideoMessage{}},
		"audio":    {AudioMessage: &waProto.AudioMessage{}},
		"document": {DocumentMessage: &waProto.DocumentMessage{}},
		"document with caption": {DocumentWithCaptionMessage: &waProto.FutureProofMessage{
			Message: &waProto.Message{DocumentMessage: &waProto.DocumentMessage{}}}},
	}
	for name, m := range cases {
		out := attachQuote(m, ci)
		if got := contextInfoOf(out).GetStanzaID(); got != "QID" {
			t.Errorf("%s: context not attached (got %q)", name, got)
		}
	}
	// Round trip: what we attach is what the inbound reader sees.
	out := attachQuote(&waProto.Message{Conversation: proto.String("x")}, buildQuoteContext("QID",
		types.NewJID("5215511111111", types.DefaultUserServer), store.QuotedRow{MessageType: "text", Body: nullString("orig")}))
	q := quoteFromContext(contextInfoOf(out))
	if q.MessageID.String != "QID" || q.Preview.String != "orig" || q.SenderJID.String != "5215511111111@s.whatsapp.net" {
		t.Fatalf("round trip: %+v", q)
	}
	// nil context leaves the message alone.
	m := &waProto.Message{Conversation: proto.String("x")}
	if attachQuote(m, nil) != m {
		t.Fatal("nil context must be a no-op")
	}
}

// ---------------------------------------------------------------------------
// Noise, classification, helpers

func TestIsSkippedChat(t *testing.T) {
	cases := []struct {
		jid  types.JID
		want bool
	}{
		{types.NewJID("status", types.BroadcastServer), true},
		{types.NewJID("867051314767696", types.BotServer), true},
		{types.NewJID("5215511111111", types.DefaultUserServer), false},
		{types.NewJID("22222222222222", types.HiddenUserServer), false},
		{types.NewJID("120363000000000000", types.GroupServer), false},
		{types.NewJID("12345", types.BroadcastServer), false},
	}
	for _, c := range cases {
		if got := isSkippedChat(c.jid); got != c.want {
			t.Errorf("%s: got %v", c.jid, got)
		}
	}
}

func TestClassifyProtocolSubtypes(t *testing.T) {
	system := []waProto.ProtocolMessage_Type{
		waProto.ProtocolMessage_EPHEMERAL_SETTING,
		waProto.ProtocolMessage_HISTORY_SYNC_NOTIFICATION,
		waProto.ProtocolMessage_APP_STATE_SYNC_KEY_SHARE,
		waProto.ProtocolMessage_APP_STATE_SYNC_KEY_REQUEST,
		waProto.ProtocolMessage_APP_STATE_FATAL_EXCEPTION_NOTIFICATION,
		waProto.ProtocolMessage_LID_MIGRATION_MAPPING_SYNC,
		waProto.ProtocolMessage_MESSAGE_EDIT, // never stored as its own message
	}
	for _, st := range system {
		if typ, body, _ := classifyMessage(&waProto.Message{ProtocolMessage: &waProto.ProtocolMessage{Type: st.Enum()}}); typ != "system" || body != "" {
			t.Errorf("%s: %q %q", st, typ, body)
		}
	}
	typ, body, _ := classifyMessage(&waProto.Message{ProtocolMessage: &waProto.ProtocolMessage{Type: waProto.ProtocolMessage_BOT_FEEDBACK_MESSAGE.Enum()}})
	if typ != "other" || body != "[proto: protocolMessage/BOT_FEEDBACK_MESSAGE]" {
		t.Errorf("unknown subtype: %q %q", typ, body)
	}
	if typ, _, _ := classifyMessage(&waProto.Message{ProtocolMessage: &waProto.ProtocolMessage{Type: waProto.ProtocolMessage_REVOKE.Enum()}}); typ != "revoked" {
		t.Errorf("revoke: %q", typ)
	}
}

func TestClassifyAlbumsAndUndecryptables(t *testing.T) {
	if typ, _, _ := classifyMessage(&waProto.Message{AlbumMessage: &waProto.AlbumMessage{}}); typ != "system" {
		t.Errorf("album header: %q", typ)
	}
	child := &waProto.Message{AssociatedChildMessage: &waProto.FutureProofMessage{Message: &waProto.Message{
		ImageMessage: &waProto.ImageMessage{Caption: proto.String("foto 2 de 3"), Mimetype: proto.String("image/jpeg")}}}}
	typ, body, media := classifyMessage(child)
	if typ != "image" || body != "foto 2 de 3" || media == nil || media.mime != "image/jpeg" {
		t.Errorf("album child: %q %q %v", typ, body, media)
	}
	for name, m := range map[string]*waProto.Message{
		"secret encrypted": {SecretEncryptedMessage: &waProto.SecretEncryptedMessage{}},
		"enc reaction":     {EncReactionMessage: &waProto.EncReactionMessage{}},
	} {
		if typ, _, _ := classifyMessage(m); typ != "system" {
			t.Errorf("%s: %q", name, typ)
		}
	}
	if typ, body, _ := classifyMessage(&waProto.Message{ReactionMessage: &waProto.ReactionMessage{Text: proto.String("\U0001F44D")}}); typ != "reaction" || body != "\U0001F44D" {
		t.Errorf("reaction: %q %q", typ, body)
	}
}

func TestTruncateIsRuneSafe(t *testing.T) {
	s := strings.Repeat("ñ", 300)
	got := truncate(s, 160)
	if !utf8.ValidString(got) || utf8.RuneCountInString(got) != 161 { // 160 + ellipsis
		t.Fatalf("got %d runes, valid=%v", utf8.RuneCountInString(got), utf8.ValidString(got))
	}
	if truncate("corto", 160) != "corto" {
		t.Fatal("short strings are untouched")
	}
}

func TestAsciiOnly(t *testing.T) {
	if got := asciiOnly("falló: ok\n"); got != "fall?: ok?" {
		t.Fatalf("got %q", got)
	}
}
