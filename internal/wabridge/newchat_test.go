package wabridge

import (
	"context"
	"database/sql/driver"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/enviadores/wabridge/internal/testdb"
	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
)

var (
	custLID = types.NewJID("123456789012345", types.HiddenUserServer)
	custPN  = types.NewJID("5215512345678", types.DefaultUserServer)
	sentAt  = time.Date(2026, 10, 17, 18, 0, 0, 0, time.UTC)
)

// resolveRows answers the status='resolve' SELECT with the given rows, one
// per call, then nothing. Every other statement succeeds unless fail says so.
type resolveDB struct {
	rows [][]driver.Value
	fail func(c testdb.Call) error
}

func (r *resolveDB) handler(c testdb.Call) testdb.Result {
	if r.fail != nil {
		if err := r.fail(c); err != nil {
			return testdb.Result{Err: err}
		}
	}
	if c.IsQuery && strings.Contains(c.Query, "WHERE status = 'resolve'") {
		res := testdb.Result{Cols: []string{"id", "client_request_id", "to_phone", "to_name", "body", "kind", "attempts", "whatsapp_message_id", "expired"}}
		if len(r.rows) > 0 {
			res.Rows = [][]driver.Value{r.rows[0]}
			r.rows = r.rows[1:]
		}
		return res
	}
	return testdb.Result{}
}

func resolveRow(id int64, phone string, attempts int64) []driver.Value {
	return []driver.Value{id, "req-" + phone, phone, "Ana Lopez", "Hola Ana", "text", attempts, nil, int64(0)}
}

const (
	colWAMessageID = 7
	colExpired     = 8
)

type stubWA struct {
	lookups []string
	sends   []types.JID
	sendIDs []types.MessageID
	res     []types.IsOnWhatsAppResponse
	lookErr error
	sendErr error
}

func newChatBridge(rdb *resolveDB, wa *stubWA) (*Bridge, *testdb.DB, *time.Time) {
	b, db := newTestBridge(rdb.handler)
	clock := sentAt
	b.now = func() time.Time { return clock }
	b.isOnWhatsApp = func(_ context.Context, phones []string) ([]types.IsOnWhatsAppResponse, error) {
		wa.lookups = append(wa.lookups, phones...)
		return wa.res, wa.lookErr
	}
	b.sendMessage = func(_ context.Context, to types.JID, msg *waProto.Message, extra ...whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
		if len(extra) != 1 || extra[0].ID == "" {
			panic("new chat must send with the message ID claimed on the row")
		}
		wa.sends = append(wa.sends, to)
		wa.sendIDs = append(wa.sendIDs, extra[0].ID)
		if wa.sendErr != nil {
			return whatsmeow.SendResponse{}, wa.sendErr
		}
		if msg.GetConversation() == "" {
			panic("new chat must send the body as Conversation")
		}
		return whatsmeow.SendResponse{ID: extra[0].ID, Timestamp: sentAt}, nil
	}
	b.newMessageID = func() types.MessageID { return "3EB0NEWCHAT" }
	b.lidForPN = func(_ context.Context, pn types.JID) (types.JID, error) {
		if pn == custPN {
			return custLID, nil
		}
		return types.EmptyJID, nil
	}
	return b, db, &clock
}

func inLID() []types.IsOnWhatsAppResponse {
	return []types.IsOnWhatsAppResponse{{Query: "+525512345678", JID: custLID, IsIn: true, PhoneNumber: custPN}}
}

// writes returns the statements after the resolve SELECT, as short labels.
func writes(db *testdb.DB) []string {
	var out []string
	for _, c := range db.Calls() {
		switch {
		case c.IsQuery:
			continue
		case strings.HasPrefix(c.Query, "INSERT INTO wa_chats"):
			out = append(out, "chat")
		case strings.HasPrefix(c.Query, "INSERT IGNORE INTO wa_messages"):
			out = append(out, "message")
		case strings.Contains(c.Query, "SET whatsapp_message_id = ?"):
			out = append(out, "claim")
		case strings.Contains(c.Query, "SET chat_jid = ?"):
			out = append(out, "chat_jid")
		case strings.Contains(c.Query, "SET status = 'sent'"):
			out = append(out, "sent")
		case strings.Contains(c.Query, "SET status = 'failed'"):
			out = append(out, "failed")
		case strings.Contains(c.Query, "SET attempts = ?"):
			out = append(out, "attempt")
		default:
			out = append(out, c.Query)
		}
	}
	return out
}

func failedCall(t *testing.T, db *testdb.DB) testdb.Call {
	t.Helper()
	fs := calls(db, "SET status = 'failed'")
	if len(fs) != 1 {
		t.Fatalf("one failed UPDATE expected: %v", writes(db))
	}
	return fs[0]
}

func TestNewChatNotOnWhatsAppFailsWithoutChatRow(t *testing.T) {
	rdb := &resolveDB{rows: [][]driver.Value{resolveRow(1, "+525512345678", 0)}}
	wa := &stubWA{res: []types.IsOnWhatsAppResponse{{Query: "+525512345678", JID: custPN, IsIn: false}}}
	b, db, _ := newChatBridge(rdb, wa)
	b.processNewChats(context.Background())

	f := failedCall(t, db)
	if f.Args[2] != codeNotOnWhatsApp || !strings.HasPrefix(f.Args[1].(string), "NOT_ON_WHATSAPP: ") || f.Args[3] != int64(1) {
		t.Fatalf("failed args %v", f.Args)
	}
	if len(calls(db, "wa_chats")) != 0 || len(calls(db, "wa_messages")) != 0 {
		t.Fatalf("no chat or message may be written: %v", writes(db))
	}
	if len(wa.sends) != 0 {
		t.Fatal("nothing may be sent")
	}
	if len(wa.lookups) != 1 || wa.lookups[0] != "+525512345678" {
		t.Fatalf("lookup %v", wa.lookups)
	}
}

func TestNewChatEmptyLookupIsNotOnWhatsApp(t *testing.T) {
	rdb := &resolveDB{rows: [][]driver.Value{resolveRow(1, "+525512345678", 0)}}
	wa := &stubWA{}
	b, db, _ := newChatBridge(rdb, wa)
	b.processNewChats(context.Background())
	if f := failedCall(t, db); f.Args[2] != codeNotOnWhatsApp {
		t.Fatalf("code %v", f.Args[2])
	}
	if len(wa.sends) != 0 || len(calls(db, "wa_chats")) != 0 {
		t.Fatal("no send, no chat")
	}
}

func TestNewChatInvalidPhoneFailsPermanently(t *testing.T) {
	for _, phone := range []string{"+52551234567", "+15551234567", "", "5512345678", "+5215512345678"} {
		t.Run(phone, func(t *testing.T) {
			rdb := &resolveDB{rows: [][]driver.Value{resolveRow(1, phone, 0)}}
			wa := &stubWA{res: inLID()}
			b, db, _ := newChatBridge(rdb, wa)
			b.processNewChats(context.Background())
			f := failedCall(t, db)
			if f.Args[2] != codeInvalidPhone || f.Args[0] != int64(outboundMaxAttempts) {
				t.Fatalf("args %v", f.Args)
			}
			if len(wa.lookups) != 0 || len(wa.sends) != 0 {
				t.Fatal("an invalid phone is never looked up or sent")
			}
		})
	}
}

func TestNewChatNullPhoneFailsPermanently(t *testing.T) {
	row := resolveRow(1, "", 0)
	row[2] = nil
	rdb := &resolveDB{rows: [][]driver.Value{row}}
	b, db, _ := newChatBridge(rdb, &stubWA{res: inLID()})
	b.processNewChats(context.Background())
	if f := failedCall(t, db); f.Args[2] != codeInvalidPhone {
		t.Fatalf("code %v", f.Args[2])
	}
}

func TestNewChatNonTextFailsPermanently(t *testing.T) {
	row := resolveRow(1, "+525512345678", 0)
	row[5] = "image"
	rdb := &resolveDB{rows: [][]driver.Value{row}}
	wa := &stubWA{res: inLID()}
	b, db, _ := newChatBridge(rdb, wa)
	b.processNewChats(context.Background())
	if f := failedCall(t, db); f.Args[2] != codeUnsupportedKind {
		t.Fatalf("code %v", f.Args[2])
	}
	if len(wa.lookups)+len(wa.sends) != 0 {
		t.Fatal("no WhatsApp call")
	}
}

func TestNewChatResolvesLIDAndWritesChatThenMessageThenOutbound(t *testing.T) {
	rdb := &resolveDB{rows: [][]driver.Value{resolveRow(42, "+525512345678", 0)}}
	wa := &stubWA{res: inLID()}
	b, db, _ := newChatBridge(rdb, wa)
	b.processNewChats(context.Background())

	if len(wa.sends) != 1 || wa.sends[0] != custLID {
		t.Fatalf("send to the LID: %v", wa.sends)
	}
	if got := strings.Join(writes(db), ","); got != "claim,sent,chat,message,chat_jid" {
		t.Fatalf("write order %s", got)
	}
	if claim := calls(db, "SET whatsapp_message_id = ?")[0]; claim.Args[0] != "3EB0NEWCHAT" || claim.Args[1] != int64(42) {
		t.Fatalf("claim args %v", claim.Args)
	}
	if len(wa.sendIDs) != 1 || wa.sendIDs[0] != "3EB0NEWCHAT" {
		t.Fatalf("sent with the claimed ID: %v", wa.sendIDs)
	}
	chat := calls(db, "INSERT INTO wa_chats")[0]
	// jid, display_name, phone_e164, is_group, pic, pic-marker, last_at, preview, from_me, unread, inc
	if chat.Args[0] != custLID.String() || chat.Args[1] != "Ana Lopez" || chat.Args[2] != "+5215512345678" {
		t.Fatalf("chat args %v", chat.Args)
	}
	if chat.Args[7] != "Hola Ana" || chat.Args[8] != int64(1) {
		t.Fatalf("chat tail %v", chat.Args)
	}
	msg := calls(db, "INSERT IGNORE INTO wa_messages")[0]
	if msg.Args[0] != "3EB0NEWCHAT" || msg.Args[1] != custLID.String() || msg.Args[3] != int64(1) || msg.Args[16] != "req-+525512345678" || msg.Args[18] != "sent" {
		t.Fatalf("message args %v", msg.Args)
	}
	sent := calls(db, "SET status = 'sent'")[0]
	if !strings.Contains(sent.Query, "error_code = NULL") || sent.Args[0] != "3EB0NEWCHAT" || sent.Args[1] != int64(42) {
		t.Fatalf("sent stamp %s %v", sent.Query, sent.Args)
	}
	if cj := calls(db, "SET chat_jid = ?")[0]; cj.Args[0] != custLID.String() || cj.Args[1] != int64(42) {
		t.Fatalf("chat_jid args %v", cj.Args)
	}
	if len(b.newChat.sent) != 0 {
		t.Fatal("the resend guard must be cleared once stamped")
	}
}

func TestNewChatDisplayNameFallsBackToVerifiedThenNull(t *testing.T) {
	row := resolveRow(1, "+525512345678", 0)
	row[3] = nil
	rdb := &resolveDB{rows: [][]driver.Value{row}}
	b, db, _ := newChatBridge(rdb, &stubWA{res: inLID()})
	b.processNewChats(context.Background())
	if c := calls(db, "INSERT INTO wa_chats")[0]; c.Args[1] != nil {
		t.Fatalf("no name known -> NULL, got %v", c.Args[1])
	}
}

func TestNewChatPNResultIsCanonicalized(t *testing.T) {
	rdb := &resolveDB{rows: [][]driver.Value{resolveRow(1, "+525512345678", 0)}}
	wa := &stubWA{res: []types.IsOnWhatsAppResponse{{JID: custPN, IsIn: true, PhoneNumber: custPN}}}
	b, db, _ := newChatBridge(rdb, wa)
	b.processNewChats(context.Background())
	if len(wa.sends) != 1 || wa.sends[0] != custLID {
		t.Fatalf("send to the mapped LID: %v", wa.sends)
	}
	if c := calls(db, "INSERT INTO wa_chats")[0]; c.Args[0] != custLID.String() {
		t.Fatalf("stored under %v", c.Args[0])
	}
}

func TestNewChatPNWithoutMappingStaysPN(t *testing.T) {
	other := types.NewJID("5213311112222", types.DefaultUserServer)
	rdb := &resolveDB{rows: [][]driver.Value{resolveRow(1, "+523311112222", 0)}}
	wa := &stubWA{res: []types.IsOnWhatsAppResponse{{JID: other, IsIn: true}}}
	b, db, _ := newChatBridge(rdb, wa)
	b.processNewChats(context.Background())
	c := calls(db, "INSERT INTO wa_chats")[0]
	// No PhoneNumber in the answer: phone_e164 is the queued to_phone.
	if c.Args[0] != other.String() || c.Args[2] != "+523311112222" {
		t.Fatalf("chat args %v", c.Args)
	}
}

func TestNewChatNonUserJIDIsInvalidPhone(t *testing.T) {
	rdb := &resolveDB{rows: [][]driver.Value{resolveRow(1, "+525512345678", 0)}}
	wa := &stubWA{res: []types.IsOnWhatsAppResponse{{JID: types.NewJID("1203", types.GroupServer), IsIn: true}}}
	b, db, _ := newChatBridge(rdb, wa)
	b.processNewChats(context.Background())
	if f := failedCall(t, db); f.Args[2] != codeInvalidPhone {
		t.Fatalf("code %v", f.Args[2])
	}
	if len(wa.sends) != 0 {
		t.Fatal("no send")
	}
}

func TestNewChatLookupErrorRetriesKeepingResolve(t *testing.T) {
	rdb := &resolveDB{rows: [][]driver.Value{resolveRow(1, "+525512345678", 2)}}
	wa := &stubWA{lookErr: errors.New("usync timeout")}
	b, db, _ := newChatBridge(rdb, wa)
	b.processNewChats(context.Background())
	if got := strings.Join(writes(db), ","); got != "attempt" {
		t.Fatalf("only the attempt bump: %s", got)
	}
	a := calls(db, "SET attempts = ?")[0]
	if strings.Contains(a.Query, "status") || a.Args[0] != int64(3) {
		t.Fatalf("attempt must not touch status: %s %v", a.Query, a.Args)
	}
	if len(wa.sends) != 0 {
		t.Fatal("no send")
	}
}

func TestNewChatLookupErrorAtCapFailsWithLookupFailed(t *testing.T) {
	rdb := &resolveDB{rows: [][]driver.Value{resolveRow(1, "+525512345678", outboundMaxAttempts-1)}}
	b, db, _ := newChatBridge(rdb, &stubWA{lookErr: errors.New("usync timeout")})
	b.processNewChats(context.Background())
	if f := failedCall(t, db); f.Args[2] != codeLookupFailed {
		t.Fatalf("code %v", f.Args[2])
	}
}

func TestNewChatLookupWarningWithAnswerStillSends(t *testing.T) {
	rdb := &resolveDB{rows: [][]driver.Value{resolveRow(1, "+525512345678", 0)}}
	wa := &stubWA{res: inLID(), lookErr: errors.New("failed to store LID mappings")}
	b, db, _ := newChatBridge(rdb, wa)
	b.processNewChats(context.Background())
	if len(wa.sends) != 1 || len(calls(db, "SET status = 'sent'")) != 1 {
		t.Fatalf("an answer with a cache warning is still an answer: %v", writes(db))
	}
}

func TestNewChatSendErrorRetriesThenFailsWithSendFailed(t *testing.T) {
	retry := resolveRow(1, "+525512345678", outboundMaxAttempts-1)
	retry[colWAMessageID] = "3EB0NEWCHAT"
	rdb := &resolveDB{rows: [][]driver.Value{resolveRow(1, "+525512345678", 0), retry}}
	wa := &stubWA{res: inLID(), sendErr: errors.New("server returned error 479")}
	b, db, clock := newChatBridge(rdb, wa)

	b.processNewChats(context.Background())
	if got := strings.Join(writes(db), ","); got != "claim,attempt" {
		t.Fatalf("first failure retries: %s", got)
	}
	db.Reset()
	*clock = clock.Add(newChatMinGap)
	b.processNewChats(context.Background())
	if f := failedCall(t, db); f.Args[2] != codeSendFailed || !strings.HasPrefix(f.Args[1].(string), "SEND_FAILED: ") {
		t.Fatalf("args %v", f.Args)
	}
	if len(wa.lookups) != 2 {
		t.Fatalf("each attempt looks the number up again: %v", wa.lookups)
	}
	if len(calls(db, "SET whatsapp_message_id = ?")) != 0 || wa.sendIDs[1] != "3EB0NEWCHAT" {
		t.Fatalf("the retry reuses the claimed ID: %v", wa.sendIDs)
	}
	if len(calls(db, "wa_chats")) != 0 {
		t.Fatal("no chat after a failed send")
	}
}

func TestNewChatPostSendDBFailureNeverResends(t *testing.T) {
	rdb := &resolveDB{rows: [][]driver.Value{resolveRow(7, "+525512345678", 0)}}
	rdb.fail = func(c testdb.Call) error {
		if strings.HasPrefix(c.Query, "INSERT INTO wa_chats") {
			return errors.New("lost connection")
		}
		return nil
	}
	wa := &stubWA{res: inLID()}
	b, db, _ := newChatBridge(rdb, wa)
	b.processNewChats(context.Background())

	if len(wa.sends) != 1 {
		t.Fatalf("sent once: %v", wa.sends)
	}
	if len(calls(db, "INSERT IGNORE INTO wa_messages")) != 0 {
		t.Fatal("no message row without its chat (FK)")
	}
	sent := calls(db, "SET status = 'sent'")
	if len(sent) != 1 || sent[0].Args[1] != int64(7) {
		t.Fatalf("marked sent: %+v", sent)
	}
	if len(calls(db, "SET chat_jid = ?")) != 0 {
		t.Fatal("chat_jid stays NULL without its chat row (FK)")
	}
	if got := strings.Join(writes(db), ","); got != "claim,sent,chat,chat,chat" {
		t.Fatalf("the stamp comes before the chat (three tries): %s", got)
	}
}

func TestNewChatStampFailureRetriesWritesNotSend(t *testing.T) {
	stampFails := true
	rdb := &resolveDB{rows: [][]driver.Value{resolveRow(7, "+525512345678", 0), resolveRow(7, "+525512345678", 0)}}
	rdb.fail = func(c testdb.Call) error {
		if stampFails && strings.Contains(c.Query, "SET status = 'sent'") {
			return errors.New("lost connection")
		}
		return nil
	}
	wa := &stubWA{res: inLID()}
	b, db, clock := newChatBridge(rdb, wa)
	b.processNewChats(context.Background())
	if len(b.newChat.sent) != 1 {
		t.Fatal("the sent message must be remembered until stamped")
	}
	stampFails = false
	db.Reset()
	*clock = clock.Add(newChatMinGap)
	b.processNewChats(context.Background())
	if len(wa.sends) != 1 || len(wa.lookups) != 1 {
		t.Fatalf("no second lookup or send: %v %v", wa.lookups, wa.sends)
	}
	if got := strings.Join(writes(db), ","); got != "sent,chat,message,chat_jid" {
		t.Fatalf("retried writes %s", got)
	}
	if len(b.newChat.sent) != 0 {
		t.Fatal("guard cleared after the stamp")
	}
}

func TestNewChatMinGap(t *testing.T) {
	rdb := &resolveDB{rows: [][]driver.Value{resolveRow(1, "+525512345678", 0), resolveRow(2, "+525598765432", 0)}}
	wa := &stubWA{res: inLID()}
	b, db, clock := newChatBridge(rdb, wa)

	b.processNewChats(context.Background())
	*clock = clock.Add(newChatMinGap - time.Second)
	db.Reset()
	b.processNewChats(context.Background())
	if len(wa.sends) != 1 {
		t.Fatalf("one send inside the gap: %v", wa.sends)
	}
	if len(db.Calls()) != 0 {
		t.Fatal("inside the gap the queue is not even read")
	}
	*clock = clock.Add(time.Second)
	b.processNewChats(context.Background())
	if len(wa.sends) != 2 {
		t.Fatalf("the next row goes out after the gap: %v", wa.sends)
	}
}

func TestResolveQueryFallsBackWhenColumnMissing(t *testing.T) {
	b, db := newTestBridge(func(c testdb.Call) testdb.Result {
		if strings.Contains(c.Query, "to_phone") {
			return testdb.Result{Err: errBadField}
		}
		return testdb.Result{}
	})
	b.isOnWhatsApp = func(context.Context, []string) ([]types.IsOnWhatsAppResponse, error) {
		t.Fatal("no lookup without the feature")
		return nil, nil
	}
	b.processNewChats(context.Background())
	b.processNewChats(context.Background())
	if n := len(db.Calls()); n != 1 {
		t.Fatalf("feature off after the first unknown column, got %d statements", n)
	}
}

func TestResolveQueryShape(t *testing.T) {
	rdb := &resolveDB{}
	b, db, _ := newChatBridge(rdb, &stubWA{})
	b.processNewChats(context.Background())
	q := calls(db, "WHERE status = 'resolve'")
	if len(q) != 1 || !strings.Contains(q[0].Query, "attempts < ?") || !strings.Contains(q[0].Query, "CASE attempts") || !strings.HasSuffix(q[0].Query, "ORDER BY id LIMIT 1") {
		t.Fatalf("resolve query %+v", q)
	}
	if !strings.Contains(q[0].Query, "TIMESTAMPDIFF(SECOND, created_at, NOW()) > ? AS expired") || q[0].Args[0] != int64(23*3600) {
		t.Fatalf("expiry column %s %v", q[0].Query, q[0].Args)
	}
}

func TestPendingQueryNeverSelectsResolve(t *testing.T) {
	b, db := newTestBridge(nil)
	if _, err := b.queryPendingOutbound(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	q := db.Calls()[0].Query
	if !strings.Contains(q, "WHERE status = 'pending'") || strings.Contains(q, "resolve") {
		t.Fatalf("pending query %s", q)
	}
}

func TestPermanentFailuresCarryTheirCode(t *testing.T) {
	b, db := newTestBridge(nil)
	b.sendOutboundRow(context.Background(), outboundRow{ID: 3, ChatJID: "1.2.3@s.whatsapp.net", Kind: "text"})
	f := failedCall(t, db)
	if f.Args[2] != "INVALID_JID" || !regexp.MustCompile(`^INVALID_JID: `).MatchString(f.Args[1].(string)) {
		t.Fatalf("args %v", f.Args)
	}
}

func TestNormalizeCheckPhone(t *testing.T) {
	for in, want := range map[string]string{
		"+525512345678":     "+525512345678",
		"5215512345678":     "+5215512345678",
		"+52 55 1234-5678":  "+525512345678",
		"+52 (55) 12345678": "+525512345678",
	} {
		got, err := NormalizeCheckPhone(in)
		if err != nil || got != want {
			t.Fatalf("%q -> %q, %v", in, got, err)
		}
	}
	for _, bad := range []string{"", "+52abc", "123", "+1234567890123456"} {
		if _, err := NormalizeCheckPhone(bad); err == nil {
			t.Fatalf("%q must be rejected", bad)
		}
	}
}

func TestCheckNumberAnswersRequestFileWithoutDBWrites(t *testing.T) {
	dir := t.TempDir()
	b, db, _ := newChatBridge(&resolveDB{}, &stubWA{res: []types.IsOnWhatsAppResponse{{Query: "+525512345678", JID: custPN, IsIn: true, PhoneNumber: custPN}}})
	if err := writeFileAtomic(filepath.Join(dir, checkRequestFile), CheckNumberRequest{ID: "abc", Phone: "+525512345678"}); err != nil {
		t.Fatal(err)
	}
	b.answerCheckRequest(context.Background(), dir)
	if _, err := os.Stat(filepath.Join(dir, checkRequestFile)); !os.IsNotExist(err) {
		t.Fatal("the request is consumed")
	}
	data, err := os.ReadFile(filepath.Join(dir, checkResultFile))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"id":"abc"`, `"is_in":true`, `"jid":"5215512345678@s.whatsapp.net"`, `"store_jid":"123456789012345@lid"`} {
		if !strings.Contains(string(data), want) {
			t.Fatalf("result %s lacks %s", data, want)
		}
	}
	if len(db.Calls()) != 0 {
		t.Fatalf("check-number writes nothing to MySQL: %+v", db.Calls())
	}
}

// The web app counts a 'resolve' row older than 24 h as undelivered and lets
// staff write again; the bridge must never send such a row late.
func TestNewChatExpiredRowFailsWithoutLookupOrSend(t *testing.T) {
	stale := resolveRow(1, "+525512345678", 0)
	stale[colExpired] = int64(1)
	rdb := &resolveDB{rows: [][]driver.Value{stale, resolveRow(2, "+525598765432", 0)}}
	wa := &stubWA{res: inLID()}
	b, db, _ := newChatBridge(rdb, wa)
	b.processNewChats(context.Background())
	f := failedCall(t, db)
	if f.Args[2] != codeExpired || !strings.HasPrefix(f.Args[1].(string), "EXPIRED: ") || f.Args[3] != int64(1) {
		t.Fatalf("args %v", f.Args)
	}
	if len(wa.lookups)+len(wa.sends) != 0 || len(calls(db, "wa_chats")) != 0 {
		t.Fatal("an expired row is never looked up or sent")
	}
	// Expiring takes no WhatsApp call, so it does not use up the pacing gap.
	b.processNewChats(context.Background())
	if len(wa.sends) != 1 {
		t.Fatalf("the next current row goes out right away: %v", wa.sends)
	}
}

func TestNewChatExpiredRowWithClaimedIDIsNotResent(t *testing.T) {
	stale := resolveRow(1, "+525512345678", 0)
	stale[colExpired] = int64(1)
	stale[colWAMessageID] = "3EB0OLD"
	rdb := &resolveDB{rows: [][]driver.Value{stale}}
	wa := &stubWA{res: inLID()}
	b, db, _ := newChatBridge(rdb, wa)
	b.processNewChats(context.Background())
	if f := failedCall(t, db); f.Args[2] != codeExpired || !strings.Contains(f.Args[1].(string), "3EB0OLD") {
		t.Fatalf("args %v", f.Args)
	}
	if len(wa.sends) != 0 {
		t.Fatal("no late resend")
	}
}

// A restart between the send and the stamp: the new process has an empty
// in-memory guard, but the ID claimed on the row makes the resend carry the
// same message ID, which the recipient drops as a duplicate.
func TestNewChatRestartResendsWithTheClaimedID(t *testing.T) {
	row := resolveRow(7, "+525512345678", 0)
	row[colWAMessageID] = "3EB0CLAIMED"
	rdb := &resolveDB{rows: [][]driver.Value{row}}
	wa := &stubWA{res: inLID()}
	b, db, _ := newChatBridge(rdb, wa)
	b.newMessageID = func() types.MessageID {
		t.Fatal("a claimed row never gets a new ID")
		return ""
	}
	b.processNewChats(context.Background())
	if len(wa.sendIDs) != 1 || wa.sendIDs[0] != "3EB0CLAIMED" {
		t.Fatalf("resent with the claimed ID: %v", wa.sendIDs)
	}
	if got := strings.Join(writes(db), ","); got != "sent,chat,message,chat_jid" {
		t.Fatalf("writes %s", got)
	}
	if sent := calls(db, "SET status = 'sent'")[0]; sent.Args[0] != "3EB0CLAIMED" {
		t.Fatalf("stamped with the claimed ID: %v", sent.Args)
	}
}

func TestNewChatClaimFailureDoesNotSend(t *testing.T) {
	rdb := &resolveDB{rows: [][]driver.Value{resolveRow(1, "+525512345678", 0)}}
	rdb.fail = func(c testdb.Call) error {
		if strings.Contains(c.Query, "SET whatsapp_message_id = ?") {
			return errors.New("lost connection")
		}
		return nil
	}
	wa := &stubWA{res: inLID()}
	b, db, _ := newChatBridge(rdb, wa)
	b.processNewChats(context.Background())
	if len(wa.sends) != 0 {
		t.Fatal("no send without the ID stored first")
	}
	if got := strings.Join(writes(db), ","); got != "claim" {
		t.Fatalf("nothing else is written: %s", got)
	}
}

func TestNewChatRowThatLeftResolveIsNotSent(t *testing.T) {
	b, db := newTestBridge(func(c testdb.Call) testdb.Result {
		if c.IsQuery && strings.Contains(c.Query, "WHERE status = 'resolve'") {
			return testdb.Result{
				Cols: []string{"id", "client_request_id", "to_phone", "to_name", "body", "kind", "attempts", "whatsapp_message_id", "expired"},
				Rows: [][]driver.Value{resolveRow(1, "+525512345678", 0)},
			}
		}
		if strings.Contains(c.Query, "SET whatsapp_message_id = ?") {
			return testdb.Result{NoRowsAffected: true}
		}
		return testdb.Result{}
	})
	wa := &stubWA{res: inLID()}
	b.now = func() time.Time { return sentAt }
	b.isOnWhatsApp = func(context.Context, []string) ([]types.IsOnWhatsAppResponse, error) { return wa.res, nil }
	b.newMessageID = func() types.MessageID { return "3EB0NEWCHAT" }
	b.lidForPN = func(context.Context, types.JID) (types.JID, error) { return types.EmptyJID, nil }
	b.sendMessage = func(context.Context, types.JID, *waProto.Message, ...whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
		t.Fatal("a row failed by hand meanwhile is not sent")
		return whatsmeow.SendResponse{}, nil
	}
	b.processNewChats(context.Background())
	if len(calls(db, "SET status")) != 0 {
		t.Fatalf("no stamp: %+v", db.Calls())
	}
}
