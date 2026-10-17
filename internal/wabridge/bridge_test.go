package wabridge

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"
	"time"

	"github.com/enviadores/wabridge/internal/store"
	"github.com/enviadores/wabridge/internal/testdb"
	"github.com/go-sql-driver/mysql"
	waProto "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

var errBadField = &mysql.MySQLError{Number: 1054, Message: "Unknown column 'x' in 'field list'"}

func newTestBridge(h testdb.Handler) (*Bridge, *testdb.DB) {
	db := testdb.New(h)
	return &Bridge{store: store.NewFromDB(db.DB), logger: waLog.Noop}, db
}

func calls(db *testdb.DB, substr string) []testdb.Call {
	var out []testdb.Call
	for _, c := range db.Calls() {
		if strings.Contains(c.Query, substr) {
			out = append(out, c)
		}
	}
	return out
}

// existsHandler answers the "does the original row exist" probe.
func existsHandler(exists bool) testdb.Handler {
	return func(c testdb.Call) testdb.Result {
		if c.IsQuery && strings.Contains(c.Query, "SELECT 1 FROM wa_messages") && exists {
			return testdb.Result{Cols: []string{"1"}, Rows: [][]driver.Value{{int64(1)}}}
		}
		return testdb.Result{}
	}
}

func inboundEvt(id string) *events.Message {
	e := &events.Message{}
	e.Info.ID = id
	e.Info.Chat = types.NewJID("5215511111111", types.DefaultUserServer)
	return e
}

func TestApplyProtocolActionEditUpdatesOriginalAndRecomputesTail(t *testing.T) {
	b, db := newTestBridge(existsHandler(true))
	evt := inboundEvt("EDIT-STANZA")
	act := resolveAction(context.Background(), evtOf(editProto("ORIG", "Calle Nueva 5")), nil)

	if !b.applyProtocolAction(context.Background(), evt, "chat@lid", act) {
		t.Fatal("an edit must be fully handled")
	}
	ups := calls(db, "original_body = COALESCE(original_body, body)")
	if len(ups) != 1 || ups[0].Args[0] != "Calle Nueva 5" || ups[0].Args[1] != "ORIG" || ups[0].Args[2] != "chat@lid" || ups[0].Args[3] != int64(0) {
		t.Fatalf("edit update: %+v", ups)
	}
	if len(calls(db, "INSERT")) != 0 {
		t.Fatal("an edit must not insert a row")
	}
	if len(calls(db, "UPDATE wa_chats c")) != 1 {
		t.Fatalf("the chat tail must be recomputed: %+v", db.Calls())
	}
}

func TestApplyProtocolActionEditFromOurPhoneMatchesOurRow(t *testing.T) {
	b, db := newTestBridge(existsHandler(true))
	evt := inboundEvt("E")
	evt.Info.IsFromMe = true
	act := resolveAction(context.Background(), evtOf(editProto("MINE", "corregido")), nil)
	b.applyProtocolAction(context.Background(), evt, "chat", act)
	if ups := calls(db, "original_body"); len(ups) != 1 || ups[0].Args[3] != int64(1) {
		t.Fatalf("from_me must be 1: %+v", ups)
	}
}

func TestApplyProtocolActionEditOfUnknownMessageIsIgnored(t *testing.T) {
	b, db := newTestBridge(existsHandler(false))
	act := resolveAction(context.Background(), evtOf(editProto("NEVER-SEEN", "x")), nil)
	if !b.applyProtocolAction(context.Background(), inboundEvt("E"), "chat", act) {
		t.Fatal("must be handled (nothing is stored for it)")
	}
	if len(calls(db, "INSERT")) != 0 || len(calls(db, "UPDATE wa_chats")) != 0 {
		t.Fatalf("an edit of an unknown message must change nothing visible: %+v", db.Calls())
	}
}

func TestApplyProtocolActionDroppedDecryptFailureStoresNothing(t *testing.T) {
	b, db := newTestBridge(nil)
	dec := func(context.Context, *events.Message) (*waProto.Message, error) {
		return nil, context.DeadlineExceeded
	}
	act := resolveAction(context.Background(), secretEvt("ORIG"), dec)
	if !b.applyProtocolAction(context.Background(), inboundEvt("S"), "chat", act) {
		t.Fatal("must be handled")
	}
	if n := len(db.Calls()); n != 0 {
		t.Fatalf("no statements expected, got %+v", db.Calls())
	}
}

func TestApplyProtocolActionRevokeFlagsOriginal(t *testing.T) {
	b, db := newTestBridge(existsHandler(true))
	evt := evtOf(&waProto.Message{ProtocolMessage: &waProto.ProtocolMessage{
		Type: waProto.ProtocolMessage_REVOKE.Enum(), Key: key("ORIG")}})
	act := resolveAction(context.Background(), evt, nil)
	if !b.applyProtocolAction(context.Background(), inboundEvt("REV"), "chat@lid", act) {
		t.Fatal("a revoke of a stored message is fully handled")
	}
	rv := calls(db, "revoked_at = COALESCE")
	if len(rv) != 1 || rv[0].Args[0] != "ORIG" || rv[0].Args[1] != "chat@lid" {
		t.Fatalf("revoke must target the original by key id: %+v", rv)
	}
	if len(calls(db, "INSERT")) != 0 {
		t.Fatal("no 'revoked' row when the original exists")
	}
	if len(calls(db, "UPDATE wa_chats c")) != 1 {
		t.Fatal("tail must be recomputed so the list shows the deletion")
	}
}

func TestApplyProtocolActionRevokeOfMissingOriginalKeepsOldBehaviour(t *testing.T) {
	b, db := newTestBridge(existsHandler(false))
	evt := evtOf(&waProto.Message{ProtocolMessage: &waProto.ProtocolMessage{
		Type: waProto.ProtocolMessage_REVOKE.Enum(), Key: key("OLD")}})
	act := resolveAction(context.Background(), evt, nil)
	if b.applyProtocolAction(context.Background(), inboundEvt("REV"), "chat", act) {
		t.Fatal("must return false so handleMessage inserts the 'revoked' row")
	}
	if len(calls(db, "UPDATE wa_chats c")) != 0 {
		t.Fatal("nothing changed, no tail recompute")
	}
}

func TestApplyProtocolActionNoneAndOrdinary(t *testing.T) {
	b, db := newTestBridge(nil)
	if b.applyProtocolAction(context.Background(), inboundEvt("X"), "chat", protoAction{}) {
		t.Fatal("ordinary messages are not handled here")
	}
	if len(db.Calls()) != 0 {
		t.Fatal("no statements for an ordinary message")
	}
}

func TestStoreReactionUpsertsAndRemoves(t *testing.T) {
	existing := func(c testdb.Call) testdb.Result {
		if c.IsQuery && strings.Contains(c.Query, "SELECT id FROM wa_messages") {
			return testdb.Result{Cols: []string{"id"}, Rows: [][]driver.Value{{"R-OLD"}}}
		}
		return testdb.Result{}
	}
	b, db := newTestBridge(existing)
	evt := inboundEvt("R-NEW")
	evt.Info.Sender = evt.Info.Chat
	evt.Info.Timestamp = time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)

	// New emoji replaces the previous one (same sender, same target).
	b.storeReaction(context.Background(), evt, "chat", &waProto.ReactionMessage{Key: key("TARGET"), Text: proto.String("❤")})
	if up := calls(db, "SET message_type = 'reaction'"); len(up) != 1 || up[0].Args[0] != "❤" || up[0].Args[2] != "R-OLD" {
		t.Fatalf("replace: %+v", db.Calls())
	}
	// Empty text removes it.
	db.Reset()
	b.storeReaction(context.Background(), evt, "chat", &waProto.ReactionMessage{Key: key("TARGET"), Text: proto.String("")})
	if up := calls(db, "SET message_type = 'system'"); len(up) != 1 || up[0].Args[1] != "R-OLD" {
		t.Fatalf("removal: %+v", db.Calls())
	}
	// Reactions never move the chat.
	if len(calls(db, "wa_chats")) != 0 {
		t.Fatal("a reaction must not touch wa_chats (preview, unread, ordering)")
	}
	// A reaction without a target is ignored.
	db.Reset()
	b.storeReaction(context.Background(), evt, "chat", &waProto.ReactionMessage{Text: proto.String("x")})
	if len(db.Calls()) != 0 {
		t.Fatal("reaction without target must be ignored")
	}
}

func TestIsSelfReadOnlyForOurOwnDevices(t *testing.T) {
	r := &events.Receipt{Type: types.ReceiptTypeRead}
	r.IsFromMe = true
	if !isSelfRead(r) {
		t.Fatal("read from our own device must count")
	}
	r.Type = types.ReceiptTypeReadSelf
	if !isSelfRead(r) {
		t.Fatal("read-self must count")
	}
	r.Type = types.ReceiptTypeDelivered
	if isSelfRead(r) {
		t.Fatal("delivery is not a read")
	}
	other := &events.Receipt{Type: types.ReceiptTypeRead} // customer read our message
	if isSelfRead(other) {
		t.Fatal("a customer's read receipt must not clear our unread count")
	}
}

func TestProcessOutboundBatchFallsBackWithoutQuotedColumn(t *testing.T) {
	b, db := newTestBridge(func(c testdb.Call) testdb.Result {
		if c.IsQuery && strings.Contains(c.Query, "quoted_message_id") {
			return testdb.Result{Err: errBadField}
		}
		return testdb.Result{}
	})
	for i := 0; i < 2; i++ {
		if err := b.processOutboundBatch(context.Background()); err != nil {
			t.Fatalf("poll %d: %v", i, err)
		}
	}
	all := db.Calls()
	if len(all) != 3 { // new (fails), legacy, legacy
		t.Fatalf("expected 3 statements, got %d: %+v", len(all), all)
	}
	if strings.Contains(all[1].Query, "quoted_message_id") || !strings.Contains(all[1].Query, "attempts, NULL") {
		t.Fatalf("legacy query should select NULL for the quote: %s", all[1].Query)
	}
}

func TestProcessOutboundBatchReadsQuotedMessageID(t *testing.T) {
	var seen string
	b, _ := newTestBridge(func(c testdb.Call) testdb.Result {
		if c.IsQuery && strings.Contains(c.Query, "FROM wa_outbound") {
			seen = c.Query
		}
		return testdb.Result{}
	})
	if err := b.processOutboundBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(seen, "attempts, quoted_message_id") {
		t.Fatalf("the poll must select quoted_message_id: %s", seen)
	}
}
