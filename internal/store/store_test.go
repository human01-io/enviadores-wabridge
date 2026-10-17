package store

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/enviadores/wabridge/internal/testdb"
	"github.com/go-sql-driver/mysql"
)

var errBadField = &mysql.MySQLError{Number: 1054, Message: "Unknown column 'x' in 'field list'"}

func newStore(h testdb.Handler) (*Store, *testdb.DB, *[]string) {
	db := testdb.New(h)
	st := NewFromDB(db.DB)
	var logs []string
	st.SetLogger(func(format string, args ...any) {
		logs = append(logs, strings.TrimSpace(fmt.Sprintf(format, args...)))
	})
	return st, db, &logs
}

func has(c testdb.Call, parts ...string) bool {
	for _, p := range parts {
		if !strings.Contains(c.Query, p) {
			return false
		}
	}
	return true
}

func countCalls(calls []testdb.Call, parts ...string) int {
	n := 0
	for _, c := range calls {
		if has(c, parts...) {
			n++
		}
	}
	return n
}

var t0 = time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)

func assertASCII(t *testing.T, logs []string) {
	t.Helper()
	for _, l := range logs {
		for _, r := range l {
			if r > 0x7e {
				t.Errorf("log line is not ASCII: %q", l)
			}
		}
	}
}

func TestInsertMessageUnknownColumnFallsBackOnceAndLogsOnce(t *testing.T) {
	st, db, logs := newStore(func(c testdb.Call) testdb.Result {
		if has(c, "quoted_preview") {
			return testdb.Result{Err: errBadField}
		}
		return testdb.Result{}
	})
	m := Message{
		ID: "A1", ChatJID: "5215500000000@s.whatsapp.net", MessageType: "text",
		Body:            sql.NullString{String: "hola", Valid: true},
		QuotedMessageID: sql.NullString{String: "Q1", Valid: true},
		QuotedPreview:   sql.NullString{String: "texto citado", Valid: true},
		Timestamp:       t0,
	}
	if err := st.InsertMessage(t.Context(), m); err != nil {
		t.Fatalf("InsertMessage: %v", err)
	}
	calls := db.Calls()
	if len(calls) != 2 || !has(calls[0], "quoted_preview") || has(calls[1], "quoted_preview") || !has(calls[1], "quoted_message_id") {
		t.Fatalf("expected new statement then legacy statement, got %+v", calls)
	}
	// Second insert goes straight to the legacy statement.
	db.Reset()
	m.ID = "A2"
	if err := st.InsertMessage(t.Context(), m); err != nil {
		t.Fatalf("second InsertMessage: %v", err)
	}
	if calls = db.Calls(); len(calls) != 1 || has(calls[0], "quoted_preview") {
		t.Fatalf("expected a single legacy statement, got %+v", calls)
	}
	if len(*logs) != 1 {
		t.Fatalf("expected exactly one log line, got %v", *logs)
	}
	assertASCII(t, *logs)
}

func TestInsertMessageOtherErrorsAreNotSwallowed(t *testing.T) {
	boom := &mysql.MySQLError{Number: 1062, Message: "dup"}
	st, db, logs := newStore(func(testdb.Call) testdb.Result { return testdb.Result{Err: boom} })
	if err := st.InsertMessage(t.Context(), Message{ID: "A", ChatJID: "c", MessageType: "text", Timestamp: t0}); err == nil {
		t.Fatal("expected the error to propagate")
	}
	if n := len(db.Calls()); n != 1 || len(*logs) != 0 || st.FeatureOff(featureQuotes) {
		t.Fatalf("a non-1054 error must not trigger the fallback (calls=%d logs=%v)", n, *logs)
	}
}

// locateHandler answers LocateMessage with (chat, fromMe) when chat != "".
func locateHandler(chat string, fromMe int64, inner testdb.Handler) testdb.Handler {
	return func(c testdb.Call) testdb.Result {
		if c.IsQuery && has(c, "SELECT chat_jid, from_me FROM wa_messages") {
			if chat == "" {
				return testdb.Result{}
			}
			return testdb.Result{Cols: []string{"chat_jid", "from_me"}, Rows: [][]driver.Value{{chat, fromMe}}}
		}
		if inner != nil {
			return inner(c)
		}
		return testdb.Result{}
	}
}

func TestLocateMessageSearchesEveryChatAlias(t *testing.T) {
	st, db, _ := newStore(locateHandler("5215511111111@s.whatsapp.net", 0, nil))
	ref, found, err := st.LocateMessage(t.Context(), "MSG1", []string{"123@lid", "5215511111111@s.whatsapp.net"})
	if err != nil || !found || ref.ChatJID != "5215511111111@s.whatsapp.net" || ref.FromMe {
		t.Fatalf("ref=%+v found=%v err=%v", ref, found, err)
	}
	c := db.Calls()[0]
	if !has(c, "WHERE id = ? AND chat_jid IN (?,?)") || c.Args[0] != "MSG1" || c.Args[1] != "123@lid" || c.Args[2] != "5215511111111@s.whatsapp.net" {
		t.Fatalf("unexpected locate %+v", c)
	}
}

func TestApplyEditKeepsFirstOriginalAndStampsEditedAt(t *testing.T) {
	// The original was stored under the phone JID before the LID mapping
	// was learned; the edit arrives on the LID chat.
	st, db, _ := newStore(locateHandler("5215511111111@s.whatsapp.net", 0, nil))
	ref, found, err := st.ApplyEdit(t.Context(), []string{"123@lid", "5215511111111@s.whatsapp.net"}, "MSG1", false, sql.NullString{String: "Calle Nueva 5", Valid: true})
	if err != nil || !found || ref.ChatJID != "5215511111111@s.whatsapp.net" {
		t.Fatalf("ApplyEdit ref=%+v found=%v err=%v", ref, found, err)
	}
	calls := db.Calls()
	if len(calls) != 2 {
		t.Fatalf("expected locate + UPDATE, got %+v", calls)
	}
	up := calls[1]
	if !has(up, "UPDATE wa_messages", "original_body = COALESCE(original_body, body)", "body = ?", "edited_at = UTC_TIMESTAMP()", "WHERE id = ? AND chat_jid = ?") {
		t.Fatalf("unexpected edit statement: %s", up.Query)
	}
	// original_body must be assigned before body (MySQL evaluates SET left to right).
	if strings.Index(up.Query, "original_body = COALESCE") > strings.Index(up.Query, "body = ?") {
		t.Fatalf("original_body must be assigned before body: %s", up.Query)
	}
	if up.Args[0] != "Calle Nueva 5" || up.Args[1] != "MSG1" || up.Args[2] != "5215511111111@s.whatsapp.net" {
		t.Fatalf("unexpected args %v", up.Args)
	}
	if has(up, "INSERT") {
		t.Fatal("an edit must never insert a row")
	}
}

func TestApplyEditMissingOriginalReportsNotFound(t *testing.T) {
	st, db, _ := newStore(nil) // locate returns no rows
	_, found, err := st.ApplyEdit(t.Context(), []string{"chat"}, "GONE", true, sql.NullString{String: "x", Valid: true})
	if err != nil || found {
		t.Fatalf("found=%v err=%v, want false,nil", found, err)
	}
	if countCalls(db.Calls(), "UPDATE") != 0 {
		t.Fatal("nothing to update")
	}
}

func TestApplyEditOfTheOtherSidesMessageIsRefused(t *testing.T) {
	st, db, _ := newStore(locateHandler("chat", 1, nil)) // our message
	_, _, err := st.ApplyEdit(t.Context(), []string{"chat"}, "OURS", false, sql.NullString{String: "x", Valid: true})
	if !errors.Is(err, ErrNotAuthor) {
		t.Fatalf("err=%v, want ErrNotAuthor", err)
	}
	if countCalls(db.Calls(), "UPDATE") != 0 {
		t.Fatal("the contact must not rewrite our message")
	}
}

func TestApplyEditWithoutNewColumnsLeavesOriginal(t *testing.T) {
	st, db, logs := newStore(locateHandler("chat", 0, func(c testdb.Call) testdb.Result {
		if has(c, "original_body") {
			return testdb.Result{Err: errBadField}
		}
		return testdb.Result{}
	}))
	ref, found, err := st.ApplyEdit(t.Context(), []string{"chat"}, "M", false, sql.NullString{String: "new", Valid: true})
	if !errors.Is(err, ErrEditsUnsupported) || !found || ref.ChatJID != "chat" {
		t.Fatalf("ref=%+v found=%v err=%v", ref, found, err)
	}
	for _, c := range db.Calls() {
		if has(c, "UPDATE wa_messages SET body") {
			t.Fatalf("the original text must not be overwritten without original_body: %s", c.Query)
		}
	}
	if len(*logs) != 1 {
		t.Fatalf("one log line expected, got %v", *logs)
	}
	assertASCII(t, *logs)
}

func TestRevokeMessageTargetsOriginalById(t *testing.T) {
	st, db, _ := newStore(locateHandler("5215511111111@s.whatsapp.net", 0, nil))
	ref, found, err := st.RevokeMessage(t.Context(), []string{"123@lid", "5215511111111@s.whatsapp.net"}, "ORIG", false, false)
	if err != nil || !found || ref.ChatJID != "5215511111111@s.whatsapp.net" {
		t.Fatalf("ref=%+v found=%v err=%v", ref, found, err)
	}
	up := db.Calls()[1]
	if !has(up, "UPDATE wa_messages", "revoked_at = COALESCE(revoked_at, UTC_TIMESTAMP())", "WHERE id = ? AND chat_jid = ?") ||
		up.Args[0] != "ORIG" || up.Args[1] != "5215511111111@s.whatsapp.net" {
		t.Fatalf("unexpected revoke statement %+v", up)
	}
	if has(up, "body") {
		t.Fatal("revoke must keep the body for audit")
	}
}

func TestRevokeMessageOneToOneOnlyByAuthor(t *testing.T) {
	// 1:1: the contact cannot delete our message.
	st, db, _ := newStore(locateHandler("chat", 1, nil))
	if _, _, err := st.RevokeMessage(t.Context(), []string{"chat"}, "OURS", false, false); !errors.Is(err, ErrNotAuthor) {
		t.Fatalf("err=%v, want ErrNotAuthor", err)
	}
	if countCalls(db.Calls(), "revoked_at") != 0 {
		t.Fatal("must not flag our message")
	}
	// Group: an admin may delete anyone's message.
	st, db, _ = newStore(locateHandler("g@g.us", 1, nil))
	if _, found, err := st.RevokeMessage(t.Context(), []string{"g@g.us"}, "OURS", false, true); err != nil || !found {
		t.Fatalf("group: found=%v err=%v", found, err)
	}
	if countCalls(db.Calls(), "revoked_at") != 1 {
		t.Fatal("group delete must flag the message")
	}
}

func TestRevokeMessageMissingOriginalOrOldSchema(t *testing.T) {
	// Original missing: the locate finds nothing.
	st, _, _ := newStore(nil)
	if _, found, err := st.RevokeMessage(t.Context(), []string{"chat"}, "X", false, false); err != nil || found {
		t.Fatalf("missing original: found=%v err=%v", found, err)
	}
	// Old schema: no revoked_at; caller must fall back to inserting a 'revoked' row.
	st, db, logs := newStore(locateHandler("chat", 0, func(c testdb.Call) testdb.Result {
		if has(c, "revoked_at") {
			return testdb.Result{Err: errBadField}
		}
		return testdb.Result{}
	}))
	if _, found, err := st.RevokeMessage(t.Context(), []string{"chat"}, "X", false, false); err != nil || found {
		t.Fatalf("old schema: found=%v err=%v", found, err)
	}
	if _, found, _ := st.RevokeMessage(t.Context(), []string{"chat"}, "Y", false, false); found {
		t.Fatal("old schema must keep reporting not found")
	}
	if n := countCalls(db.Calls(), "revoked_at"); n != 1 || len(*logs) != 1 {
		t.Fatalf("expected one probing statement and one log line, got %d / %v", n, *logs)
	}
}

func existingReaction(id string) testdb.Handler {
	return func(c testdb.Call) testdb.Result {
		if c.IsQuery && has(c, "SELECT id FROM wa_messages", "target_message_id") && id != "" {
			return testdb.Result{Cols: []string{"id"}, Rows: [][]driver.Value{{id}}}
		}
		return testdb.Result{}
	}
}

func reaction(emoji string) Reaction {
	return Reaction{
		ID: "R2", ChatJID: "5215511111111@s.whatsapp.net",
		SenderJID: sql.NullString{String: "5215511111111@s.whatsapp.net", Valid: true},
		TargetID:  "TARGET", Emoji: emoji, Timestamp: t0,
	}
}

func TestUpsertReactionInsertsNewRowAndTouchesTarget(t *testing.T) {
	st, db, _ := newStore(existingReaction(""))
	if err := st.UpsertReaction(t.Context(), reaction("\U0001F44D")); err != nil {
		t.Fatal(err)
	}
	calls := db.Calls()
	if countCalls(calls, "INSERT IGNORE INTO wa_messages") != 1 {
		t.Fatalf("expected one insert, got %+v", calls)
	}
	ins := calls[1]
	if ins.Args[0] != "R2" || ins.Args[4] != "reaction" || ins.Args[5] != "\U0001F44D" {
		t.Fatalf("insert args %v", ins.Args)
	}
	last := calls[len(calls)-1]
	if !has(last, "UPDATE wa_messages SET status_updated_at = UTC_TIMESTAMP()") || last.Args[0] != "TARGET" {
		t.Fatalf("the reacted-to message must be touched, got %+v", last)
	}
	// A 1:1 chat matches on from_me only (the contact's JID may flip PN/LID).
	if has(calls[0], "sender_jid") {
		t.Fatalf("1:1 lookup must not match on sender_jid: %s", calls[0].Query)
	}
}

func TestUpsertReactionGroupMatchesOnSender(t *testing.T) {
	st, db, _ := newStore(existingReaction(""))
	r := reaction("❤")
	r.IsGroup = true
	if err := st.UpsertReaction(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	if !has(db.Calls()[0], "AND sender_jid IN (?)") {
		t.Fatalf("group lookup must match on sender_jid: %s", db.Calls()[0].Query)
	}
	// Every known form of the sender (phone, LID) matches the earlier row.
	db.Reset()
	r.SenderAliases = []string{"5215511111111@s.whatsapp.net", "123@lid"}
	if err := st.UpsertReaction(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	if c := db.Calls()[0]; !has(c, "AND sender_jid IN (?,?)") || c.Args[3] != "5215511111111@s.whatsapp.net" || c.Args[4] != "123@lid" {
		t.Fatalf("group lookup must match on every sender alias: %+v", c)
	}
	r.SenderAliases = nil
	// Our own reaction in a group matches on from_me alone.
	db.Reset()
	r.FromMe = true
	if err := st.UpsertReaction(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	if has(db.Calls()[0], "sender_jid IN") {
		t.Fatalf("own reaction must not match on sender_jid: %s", db.Calls()[0].Query)
	}
}

func TestUpsertReactionReplacesPreviousReactionOfSameSender(t *testing.T) {
	st, db, _ := newStore(existingReaction("R1"))
	if err := st.UpsertReaction(t.Context(), reaction("\U0001F602")); err != nil {
		t.Fatal(err)
	}
	calls := db.Calls()
	if countCalls(calls, "INSERT") != 0 {
		t.Fatalf("a replacement must not insert: %+v", calls)
	}
	up := calls[1]
	if !has(up, "UPDATE wa_messages SET message_type = 'reaction', body = ?", "WHERE id = ? AND timestamp <= ?") ||
		up.Args[0] != "\U0001F602" || up.Args[2] != "R1" {
		t.Fatalf("unexpected replace statement %+v", up)
	}
}

func TestUpsertReactionEmptyEmojiRemovesWithoutDelete(t *testing.T) {
	st, db, _ := newStore(existingReaction("R1"))
	if err := st.UpsertReaction(t.Context(), reaction("")); err != nil {
		t.Fatal(err)
	}
	calls := db.Calls()
	if !has(calls[1], "SET message_type = 'system', body = NULL") || calls[1].Args[1] != "R1" {
		t.Fatalf("removal must soft-delete the row: %+v", calls[1])
	}
	for _, c := range calls {
		if has(c, "DELETE") {
			t.Fatalf("the bridge has no DELETE grant: %s", c.Query)
		}
	}
	if !has(calls[len(calls)-1], "status_updated_at") {
		t.Fatal("removal must also touch the reacted-to message")
	}
}

func TestUpsertReactionRemovalOfUnknownReactionIsANoop(t *testing.T) {
	st, db, _ := newStore(existingReaction(""))
	if err := st.UpsertReaction(t.Context(), reaction("")); err != nil {
		t.Fatal(err)
	}
	if n := len(db.Calls()); n != 1 {
		t.Fatalf("only the lookup should run, got %d statements", n)
	}
}

func TestUpsertReactionWithoutTargetColumnDegradesToPlainRow(t *testing.T) {
	st, db, logs := newStore(func(c testdb.Call) testdb.Result {
		if has(c, "target_message_id") {
			return testdb.Result{Err: errBadField}
		}
		return testdb.Result{}
	})
	if err := st.UpsertReaction(t.Context(), reaction("\U0001F44D")); err != nil {
		t.Fatal(err)
	}
	calls := db.Calls()
	last := calls[len(calls)-1]
	if !has(last, "INSERT IGNORE INTO wa_messages") || has(last, "target_message_id") {
		t.Fatalf("expected the legacy insert, got %s", last.Query)
	}
	if len(*logs) != 1 {
		t.Fatalf("one log line expected: %v", *logs)
	}
	assertASCII(t, *logs)
}

func TestHeartbeatToleratesMissingColumn(t *testing.T) {
	st, db, logs := newStore(func(c testdb.Call) testdb.Result {
		if has(c, "last_seen_at") {
			return testdb.Result{Err: errBadField}
		}
		return testdb.Result{}
	})
	for i := 0; i < 3; i++ {
		if err := st.Heartbeat(t.Context()); err != nil {
			t.Fatalf("Heartbeat must not fail without the column: %v", err)
		}
	}
	// First call: the features statement, then the legacy one. Both columns
	// are now known missing, so later calls run nothing.
	if n := len(db.Calls()); n != 2 {
		t.Fatalf("after the first failure no more statements should run, got %d", n)
	}
	if len(*logs) != 2 {
		t.Fatalf("log once per feature, got %v", *logs)
	}
	assertASCII(t, *logs)
}

func TestHeartbeatWritesFeatures(t *testing.T) {
	st, db, _ := newStore(nil)
	if err := st.Heartbeat(t.Context()); err != nil {
		t.Fatal(err)
	}
	cs := db.Calls()
	if len(cs) != 1 {
		t.Fatalf("one UPDATE expected: %+v", cs)
	}
	c := cs[0]
	if !has(c, "UPDATE wa_pairing SET last_seen_at = UTC_TIMESTAMP(), bridge_features = ?, bridge_features_at = UTC_TIMESTAMP() WHERE id = 1") {
		t.Fatalf("unexpected heartbeat %s", c.Query)
	}
	if len(c.Args) != 1 || c.Args[0] != BridgeFeatures || !strings.Contains(BridgeFeatures, "new_chat") {
		t.Fatalf("features arg %v", c.Args)
	}
}

func TestHeartbeatFeaturesFallback(t *testing.T) {
	st, db, logs := newStore(func(c testdb.Call) testdb.Result {
		if has(c, "bridge_features") {
			return testdb.Result{Err: errBadField}
		}
		return testdb.Result{}
	})
	for i := 0; i < 2; i++ {
		if err := st.Heartbeat(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	cs := db.Calls()
	// 1st: features (fails) + legacy. 2nd: legacy only.
	if len(cs) != 3 {
		t.Fatalf("statements: %+v", cs)
	}
	for _, c := range cs[1:] {
		if c.Query != "UPDATE wa_pairing SET last_seen_at = UTC_TIMESTAMP() WHERE id = 1" {
			t.Fatalf("legacy heartbeat expected, got %s", c.Query)
		}
	}
	if len(*logs) != 1 {
		t.Fatalf("one log line: %v", *logs)
	}
}

func TestPermanentFailWritesErrorCode(t *testing.T) {
	st, db, _ := newStore(nil)
	if err := st.FailOutbound(t.Context(), 7, 5, "NOT_ON_WHATSAPP", "number is not on WhatsApp"); err != nil {
		t.Fatal(err)
	}
	c := db.Calls()[0]
	if !has(c, "status = 'failed'", "error_code = ?") {
		t.Fatalf("unexpected %s", c.Query)
	}
	if c.Args[0] != int64(5) || c.Args[1] != "NOT_ON_WHATSAPP: number is not on WhatsApp" || c.Args[2] != "NOT_ON_WHATSAPP" || c.Args[3] != int64(7) {
		t.Fatalf("args %v", c.Args)
	}
}

func TestPermanentFailLegacyFallbackKeepsCodeInMessage(t *testing.T) {
	st, db, logs := newStore(func(c testdb.Call) testdb.Result {
		if has(c, "error_code") {
			return testdb.Result{Err: errBadField}
		}
		return testdb.Result{}
	})
	for i := 0; i < 2; i++ {
		if err := st.FailOutbound(t.Context(), 7, 5, "INVALID_JID", "bad"); err != nil {
			t.Fatal(err)
		}
	}
	cs := db.Calls()
	if len(cs) != 3 {
		t.Fatalf("statements: %+v", cs)
	}
	for _, c := range cs[1:] {
		if has(c, "error_code") || c.Args[1] != "INVALID_JID: bad" {
			t.Fatalf("legacy fail expected: %s %v", c.Query, c.Args)
		}
	}
	if len(*logs) != 1 {
		t.Fatalf("one log line: %v", *logs)
	}
}

func TestFailOutboundClipsLongMessagesOnRuneBoundary(t *testing.T) {
	st, db, _ := newStore(nil)
	long := strings.Repeat("ñ", 400) // 800 bytes
	if err := st.FailOutbound(t.Context(), 1, 5, "SEND_FAILED", long); err != nil {
		t.Fatal(err)
	}
	msg := db.Calls()[0].Args[1].(string)
	if len(msg) > 500 || !utf8.ValidString(msg) {
		t.Fatalf("clipped message len %d valid %v", len(msg), utf8.ValidString(msg))
	}
}

func TestMarkNewChatSentClearsErrorAndSetsChat(t *testing.T) {
	st, db, _ := newStore(nil)
	if err := st.MarkNewChatSent(t.Context(), 9, sql.NullString{String: "1@lid", Valid: true}, "WAID"); err != nil {
		t.Fatal(err)
	}
	c := db.Calls()[0]
	if !has(c, "status = 'sent'", "chat_jid = ?", "error_code = NULL") || c.Args[0] != "1@lid" || c.Args[1] != "WAID" || c.Args[2] != int64(9) {
		t.Fatalf("unexpected %s %v", c.Query, c.Args)
	}
}

func tailRows(msgType string, body, filename any, revoked int64) testdb.Result {
	return testdb.Result{
		Cols: []string{"message_type", "body", "media_filename", "revoked", "from_me", "timestamp"},
		Rows: [][]driver.Value{{msgType, body, filename, revoked, int64(0), t0}},
	}
}

func TestRecomputeChatTailFallsBackAndExcludesReactions(t *testing.T) {
	st, db, _ := newStore(func(c testdb.Call) testdb.Result {
		if has(c, "revoked_at") {
			return testdb.Result{Err: errBadField}
		}
		if c.IsQuery {
			return tailRows("text", "hola", nil, 0)
		}
		return testdb.Result{}
	})
	var got TailRow
	if err := st.RecomputeChatTail(t.Context(), "chat", func(r TailRow) string { got = r; return "P" }); err != nil {
		t.Fatal(err)
	}
	calls := db.Calls()
	if len(calls) != 3 || has(calls[1], "revoked_at") {
		t.Fatalf("expected new select, legacy select, update: %+v", calls)
	}
	for _, c := range calls[:2] {
		if !has(c, "message_type NOT IN ('reaction', 'system')") {
			t.Fatalf("reactions and system rows must not become the tail: %s", c.Query)
		}
	}
	up := calls[2]
	if !has(up, "UPDATE wa_chats SET last_message_at = ?, last_message_preview = ?, last_message_from_me = ?") || up.Args[1] != "P" || up.Args[3] != "chat" {
		t.Fatalf("unexpected tail update %+v", up)
	}
	if got.MessageType != "text" || got.Body.String != "hola" {
		t.Fatalf("preview got %+v", got)
	}
}

func TestRecomputeChatTailPassesMediaAndRevokedToPreview(t *testing.T) {
	st, db, _ := newStore(func(c testdb.Call) testdb.Result {
		if c.IsQuery {
			return tailRows("image", nil, nil, 1)
		}
		return testdb.Result{}
	})
	var got TailRow
	if err := st.RecomputeChatTail(t.Context(), "chat", func(r TailRow) string { got = r; return "x" }); err != nil {
		t.Fatal(err)
	}
	if got.MessageType != "image" || got.Body.Valid || !got.Revoked {
		t.Fatalf("got %+v", got)
	}
	// No countable row: the chat is left as it is.
	st, db, _ = newStore(nil)
	if err := st.RecomputeChatTail(t.Context(), "chat", func(TailRow) string { return "x" }); err != nil {
		t.Fatal(err)
	}
	if countCalls(db.Calls(), "UPDATE") != 0 {
		t.Fatal("empty chat must not be updated")
	}
}

func TestClearUnreadUpToCountsOnlyNewerInbound(t *testing.T) {
	st, db, _ := newStore(nil)
	if err := st.ClearUnreadUpTo(t.Context(), "chat@lid", []string{"A", "B"}); err != nil {
		t.Fatal(err)
	}
	c := db.Calls()[0]
	if !has(c, "UPDATE wa_chats c", "LEAST(c.unread_count", "m.from_me = 0", "r.id IN (?,?)", "WHERE c.jid = ? AND c.unread_count > 0") ||
		c.Args[0] != "A" || c.Args[1] != "B" || c.Args[2] != "chat@lid" {
		t.Fatalf("unexpected %+v", c)
	}
	db.Reset()
	if err := st.ClearUnreadUpTo(t.Context(), "chat", nil); err != nil || len(db.Calls()) != 0 {
		t.Fatal("no ids, nothing to do")
	}
}

func TestGetQuotedRow(t *testing.T) {
	st, db, _ := newStore(func(c testdb.Call) testdb.Result {
		return testdb.Result{
			Cols: []string{"sender_jid", "from_me", "message_type", "body", "media_filename", "revoked"},
			Rows: [][]driver.Value{{"5215500000000@s.whatsapp.net", int64(1), "document", nil, "guia.pdf", int64(0)}},
		}
	})
	row, found, err := st.GetQuotedRow(t.Context(), "chat", "ID")
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if !row.FromMe || row.MessageType != "document" || row.Body.Valid || row.MediaFilename.String != "guia.pdf" || row.Revoked || !row.Quotable() {
		t.Fatalf("unexpected row %+v", row)
	}
	if !has(db.Calls()[0], "revoked_at IS NOT NULL") {
		t.Fatal("the quote lookup must read revoked_at")
	}
	st, _, _ = newStore(nil)
	if _, found, err := st.GetQuotedRow(t.Context(), "chat", "ID"); found || err != nil {
		t.Fatalf("missing row: found=%v err=%v", found, err)
	}
	// Old schema: falls back without revoked_at.
	st, db, _ = newStore(func(c testdb.Call) testdb.Result {
		if has(c, "revoked_at") {
			return testdb.Result{Err: errBadField}
		}
		return testdb.Result{
			Cols: []string{"sender_jid", "from_me", "message_type", "body", "media_filename", "0"},
			Rows: [][]driver.Value{{nil, int64(0), "text", "hola", nil, int64(0)}},
		}
	})
	if row, found, err := st.GetQuotedRow(t.Context(), "chat", "ID"); err != nil || !found || row.Body.String != "hola" {
		t.Fatalf("legacy: row=%+v found=%v err=%v", row, found, err)
	}
}

func TestQuotedRowQuotable(t *testing.T) {
	for _, c := range []struct {
		row  QuotedRow
		want bool
	}{
		{QuotedRow{MessageType: "text"}, true},
		{QuotedRow{MessageType: "image"}, true},
		{QuotedRow{MessageType: "text", Revoked: true}, false},
		{QuotedRow{MessageType: "revoked"}, false},
		{QuotedRow{MessageType: "reaction"}, false},
		{QuotedRow{MessageType: "system"}, false},
	} {
		if got := c.row.Quotable(); got != c.want {
			t.Errorf("%+v: got %v", c.row, got)
		}
	}
}
