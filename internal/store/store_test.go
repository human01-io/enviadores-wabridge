package store

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"
	"time"

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

func TestApplyEditKeepsFirstOriginalAndStampsEditedAt(t *testing.T) {
	st, db, _ := newStore(func(c testdb.Call) testdb.Result {
		if c.IsQuery && has(c, "SELECT 1") {
			return testdb.Result{Cols: []string{"1"}, Rows: [][]driver.Value{{int64(1)}}}
		}
		return testdb.Result{}
	})
	found, err := st.ApplyEdit(t.Context(), "chat@lid", "MSG1", false, sql.NullString{String: "Calle Nueva 5", Valid: true})
	if err != nil || !found {
		t.Fatalf("ApplyEdit found=%v err=%v", found, err)
	}
	calls := db.Calls()
	if len(calls) != 2 {
		t.Fatalf("expected UPDATE + existence check, got %+v", calls)
	}
	up := calls[0]
	if !has(up, "UPDATE wa_messages", "original_body = COALESCE(original_body, body)", "body = ?", "edited_at = UTC_TIMESTAMP()", "WHERE id = ? AND chat_jid = ? AND from_me = ?") {
		t.Fatalf("unexpected edit statement: %s", up.Query)
	}
	// original_body must be assigned before body (MySQL evaluates SET left to right).
	if strings.Index(up.Query, "original_body = COALESCE") > strings.Index(up.Query, "body = ?") {
		t.Fatalf("original_body must be assigned before body: %s", up.Query)
	}
	if up.Args[0] != "Calle Nueva 5" || up.Args[1] != "MSG1" || up.Args[2] != "chat@lid" || up.Args[3] != int64(0) {
		t.Fatalf("unexpected args %v", up.Args)
	}
	if has(up, "INSERT") {
		t.Fatal("an edit must never insert a row")
	}
}

func TestApplyEditMissingOriginalReportsNotFound(t *testing.T) {
	st, _, _ := newStore(nil) // existence query returns no rows
	found, err := st.ApplyEdit(t.Context(), "chat", "GONE", true, sql.NullString{String: "x", Valid: true})
	if err != nil || found {
		t.Fatalf("found=%v err=%v, want false,nil", found, err)
	}
}

func TestApplyEditWithoutNewColumnsStillUpdatesBody(t *testing.T) {
	st, db, logs := newStore(func(c testdb.Call) testdb.Result {
		if has(c, "original_body") {
			return testdb.Result{Err: errBadField}
		}
		if c.IsQuery {
			return testdb.Result{Cols: []string{"1"}, Rows: [][]driver.Value{{int64(1)}}}
		}
		return testdb.Result{}
	})
	found, err := st.ApplyEdit(t.Context(), "chat", "M", false, sql.NullString{String: "new", Valid: true})
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	calls := db.Calls()
	if !has(calls[1], "UPDATE wa_messages SET body = ?") || has(calls[1], "original_body") {
		t.Fatalf("expected legacy body-only update, got %s", calls[1].Query)
	}
	if len(*logs) != 1 {
		t.Fatalf("one log line expected, got %v", *logs)
	}
	assertASCII(t, *logs)
}

func TestRevokeMessageTargetsOriginalById(t *testing.T) {
	st, db, _ := newStore(func(c testdb.Call) testdb.Result {
		if c.IsQuery {
			return testdb.Result{Cols: []string{"1"}, Rows: [][]driver.Value{{int64(1)}}}
		}
		return testdb.Result{}
	})
	found, err := st.RevokeMessage(t.Context(), "chat@lid", "ORIG")
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	up := db.Calls()[0]
	if !has(up, "UPDATE wa_messages", "revoked_at = COALESCE(revoked_at, UTC_TIMESTAMP())", "WHERE id = ? AND chat_jid = ?") ||
		up.Args[0] != "ORIG" || up.Args[1] != "chat@lid" {
		t.Fatalf("unexpected revoke statement %+v", up)
	}
	if has(up, "body") {
		t.Fatal("revoke must keep the body for audit")
	}
}

func TestRevokeMessageMissingOriginalOrOldSchema(t *testing.T) {
	// Original missing: the existence check finds nothing.
	st, _, _ := newStore(nil)
	if found, err := st.RevokeMessage(t.Context(), "chat", "X"); err != nil || found {
		t.Fatalf("missing original: found=%v err=%v", found, err)
	}
	// Old schema: no revoked_at; caller must fall back to inserting a 'revoked' row.
	st, db, logs := newStore(func(c testdb.Call) testdb.Result {
		if has(c, "revoked_at") {
			return testdb.Result{Err: errBadField}
		}
		return testdb.Result{}
	})
	if found, err := st.RevokeMessage(t.Context(), "chat", "X"); err != nil || found {
		t.Fatalf("old schema: found=%v err=%v", found, err)
	}
	if found, _ := st.RevokeMessage(t.Context(), "chat", "Y"); found {
		t.Fatal("old schema must keep reporting not found")
	}
	if n := len(db.Calls()); n != 1 || len(*logs) != 1 {
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
	if !has(db.Calls()[0], "AND sender_jid = ?") {
		t.Fatalf("group lookup must match on sender_jid: %s", db.Calls()[0].Query)
	}
	// Our own reaction in a group matches on from_me alone.
	db.Reset()
	r.FromMe = true
	if err := st.UpsertReaction(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	if has(db.Calls()[0], "sender_jid = ?") {
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
	if n := len(db.Calls()); n != 1 {
		t.Fatalf("after the first failure no more statements should run, got %d", n)
	}
	if len(*logs) != 1 {
		t.Fatalf("log once, got %v", *logs)
	}
	assertASCII(t, *logs)
}

func TestHeartbeatWritesLastSeenAt(t *testing.T) {
	st, db, _ := newStore(nil)
	if err := st.Heartbeat(t.Context()); err != nil {
		t.Fatal(err)
	}
	if c := db.Calls()[0]; !has(c, "UPDATE wa_pairing SET last_seen_at = NOW() WHERE id = 1") {
		t.Fatalf("unexpected heartbeat %s", c.Query)
	}
}

func TestRecomputeChatTailFallsBackAndExcludesReactions(t *testing.T) {
	st, db, _ := newStore(func(c testdb.Call) testdb.Result {
		if has(c, "revoked_at") {
			return testdb.Result{Err: errBadField}
		}
		return testdb.Result{}
	})
	if err := st.RecomputeChatTail(t.Context(), "chat"); err != nil {
		t.Fatal(err)
	}
	calls := db.Calls()
	if len(calls) != 2 || !has(calls[0], "Mensaje eliminado") || has(calls[1], "revoked_at") {
		t.Fatalf("expected new then legacy tail statement: %+v", calls)
	}
	for _, c := range calls {
		if !has(c, "message_type NOT IN ('reaction', 'system')") {
			t.Fatalf("reactions and system rows must not become the tail: %s", c.Query)
		}
	}
}

func TestClearUnread(t *testing.T) {
	st, db, _ := newStore(nil)
	if err := st.ClearUnread(t.Context(), "chat@lid"); err != nil {
		t.Fatal(err)
	}
	c := db.Calls()[0]
	if !has(c, "UPDATE wa_chats SET unread_count = 0 WHERE jid = ?") || c.Args[0] != "chat@lid" {
		t.Fatalf("unexpected %+v", c)
	}
}

func TestGetQuotedRow(t *testing.T) {
	st, _, _ := newStore(func(c testdb.Call) testdb.Result {
		return testdb.Result{
			Cols: []string{"sender_jid", "from_me", "message_type", "body", "media_filename"},
			Rows: [][]driver.Value{{"5215500000000@s.whatsapp.net", int64(1), "document", nil, "guia.pdf"}},
		}
	})
	row, found, err := st.GetQuotedRow(t.Context(), "chat", "ID")
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if !row.FromMe || row.MessageType != "document" || row.Body.Valid || row.MediaFilename.String != "guia.pdf" {
		t.Fatalf("unexpected row %+v", row)
	}
	st, _, _ = newStore(nil)
	if _, found, err := st.GetQuotedRow(t.Context(), "chat", "ID"); found || err != nil {
		t.Fatalf("missing row: found=%v err=%v", found, err)
	}
}
