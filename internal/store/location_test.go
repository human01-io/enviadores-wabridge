package store

import (
	"database/sql"
	"testing"

	"github.com/enviadores/wabridge/internal/testdb"
	"github.com/go-sql-driver/mysql"
)

func badColumn(col string) error {
	return &mysql.MySQLError{Number: 1054, Message: "Unknown column '" + col + "' in 'field list'"}
}

func placeMessage(id string) Message {
	return Message{
		ID: id, ChatJID: "chat@lid", MessageType: "location",
		Body:              sql.NullString{String: "19.432608, -99.133208", Valid: true},
		MediaThumbnailURL: sql.NullString{String: "https://m/abc_thumb.jpg", Valid: true},
		QuotedMessageID:   sql.NullString{String: "Q1", Valid: true},
		QuotedPreview:     sql.NullString{String: "hola", Valid: true},
		LocationName:      sql.NullString{String: "Sucursal Centro", Valid: true},
		LocationAddress:   sql.NullString{String: "Av. Juarez 10", Valid: true},
		Timestamp:         t0,
	}
}

func TestInsertMessageWritesPlaceColumns(t *testing.T) {
	st, db, logs := newStore(nil)
	if err := st.InsertMessage(t.Context(), placeMessage("L1")); err != nil {
		t.Fatal(err)
	}
	calls := db.Calls()
	if len(calls) != 1 || !has(calls[0], "quoted_preview", "location_name, location_address", "media_thumbnail_url") {
		t.Fatalf("expected one full insert, got %+v", calls)
	}
	n := len(calls[0].Args)
	if calls[0].Args[n-2] != "Sucursal Centro" || calls[0].Args[n-1] != "Av. Juarez 10" {
		t.Fatalf("place args %v", calls[0].Args[n-2:])
	}
	if len(*logs) != 0 {
		t.Fatalf("logs %v", *logs)
	}
}

func TestInsertMessageWithoutPlaceColumnsFallsBackOnceKeepingQuotes(t *testing.T) {
	st, db, logs := newStore(func(c testdb.Call) testdb.Result {
		if has(c, "location_name") {
			return testdb.Result{Err: badColumn("location_name")}
		}
		return testdb.Result{}
	})
	if err := st.InsertMessage(t.Context(), placeMessage("L1")); err != nil {
		t.Fatalf("InsertMessage: %v", err)
	}
	calls := db.Calls()
	if len(calls) != 2 || !has(calls[0], "location_name") || has(calls[1], "location_name") || !has(calls[1], "quoted_preview", "media_thumbnail_url") {
		t.Fatalf("expected place insert then insert without place (quotes kept), got %+v", calls)
	}
	if !st.FeatureOff(featureLocation) || st.FeatureOff(featureQuotes) {
		t.Fatal("only the location feature may be switched off")
	}
	db.Reset()
	if err := st.InsertMessage(t.Context(), placeMessage("L2")); err != nil {
		t.Fatal(err)
	}
	if calls = db.Calls(); len(calls) != 1 || has(calls[0], "location_name") || !has(calls[0], "quoted_preview") {
		t.Fatalf("second insert must skip the place columns directly: %+v", calls)
	}
	if len(*logs) != 1 {
		t.Fatalf("expected exactly one log line, got %v", *logs)
	}
	assertASCII(t, *logs)
}

func TestInsertMessageWithNeitherQuoteNorPlaceColumns(t *testing.T) {
	for _, first := range []string{"quoted_preview", "location_name"} {
		st, db, logs := newStore(func(c testdb.Call) testdb.Result {
			// MySQL names one missing column; which one depends on order.
			if has(c, first) {
				return testdb.Result{Err: badColumn(first)}
			}
			if has(c, "quoted_preview") {
				return testdb.Result{Err: badColumn("quoted_preview")}
			}
			if has(c, "location_name") {
				return testdb.Result{Err: badColumn("location_name")}
			}
			return testdb.Result{}
		})
		if err := st.InsertMessage(t.Context(), placeMessage("L1")); err != nil {
			t.Fatalf("%s: %v", first, err)
		}
		calls := db.Calls()
		last := calls[len(calls)-1]
		if has(last, "quoted_preview") || has(last, "location_name") || !has(last, "quoted_message_id", "media_thumbnail_url") {
			t.Fatalf("%s: last statement must be the legacy insert: %+v", first, calls)
		}
		if !st.FeatureOff(featureQuotes) || !st.FeatureOff(featureLocation) || len(*logs) != 2 {
			t.Fatalf("%s: both features off, one line each (logs %v)", first, *logs)
		}
	}
}

func TestInsertMessageWithoutPlaceNeverTouchesPlaceColumns(t *testing.T) {
	st, db, _ := newStore(nil)
	m := placeMessage("L1")
	m.LocationName, m.LocationAddress = sql.NullString{}, sql.NullString{}
	if err := st.InsertMessage(t.Context(), m); err != nil {
		t.Fatal(err)
	}
	if calls := db.Calls(); len(calls) != 1 || has(calls[0], "location_name") {
		t.Fatalf("a bare pin / any other message keeps the old statement: %+v", calls)
	}
}

func TestRecomputeChatTailCarriesPlaceName(t *testing.T) {
	st, _, _ := newStore(func(c testdb.Call) testdb.Result {
		if c.IsQuery {
			return tailRowsAt("location", "19.4, -99.1", nil, 0, "Sucursal Centro")
		}
		return testdb.Result{}
	})
	var got TailRow
	if err := st.RecomputeChatTail(t.Context(), "chat", func(r TailRow) string { got = r; return "x" }); err != nil {
		t.Fatal(err)
	}
	if got.LocationName.String != "Sucursal Centro" {
		t.Fatalf("got %+v", got)
	}
}

func TestRecomputeChatTailWithoutPlaceColumnKeepsRevoked(t *testing.T) {
	st, db, logs := newStore(func(c testdb.Call) testdb.Result {
		if has(c, "location_name") {
			return testdb.Result{Err: badColumn("location_name")}
		}
		if c.IsQuery {
			return tailRows("location", "19.4, -99.1", nil, 1)
		}
		return testdb.Result{}
	})
	var got TailRow
	if err := st.RecomputeChatTail(t.Context(), "chat", func(r TailRow) string { got = r; return "x" }); err != nil {
		t.Fatal(err)
	}
	calls := db.Calls()
	if len(calls) != 3 || has(calls[1], "location_name") || !has(calls[1], "revoked_at IS NOT NULL", ", NULL FROM") {
		t.Fatalf("expected select, select without place but with revoked_at, update: %+v", calls)
	}
	if !got.Revoked || got.LocationName.Valid || st.FeatureOff(featureEdits) || len(*logs) != 1 {
		t.Fatalf("got %+v edits off=%v logs=%v", got, st.FeatureOff(featureEdits), *logs)
	}
}

func TestUnknownColumnName(t *testing.T) {
	for in, want := range map[error]string{
		badColumn("location_name"):                      "location_name",
		badColumn("m.location_name"):                    "location_name",
		&mysql.MySQLError{Number: 1054, Message: "odd"}: "",
		sql.ErrNoRows:                                   "",
	} {
		if got := unknownColumnName(in); got != want {
			t.Errorf("%v: got %q want %q", in, got, want)
		}
	}
}
