package wabridge

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	"github.com/enviadores/wabridge/internal/store"
	"github.com/enviadores/wabridge/internal/testdb"
	"github.com/go-sql-driver/mysql"
	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
)

const (
	branchName    = "Enviadores: Centro de Envios Amatitlan"
	branchAddress = "Av Cuauhtemoc 53, Local 2, Cuernavaca, Morelos 62410"
	branchURL     = "https://www.google.com/maps/search/?api=1&query=18.921200,-99.226500&query_place_id=ChIJlfvaNq7fzYURox60ozSWZmo"
	branchBody    = "18.921200, -99.226500"
	customerChat  = "123456789012345@lid"
)

// placeDB answers the wa_outbound place SELECT and the quoted-row lookup.
// place nil answers the SELECT with err (or an all-NULL row when err is nil
// too); every other statement succeeds unless fail says so.
type placeDB struct {
	place []driver.Value
	err   error
	fail  func(c testdb.Call) error
}

func (p *placeDB) handler(c testdb.Call) testdb.Result {
	if p.fail != nil {
		if err := p.fail(c); err != nil {
			return testdb.Result{Err: err}
		}
	}
	switch {
	case c.IsQuery && strings.Contains(c.Query, "wa_outbound.location_name"):
		if p.err != nil {
			return testdb.Result{Err: p.err}
		}
		row := p.place
		if row == nil {
			row = []driver.Value{nil, nil, nil}
		}
		return testdb.Result{Cols: []string{"location_name", "location_address", "location_url"}, Rows: [][]driver.Value{row}}
	case c.IsQuery && strings.Contains(c.Query, "FROM wa_messages WHERE id = ? AND chat_jid = ?"):
		return testdb.Result{
			Cols: []string{"sender_jid", "from_me", "message_type", "body", "media_filename", "revoked"},
			Rows: [][]driver.Value{{customerChat, int64(0), "text", "¿Dónde están?", nil, int64(0)}},
		}
	}
	return testdb.Result{}
}

func branchPlace() []driver.Value { return []driver.Value{branchName, branchAddress, branchURL} }

// locationSendBridge records every message handed to WhatsApp.
func locationSendBridge(p *placeDB) (*Bridge, *testdb.DB, *[]*waProto.Message) {
	b, db := newTestBridge(p.handler)
	var sent []*waProto.Message
	b.sendMessage = func(_ context.Context, to types.JID, msg *waProto.Message, _ ...whatsmeow.SendRequestExtra) (whatsmeow.SendResponse, error) {
		if to.String() != customerChat {
			panic("sent to " + to.String())
		}
		sent = append(sent, msg)
		return whatsmeow.SendResponse{ID: "3EB0LOC", Timestamp: sentAt}, nil
	}
	return b, db, &sent
}

func locationRow(body string) outboundRow {
	return outboundRow{ID: 77, ClientRequestID: "req-loc-1", ChatJID: customerChat, Body: body, Kind: "location"}
}

// Positions in the full wa_messages INSERT (see store.insertMessage).
const (
	argBody            = 5
	argQuotedMessageID = 12
	argClientRequestID = 16
	argLocationName    = 19
	argLocationAddress = 20
	argChatPreview     = 7 // wa_chats upsert
)

func TestSendLocationBuildsNativePin(t *testing.T) {
	b, db, sent := locationSendBridge(&placeDB{place: branchPlace()})
	b.sendOutboundRow(t.Context(), locationRow(branchBody))

	if len(*sent) != 1 {
		t.Fatalf("one send expected, got %d", len(*sent))
	}
	msg := (*sent)[0]
	loc := msg.GetLocationMessage()
	if loc == nil || msg.Conversation != nil || msg.ExtendedTextMessage != nil {
		t.Fatalf("must send a LocationMessage only: %v", msg)
	}
	if loc.GetDegreesLatitude() != 18.9212 || loc.GetDegreesLongitude() != -99.2265 {
		t.Fatalf("coordinates %v, %v", loc.GetDegreesLatitude(), loc.GetDegreesLongitude())
	}
	if loc.GetName() != branchName || loc.GetAddress() != branchAddress || loc.GetURL() != branchURL {
		t.Fatalf("place: name %q address %q url %q", loc.GetName(), loc.GetAddress(), loc.GetURL())
	}
	if len(loc.JPEGThumbnail) != 0 || loc.ContextInfo != nil {
		t.Fatal("no thumbnail and no quote on a plain pin")
	}

	if got := writesOf(db); strings.Join(got, ",") != "sent,message,chat" {
		t.Fatalf("writes %v", got)
	}
	ins := calls(db, "INSERT IGNORE INTO wa_messages")[0]
	if !strings.Contains(ins.Query, "location_name, location_address") {
		t.Fatalf("insert must carry the place columns: %s", ins.Query)
	}
	if ins.Args[4] != "location" || ins.Args[argBody] != branchBody || ins.Args[argClientRequestID] != "req-loc-1" ||
		ins.Args[argLocationName] != branchName || ins.Args[argLocationAddress] != branchAddress {
		t.Fatalf("wa_messages row %v", ins.Args)
	}
	chat := calls(db, "INSERT INTO wa_chats")[0]
	if chat.Args[argChatPreview] != "📍 "+branchName {
		t.Fatalf("chat preview %v", chat.Args[argChatPreview])
	}
}

func TestSendLocationNormalizesBody(t *testing.T) {
	b, db, sent := locationSendBridge(&placeDB{place: branchPlace()})
	b.sendOutboundRow(t.Context(), locationRow(" 18.9212 ,-99.2265 "))
	if len(*sent) != 1 {
		t.Fatal("a valid but loosely spaced body must still send")
	}
	if got := calls(db, "INSERT IGNORE INTO wa_messages")[0].Args[argBody]; got != branchBody {
		t.Fatalf("stored body %q, want the 6-decimal form", got)
	}
}

func TestSendLocationAsReply(t *testing.T) {
	b, db, sent := locationSendBridge(&placeDB{place: branchPlace()})
	r := locationRow(branchBody)
	r.QuotedMessageID = sql.NullString{String: "3EB0ASK", Valid: true}
	b.sendOutboundRow(t.Context(), r)

	if len(*sent) != 1 {
		t.Fatalf("one send expected, got %d", len(*sent))
	}
	loc := (*sent)[0].GetLocationMessage()
	ci := loc.GetContextInfo()
	if loc == nil || ci.GetStanzaID() != "3EB0ASK" || ci.GetParticipant() != customerChat {
		t.Fatalf("quote must ride on the LocationMessage: %v", (*sent)[0])
	}
	if ci.GetQuotedMessage().GetConversation() != "¿Dónde están?" {
		t.Fatalf("quoted content %v", ci.GetQuotedMessage())
	}
	ins := calls(db, "INSERT IGNORE INTO wa_messages")[0]
	if ins.Args[argQuotedMessageID] != "3EB0ASK" || ins.Args[argLocationName] != branchName {
		t.Fatalf("wa_messages row %v", ins.Args)
	}
}

func TestSendLocationMalformedBodyFailsPermanently(t *testing.T) {
	for _, body := range []string{
		"", "18.9212", "18.9212, -99.2265, 3", "abc, def", "NaN, 1", "Inf, 1",
		"1e1, 2", "0x1p-2, 1", "91, 0", "0, 181", "-90.5, 0", "18.9212; -99.2265",
	} {
		p := &placeDB{place: branchPlace()}
		b, db, sent := locationSendBridge(p)
		b.sendOutboundRow(t.Context(), locationRow(body))
		if len(*sent) != 0 {
			t.Errorf("%q: must not send", body)
			continue
		}
		f := failedCall(t, db)
		if f.Args[0] != int64(outboundMaxAttempts) || f.Args[2] != codeInvalidLocation {
			t.Errorf("%q: fail args %v", body, f.Args)
		}
		if len(calls(db, "wa_outbound.location_name")) != 0 || len(calls(db, "SET attempts = ?")) != 0 {
			t.Errorf("%q: a bad body is not retried and needs no place lookup", body)
		}
	}
}

func TestParseCoordinatesBounds(t *testing.T) {
	for body, want := range map[string][2]float64{
		"90, 180":               {90, 180},
		"-90.000000, -180.0":    {-90, -180},
		"0, 0":                  {0, 0},
		"18.921200,-99.226500":  {18.9212, -99.2265},
		"\t18.9212 , -99.2265 ": {18.9212, -99.2265},
	} {
		lat, lng, ok := parseCoordinates(body)
		if !ok || lat != want[0] || lng != want[1] {
			t.Errorf("%q: got %v, %v, %v", body, lat, lng, ok)
		}
	}
}

func TestSendLocationPlaceReadErrorRetries(t *testing.T) {
	b, db, sent := locationSendBridge(&placeDB{err: errors.New("connection reset")})
	b.sendOutboundRow(t.Context(), locationRow(branchBody))
	if len(*sent) != 0 {
		t.Fatal("must not send without knowing the place")
	}
	if got := writesOf(db); strings.Join(got, ",") != "attempt" {
		t.Fatalf("a transient read error must bump attempts only: %v", got)
	}
}

func TestSendLocationWithoutOutboundPlaceColumnsSendsBarePin(t *testing.T) {
	missing := &mysql.MySQLError{Number: 1054, Message: "Unknown column 'wa_outbound.location_name' in 'field list'"}
	b, db, sent := locationSendBridge(&placeDB{err: missing})
	b.sendOutboundRow(t.Context(), locationRow(branchBody))

	if len(*sent) != 1 {
		t.Fatal("the pin must still go out")
	}
	loc := (*sent)[0].GetLocationMessage()
	if loc.Name != nil || loc.Address != nil || loc.URL != nil || loc.GetDegreesLatitude() != 18.9212 {
		t.Fatalf("bare pin expected: %v", loc)
	}
	if !b.store.FeatureOff("wa_outbound.location") || b.store.FeatureOff("wa_messages.location") {
		t.Fatal("only the wa_outbound place feature may switch off")
	}
	ins := calls(db, "INSERT IGNORE INTO wa_messages")[0]
	if strings.Contains(ins.Query, "location_name") || ins.Args[4] != "location" || ins.Args[argBody] != branchBody {
		t.Fatalf("row without place: %s %v", ins.Query, ins.Args)
	}
	if got := calls(db, "INSERT INTO wa_chats")[0].Args[argChatPreview]; got != "📍 Ubicación" {
		t.Fatalf("chat preview %v", got)
	}
}

func TestSendLocationWithoutMessagePlaceColumnsKeepsTheRow(t *testing.T) {
	p := &placeDB{place: branchPlace()}
	p.fail = func(c testdb.Call) error {
		if strings.HasPrefix(c.Query, "INSERT IGNORE INTO wa_messages") && strings.Contains(c.Query, "location_name") {
			return &mysql.MySQLError{Number: 1054, Message: "Unknown column 'location_name' in 'field list'"}
		}
		return nil
	}
	b, db, sent := locationSendBridge(p)
	b.sendOutboundRow(t.Context(), locationRow(branchBody))
	if len(*sent) != 1 {
		t.Fatal("send expected")
	}
	ins := calls(db, "INSERT IGNORE INTO wa_messages")
	if len(ins) != 2 || strings.Contains(ins[1].Query, "location_name") || ins[1].Args[argClientRequestID] != "req-loc-1" {
		t.Fatalf("expected the insert to fall back without the place columns: %+v", ins)
	}
	if !b.store.FeatureOff("wa_messages.location") || b.store.FeatureOff("wa_outbound.location") {
		t.Fatal("only the wa_messages place feature may switch off")
	}
	// The chat list still names the place: it comes from the outbound row.
	if got := calls(db, "INSERT INTO wa_chats")[0].Args[argChatPreview]; got != "📍 "+branchName {
		t.Fatalf("chat preview %v", got)
	}
}

func TestSendLocationClipsPlaceToColumnWidths(t *testing.T) {
	long := []driver.Value{strings.Repeat("ñ", 300), strings.Repeat("é", 600), branchURL}
	b, db, sent := locationSendBridge(&placeDB{place: long})
	b.sendOutboundRow(t.Context(), locationRow(branchBody))
	loc := (*sent)[0].GetLocationMessage()
	if n := len([]rune(loc.GetName())); n != store.LocationNameMax {
		t.Fatalf("name runes %d", n)
	}
	ins := calls(db, "INSERT IGNORE INTO wa_messages")[0]
	if n := len([]rune(ins.Args[argLocationAddress].(string))); n != store.LocationAddressMax {
		t.Fatalf("address runes %d", n)
	}
}

func TestOldKindsStillGoThroughBuildOutboundMessage(t *testing.T) {
	b, db, sent := locationSendBridge(&placeDB{})
	b.sendOutboundRow(t.Context(), outboundRow{ID: 5, ClientRequestID: "r", ChatJID: customerChat, Body: "hola", Kind: "text"})
	if len(*sent) != 1 || (*sent)[0].GetConversation() != "hola" {
		t.Fatalf("text send %v", *sent)
	}
	if len(calls(db, "wa_outbound.location_name")) != 0 {
		t.Fatal("a text row must not read the place columns")
	}
}

// writesOf labels the statements that write, in order.
func writesOf(db *testdb.DB) []string {
	var out []string
	for _, c := range db.Calls() {
		switch {
		case c.IsQuery:
			continue
		case strings.HasPrefix(strings.TrimSpace(c.Query), "INSERT IGNORE INTO wa_messages"):
			out = append(out, "message")
		case strings.HasPrefix(strings.TrimSpace(c.Query), "INSERT INTO wa_chats"):
			out = append(out, "chat")
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
