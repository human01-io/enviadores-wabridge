package wabridge

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/enviadores/wabridge/internal/store"
	"github.com/enviadores/wabridge/internal/testdb"
	waProto "go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

// thumbStub records the previews handed to the thumbnail uploader.
type thumbStub struct {
	shas []string
	err  error
}

func (s *thumbStub) upload(sha string, jpeg []byte) (string, error) {
	s.shas = append(s.shas, sha)
	if s.err != nil {
		return "", s.err
	}
	return "https://media.example/" + sha + "_thumb.jpg", nil
}

func locationBridge(stub *thumbStub) (*Bridge, *testdb.DB) {
	b, db := newTestBridge(nil)
	b.uploadThumbnail = stub.upload
	return b, db
}

var mapTile = []byte{0xff, 0xd8, 0xff, 0xe0, 'm', 'a', 'p', 0xff, 0xd9}

// storedRow builds the wa_messages row the live and history paths write for a
// classified message: coordinates in body, map preview, place columns.
func storedRow(b *Bridge, id string, msg *waProto.Message) (string, store.Message) {
	msgType, body, info := classifyMessage(msg)
	name, address := locationFields(info)
	return msgType, store.Message{
		ID: id, ChatJID: "chat@lid", MessageType: msgType, Body: nullString(body),
		MediaThumbnailURL: b.uploadLocationPreview(id, info),
		LocationName:      name, LocationAddress: address,
	}
}

func TestLocationNamedPlaceWithMapPreview(t *testing.T) {
	stub := &thumbStub{}
	b, db := locationBridge(stub)
	msg := &waProto.Message{LocationMessage: &waProto.LocationMessage{
		DegreesLatitude:  proto.Float64(19.4326077),
		DegreesLongitude: proto.Float64(-99.133208),
		Name:             proto.String("  Enviadores Centro "),
		Address:          proto.String("Av. Juárez 10, Centro, CDMX"),
		URL:              proto.String("https://maps.google.com/?q=19.43,-99.13"),
		JPEGThumbnail:    mapTile,
	}}
	msgType, row := storedRow(b, "LOC1", msg)

	if msgType != "location" || row.Body.String != "19.432608, -99.133208" {
		t.Fatalf("type %q body %q: body must stay the 6-decimal coordinates", msgType, row.Body.String)
	}
	if row.LocationName.String != "Enviadores Centro" || row.LocationAddress.String != "Av. Juárez 10, Centro, CDMX" {
		t.Fatalf("place not captured: %+v %+v", row.LocationName, row.LocationAddress)
	}
	sum := sha256.Sum256(mapTile)
	if len(stub.shas) != 1 || stub.shas[0] != hex.EncodeToString(sum[:]) {
		t.Fatalf("map preview must go through the thumbnail path keyed by its hash: %v", stub.shas)
	}
	if !strings.HasSuffix(row.MediaThumbnailURL.String, "_thumb.jpg") {
		t.Fatalf("media_thumbnail_url %+v", row.MediaThumbnailURL)
	}
	if row.MediaURL.Valid || row.MediaMime.Valid {
		t.Fatal("a location has no media file")
	}

	_, _, info := classifyMessage(msg)
	if got := previewFor("location", row.Body.String, info); got != "📍 Enviadores Centro" {
		t.Fatalf("chat-list preview %q", got)
	}
	if got := quotedPreview(msg); got != "📍 Enviadores Centro" {
		t.Fatalf("quote preview %q", got)
	}

	if err := b.store.InsertMessage(t.Context(), row); err != nil {
		t.Fatal(err)
	}
	ins := calls(db, "INSERT IGNORE INTO wa_messages")
	if len(ins) != 1 || !strings.Contains(ins[0].Query, "location_name, location_address") {
		t.Fatalf("insert must write the place columns: %+v", ins)
	}
	n := len(ins[0].Args)
	if ins[0].Args[n-2] != "Enviadores Centro" || ins[0].Args[n-1] != "Av. Juárez 10, Centro, CDMX" {
		t.Fatalf("place args %v", ins[0].Args[n-2:])
	}
}

func TestLocationBarePin(t *testing.T) {
	stub := &thumbStub{}
	b, db := locationBridge(stub)
	msg := &waProto.Message{LocationMessage: &waProto.LocationMessage{
		DegreesLatitude: proto.Float64(20.6597), DegreesLongitude: proto.Float64(-103.3496),
	}}
	msgType, row := storedRow(b, "LOC2", msg)
	if msgType != "location" || row.Body.String != "20.659700, -103.349600" {
		t.Fatalf("type %q body %q", msgType, row.Body.String)
	}
	if row.LocationName.Valid || row.LocationAddress.Valid || row.MediaThumbnailURL.Valid || len(stub.shas) != 0 {
		t.Fatalf("a bare pin has no place and no preview: %+v (uploads %v)", row, stub.shas)
	}
	_, _, info := classifyMessage(msg)
	if got := previewFor("location", row.Body.String, info); got != "📍 Ubicación" {
		t.Fatalf("a pin must never preview as raw coordinates: %q", got)
	}
	if err := b.store.InsertMessage(t.Context(), row); err != nil {
		t.Fatal(err)
	}
	if ins := calls(db, "INSERT IGNORE INTO wa_messages"); len(ins) != 1 || strings.Contains(ins[0].Query, "location_name") {
		t.Fatalf("a pin without a place needs no new columns: %+v", ins)
	}
}

func TestLiveLocationIsStoredAsLocation(t *testing.T) {
	stub := &thumbStub{}
	b, _ := locationBridge(stub)
	// Live locations often arrive wrapped (disappearing chats).
	msg := &waProto.Message{EphemeralMessage: &waProto.FutureProofMessage{Message: &waProto.Message{
		LiveLocationMessage: &waProto.LiveLocationMessage{
			DegreesLatitude:  proto.Float64(19.70078),
			DegreesLongitude: proto.Float64(-101.18443),
			Caption:          proto.String("voy en camino"),
			SequenceNumber:   proto.Int64(3),
			JPEGThumbnail:    mapTile,
		},
	}}}
	msgType, row := storedRow(b, "LIVE1", msg)
	if msgType != "location" || row.Body.String != "19.700780, -101.184430" {
		t.Fatalf("type %q body %q", msgType, row.Body.String)
	}
	if row.LocationName.Valid || row.LocationAddress.Valid {
		t.Fatalf("a live location has no place: %+v", row)
	}
	if !row.MediaThumbnailURL.Valid || len(stub.shas) != 1 {
		t.Fatalf("live location map preview not stored: %+v", row.MediaThumbnailURL)
	}
	_, _, info := classifyMessage(msg)
	if got := previewFor("location", row.Body.String, info); got != "📍 Ubicación" {
		t.Fatalf("preview %q", got)
	}
}

func TestLocationPreviewUploadFailureKeepsTheRow(t *testing.T) {
	b, _ := locationBridge(&thumbStub{err: errors.New("sftp down")})
	_, row := storedRow(b, "LOC3", &waProto.Message{LocationMessage: &waProto.LocationMessage{
		DegreesLatitude: proto.Float64(1), DegreesLongitude: proto.Float64(2),
		Name: proto.String("Bodega"), JPEGThumbnail: mapTile,
	}})
	if row.MediaThumbnailURL.Valid || row.LocationName.String != "Bodega" {
		t.Fatalf("failed upload must leave the preview NULL and keep the place: %+v", row)
	}
	// No uploader configured (tests, misconfigured box): no preview, no panic.
	b.uploadThumbnail = nil
	if _, row = storedRow(b, "LOC4", &waProto.Message{LocationMessage: &waProto.LocationMessage{JPEGThumbnail: mapTile}}); row.MediaThumbnailURL.Valid {
		t.Fatal("no uploader must mean no preview")
	}
}

func TestLocationFieldsClipToColumnWidths(t *testing.T) {
	name, address := locationFields(&mediaDescriptor{
		locationName:    strings.Repeat("ñ", 300),
		locationAddress: strings.Repeat("é", 700),
	})
	if utf8.RuneCountInString(name.String) != store.LocationNameMax || !utf8.ValidString(name.String) {
		t.Fatalf("name: %d runes", utf8.RuneCountInString(name.String))
	}
	if utf8.RuneCountInString(address.String) != store.LocationAddressMax || !utf8.ValidString(address.String) {
		t.Fatalf("address: %d runes", utf8.RuneCountInString(address.String))
	}
	if n, a := locationFields(nil); n.Valid || a.Valid {
		t.Fatal("no descriptor, no place")
	}
}

func TestTailPreviewForLocations(t *testing.T) {
	named := store.TailRow{MessageType: "location", Body: nullString("19.4, -99.1"), LocationName: sql.NullString{String: "Sucursal Norte", Valid: true}}
	if got := tailPreview(named); got != "📍 Sucursal Norte" {
		t.Fatalf("named tail %q", got)
	}
	// Rows stored before the place columns existed only have coordinates.
	old := store.TailRow{MessageType: "location", Body: nullString("19.400000, -99.100000")}
	if got := tailPreview(old); got != "📍 Ubicación" {
		t.Fatalf("old location tail %q", got)
	}
}
