package wabridge

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/enviadores/wabridge/internal/store"

	waProto "go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
)

// Branch location sends.
//
// Staff send the office pin from the web inbox. The web app
// (api/v2/whatsapp/send-location.php) reads the branch from pdv_locations,
// which the bridge has no grant on, and queues a wa_outbound row with
// kind='location', body "lat, lng" (6 decimals) and the place in
// location_name / location_address / location_url. The bridge sends it as a
// WhatsApp LocationMessage, so the customer gets a native pin. There is no
// JPEGThumbnail: we have no map image source, and the recipient's app draws
// the map itself. The web app queues these rows only while the heartbeat
// announces "location_send" (store.BridgeFeatures); an older bridge fails
// them with UNSUPPORTED_KIND.

// codeInvalidLocation: a kind='location' row whose body is not a valid
// "lat, lng" pair. Permanent: the row will never become sendable.
const codeInvalidLocation = "INVALID_LOCATION"

// coordinatesRE is the body the web app writes (sprintf('%.6f, %.6f')),
// read leniently on spacing and precision but nothing else: no exponents,
// hex floats, NaN or Inf.
var coordinatesRE = regexp.MustCompile(`^\s*(-?[0-9]{1,3}(?:\.[0-9]+)?)\s*,\s*(-?[0-9]{1,3}(?:\.[0-9]+)?)\s*$`)

// parseCoordinates reads "lat, lng" and checks both are on the globe.
func parseCoordinates(body string) (lat, lng float64, ok bool) {
	m := coordinatesRE.FindStringSubmatch(body)
	if m == nil {
		return 0, 0, false
	}
	lat, err1 := strconv.ParseFloat(m[1], 64)
	lng, err2 := strconv.ParseFloat(m[2], 64)
	if err1 != nil || err2 != nil || lat < -90 || lat > 90 || lng < -180 || lng > 180 {
		return 0, 0, false
	}
	return lat, lng, true
}

// outboundLocation is a parsed kind='location' row: what goes into the
// LocationMessage and into the wa_messages row.
type outboundLocation struct {
	lat, lng float64
	name     string // clipped to wa_messages.location_name
	address  string // clipped to wa_messages.location_address
	url      string
}

// buildLocationMessage turns a kind='location' row into a LocationMessage.
// A malformed body fails the row permanently (INVALID_LOCATION); a database
// error reading the place is transient and returned for the usual backoff.
func (b *Bridge) buildLocationMessage(ctx context.Context, r outboundRow) (*waProto.Message, *outboundLocation, error) {
	lat, lng, ok := parseCoordinates(r.Body)
	if !ok {
		b.markOutboundPermanentlyFailed(ctx, r.ID, codeInvalidLocation, `body must be "lat, lng", got `+quoteForLog(r.Body))
		return nil, nil, fmt.Errorf("invalid location body: %w", errOutboundPermanent)
	}
	place, err := b.store.OutboundLocation(ctx, r.ID)
	if err != nil {
		return nil, nil, fmt.Errorf("read location: %w", err)
	}
	loc := &outboundLocation{
		lat:     lat,
		lng:     lng,
		name:    truncateRunes(strings.TrimSpace(place.Name.String), store.LocationNameMax),
		address: truncateRunes(strings.TrimSpace(place.Address.String), store.LocationAddressMax),
		url:     strings.TrimSpace(place.URL.String),
	}
	return &waProto.Message{LocationMessage: &waProto.LocationMessage{
		DegreesLatitude:  proto.Float64(lat),
		DegreesLongitude: proto.Float64(lng),
		Name:             protoStringPtrOrNil(loc.name),
		Address:          protoStringPtrOrNil(loc.address),
		URL:              protoStringPtrOrNil(loc.url),
	}}, loc, nil
}
