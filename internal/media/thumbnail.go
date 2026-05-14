package media

import (
	"bytes"
	"errors"
	"fmt"
	"image"
	"image/jpeg"

	// Registers PNG / GIF decoders with the image package so Decode
	// accepts those formats from inbound bytes.
	_ "image/gif"
	_ "image/png"

	xdraw "golang.org/x/image/draw"
)

// thumbnailMaxEdge is the longest-edge cap WhatsApp uses for the inline
// JPEG thumbnail. Anything noticeably larger inflates the message proto
// (these are unencrypted and travel inline with every message) without
// improving the rendered preview.
const thumbnailMaxEdge = 200

// thumbnailQuality balances bytes-on-the-wire against preview crispness.
// The thumbnail is decoded and blown up by the receiving client, so very
// low quality is visible — 60 is the WhatsApp Web default ballpark.
const thumbnailQuality = 60

// MakeImageThumbnail decodes the original image bytes, returns a small
// JPEG thumbnail suitable for ImageMessage.JPEGThumbnail, plus the
// dimensions of the *original* image (which clients use to lay out the
// preview before the full file finishes downloading).
//
// Returns an error only on decode failure. The caller is expected to
// treat any error as "skip the thumbnail" and still send the message;
// WhatsApp will accept an ImageMessage with no JPEGThumbnail (the
// recipient just sees a generic placeholder until they download it).
func MakeImageThumbnail(data []byte) (thumb []byte, width, height int, err error) {
	src, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, 0, 0, fmt.Errorf("decode image: %w", err)
	}
	bounds := src.Bounds()
	origW, origH := bounds.Dx(), bounds.Dy()
	if origW <= 0 || origH <= 0 {
		return nil, 0, 0, errors.New("image has zero dimension")
	}

	// Aspect-preserving downscale to fit within thumbnailMaxEdge.
	tw, th := origW, origH
	if origW > thumbnailMaxEdge || origH > thumbnailMaxEdge {
		if origW >= origH {
			tw = thumbnailMaxEdge
			th = origH * thumbnailMaxEdge / origW
			if th < 1 {
				th = 1
			}
		} else {
			th = thumbnailMaxEdge
			tw = origW * thumbnailMaxEdge / origH
			if tw < 1 {
				tw = 1
			}
		}
	}

	dst := image.NewRGBA(image.Rect(0, 0, tw, th))
	// BiLinear is a decent quality/speed compromise at this size —
	// CatmullRom is sharper but costs noticeably more CPU per send.
	xdraw.BiLinear.Scale(dst, dst.Bounds(), src, bounds, xdraw.Over, nil)

	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, dst, &jpeg.Options{Quality: thumbnailQuality}); err != nil {
		return nil, 0, 0, fmt.Errorf("encode jpeg thumbnail: %w", err)
	}
	return buf.Bytes(), origW, origH, nil
}
