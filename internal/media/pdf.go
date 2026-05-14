package media

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// PDFPreview describes what we managed to extract from a PDF for the
// DocumentMessage proto. Any field can be zero — callers should treat
// the struct as additive hints to the message they're already building.
type PDFPreview struct {
	JPEGThumbnail   []byte
	ThumbnailWidth  int
	ThumbnailHeight int
	PageCount       int
	Title           string
}

// pdfRenderTimeout caps how long we'll wait for poppler to render and
// inspect a PDF. Large documents (multi-hundred page PDFs) can take a
// couple of seconds; capping at 8s keeps a stuck render from blocking
// the outbound queue while leaving room for normal PDFs.
const pdfRenderTimeout = 8 * time.Second

// ExtractPDFPreview produces a chat-card preview for a PDF: a first-page
// JPEG thumbnail plus page count and document title. It shells out to
// poppler-utils (pdftoppm + pdfinfo); if neither tool is on PATH the
// caller still gets a usable PDFPreview (just zero-valued fields), and
// outbound sends proceed without a thumbnail rather than failing.
//
// pdftoppm install:
//   - macOS:   brew install poppler
//   - Debian:  apt install poppler-utils
//   - Windows: download the poppler release zip and put bin/ on PATH
func ExtractPDFPreview(ctx context.Context, data []byte) (PDFPreview, error) {
	var out PDFPreview

	tmpDir, err := os.MkdirTemp("", "wabridge-pdf-")
	if err != nil {
		return out, fmt.Errorf("mkdir temp: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	pdfPath := filepath.Join(tmpDir, "in.pdf")
	if err := os.WriteFile(pdfPath, data, 0o600); err != nil {
		return out, fmt.Errorf("write temp pdf: %w", err)
	}

	// Hard ceiling for both subprocesses combined.
	ctx, cancel := context.WithTimeout(ctx, pdfRenderTimeout)
	defer cancel()

	// Page count + title via pdfinfo. Cheap (parses metadata only).
	if pages, title, err := pdfInfo(ctx, pdfPath); err == nil {
		out.PageCount = pages
		out.Title = title
	}

	// First-page JPEG via pdftoppm. 60 DPI ≈ 500px wide for letter — small
	// enough that the resize after won't waste much CPU but big enough
	// that the resized thumbnail is still legible.
	thumb, w, h, err := pdfFirstPageThumb(ctx, pdfPath, tmpDir)
	if err != nil {
		// Not having poppler installed is the expected case on a fresh
		// Windows box — surface it via the returned error so the caller
		// can log once, but don't drop the page count we already have.
		return out, err
	}
	out.JPEGThumbnail = thumb
	out.ThumbnailWidth = w
	out.ThumbnailHeight = h
	return out, nil
}

func pdfInfo(ctx context.Context, pdfPath string) (pages int, title string, err error) {
	cmd := exec.CommandContext(ctx, "pdfinfo", pdfPath)
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		return 0, "", fmt.Errorf("pdfinfo: %w", err)
	}
	scanner := bufio.NewScanner(&stdout)
	for scanner.Scan() {
		line := scanner.Text()
		if v, ok := pdfInfoField(line, "Pages:"); ok {
			if n, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
				pages = n
			}
		} else if v, ok := pdfInfoField(line, "Title:"); ok {
			title = strings.TrimSpace(v)
		}
	}
	return pages, title, nil
}

func pdfInfoField(line, prefix string) (string, bool) {
	if !strings.HasPrefix(line, prefix) {
		return "", false
	}
	return strings.TrimPrefix(line, prefix), true
}

func pdfFirstPageThumb(ctx context.Context, pdfPath, tmpDir string) (jpegBytes []byte, width, height int, err error) {
	outPrefix := filepath.Join(tmpDir, "page")
	cmd := exec.CommandContext(ctx, "pdftoppm",
		"-jpeg",
		"-jpegopt", "quality=70",
		"-r", "60",
		"-f", "1", "-l", "1",
		pdfPath, outPrefix,
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, 0, 0, fmt.Errorf("pdftoppm: %w (%s)", err, strings.TrimSpace(stderr.String()))
	}

	// pdftoppm picks the suffix based on page count: a single page is
	// usually "page-1.jpg" but very large documents zero-pad. Glob to
	// cover both.
	matches, _ := filepath.Glob(outPrefix + "*.jpg")
	if len(matches) == 0 {
		return nil, 0, 0, errors.New("pdftoppm produced no output")
	}
	raw, err := os.ReadFile(matches[0])
	if err != nil {
		return nil, 0, 0, fmt.Errorf("read pdftoppm output: %w", err)
	}
	// Reuse the image-thumbnail path to downscale to the inline-thumb
	// cap and re-encode at our standard quality.
	return MakeImageThumbnail(raw)
}
