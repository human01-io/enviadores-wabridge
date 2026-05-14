// Package media writes incoming WhatsApp media to the shared host over SFTP
// and returns a public URL the React app can fetch.
package media

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"mime"
	"path"
	"strings"
	"sync"

	"github.com/enviadores/wabridge/internal/config"
	"github.com/pkg/sftp"
)

type Uploader struct {
	cfg  *config.Config
	sftp *sftp.Client

	mu        sync.Mutex
	ensuredOK bool
}

func New(cfg *config.Config, s *sftp.Client) *Uploader {
	return &Uploader{cfg: cfg, sftp: s}
}

// Upload stores data on the remote at <remote_path>/<sha256>.<ext> and
// returns its public URL. Idempotent: if a file with the same hash already
// exists, no re-upload happens.
func (u *Uploader) Upload(data []byte, mimeType, filename string) (publicURL, sha string, err error) {
	if err := u.ensureRemoteDir(); err != nil {
		return "", "", err
	}

	sum := sha256.Sum256(data)
	sha = hex.EncodeToString(sum[:])
	ext := pickExtension(mimeType, filename)
	remoteFile := path.Join(u.cfg.Media.RemotePath, sha+ext)

	// Skip upload if it's already there.
	if info, err := u.sftp.Stat(remoteFile); err == nil && info.Size() == int64(len(data)) {
		return u.publicURL(sha + ext), sha, nil
	}

	tmp := remoteFile + ".part"
	f, err := u.sftp.Create(tmp)
	if err != nil {
		return "", "", fmt.Errorf("sftp create %s: %w", tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = u.sftp.Remove(tmp)
		return "", "", fmt.Errorf("sftp write: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = u.sftp.Remove(tmp)
		return "", "", fmt.Errorf("sftp close: %w", err)
	}
	if err := u.sftp.Rename(tmp, remoteFile); err != nil {
		// File may have been written by a concurrent run.
		_ = u.sftp.Remove(tmp)
	}
	return u.publicURL(sha + ext), sha, nil
}

// UploadThumbnail stores a small JPEG preview alongside an existing
// media file, returning its public URL. The naming convention
// "<sha>_thumb.jpg" keeps the thumbnail co-located with the original
// (so the wa_media retention sweep on the host treats them as one unit)
// and avoids hashing the thumbnail bytes — different JPEG encoders
// produce different bytes for visually identical thumbnails, so hashing
// would just suppress dedup of the main file.
func (u *Uploader) UploadThumbnail(originalSHA string, jpegBytes []byte) (publicURL string, err error) {
	if originalSHA == "" {
		return "", fmt.Errorf("upload thumbnail: empty sha")
	}
	if len(jpegBytes) == 0 {
		return "", fmt.Errorf("upload thumbnail: empty bytes")
	}
	if err := u.ensureRemoteDir(); err != nil {
		return "", err
	}
	basename := originalSHA + "_thumb.jpg"
	remoteFile := path.Join(u.cfg.Media.RemotePath, basename)

	if info, err := u.sftp.Stat(remoteFile); err == nil && info.Size() == int64(len(jpegBytes)) {
		return u.publicURL(basename), nil
	}

	tmp := remoteFile + ".part"
	f, err := u.sftp.Create(tmp)
	if err != nil {
		return "", fmt.Errorf("sftp create thumb %s: %w", tmp, err)
	}
	if _, err := f.Write(jpegBytes); err != nil {
		_ = f.Close()
		_ = u.sftp.Remove(tmp)
		return "", fmt.Errorf("sftp write thumb: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = u.sftp.Remove(tmp)
		return "", fmt.Errorf("sftp close thumb: %w", err)
	}
	if err := u.sftp.Rename(tmp, remoteFile); err != nil {
		_ = u.sftp.Remove(tmp)
	}
	return u.publicURL(basename), nil
}

// Read returns the bytes of a file previously stored in remote_path,
// identified by its basename (e.g. "<sha256>.<ext>"). Used by the
// outbound sender to load files the PHP gateway dropped into wa_media
// before re-encrypting them for WhatsApp's CDN. Returns the original
// plaintext bytes — the file on disk is stored unencrypted.
func (u *Uploader) Read(basename string) ([]byte, error) {
	if basename == "" {
		return nil, fmt.Errorf("read media: empty basename")
	}
	remoteFile := path.Join(u.cfg.Media.RemotePath, basename)
	f, err := u.sftp.Open(remoteFile)
	if err != nil {
		return nil, fmt.Errorf("sftp open %s: %w", remoteFile, err)
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("sftp read %s: %w", remoteFile, err)
	}
	return data, nil
}

// BasenameFromPublicURL returns the trailing file segment of a URL that
// was previously produced by Upload or by the PHP gateway — both share
// the configured PublicBaseURL prefix. Empty string if the URL doesn't
// belong to this uploader's public base.
func (u *Uploader) BasenameFromPublicURL(publicURL string) string {
	prefix := strings.TrimRight(u.cfg.Media.PublicBaseURL, "/") + "/"
	if !strings.HasPrefix(publicURL, prefix) {
		return ""
	}
	return strings.TrimPrefix(publicURL, prefix)
}

func (u *Uploader) ensureRemoteDir() error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.ensuredOK {
		return nil
	}
	if err := u.sftp.MkdirAll(u.cfg.Media.RemotePath); err != nil {
		return fmt.Errorf("mkdir remote media path: %w", err)
	}
	u.ensuredOK = true
	return nil
}

func (u *Uploader) publicURL(basename string) string {
	base := strings.TrimRight(u.cfg.Media.PublicBaseURL, "/")
	return base + "/" + basename
}

func pickExtension(mimeType, filename string) string {
	if filename != "" {
		if ext := path.Ext(filename); ext != "" {
			return strings.ToLower(ext)
		}
	}
	if mimeType != "" {
		// Prefer canonical extensions before consulting the system mime DB.
		// On Debian-based hosts mime.ExtensionsByType("image/jpeg") returns
		// [".jfif", ".jpe", ".jpeg", ".jpg"] — and Apache's default mime.types
		// has no mapping for .jfif, so the served file goes out with no
		// Content-Type and browsers refuse to render it.
		switch {
		case strings.Contains(mimeType, "jpeg"):
			return ".jpg"
		case strings.Contains(mimeType, "png"):
			return ".png"
		case strings.Contains(mimeType, "webp"):
			return ".webp"
		case strings.Contains(mimeType, "gif"):
			return ".gif"
		case strings.Contains(mimeType, "pdf"):
			return ".pdf"
		case strings.Contains(mimeType, "mp4"):
			return ".mp4"
		case strings.Contains(mimeType, "ogg"):
			return ".ogg"
		}
		exts, _ := mime.ExtensionsByType(mimeType)
		if len(exts) > 0 {
			return strings.ToLower(exts[0])
		}
	}
	return ".bin"
}
