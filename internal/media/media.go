// Package media writes incoming WhatsApp media to the shared host over SFTP
// and returns a public URL the React app can fetch.
package media

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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
