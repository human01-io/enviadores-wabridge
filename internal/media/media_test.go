package media

import (
	"testing"

	"github.com/enviadores/wabridge/internal/config"
)

func uploaderWithBase(base string) *Uploader {
	cfg := &config.Config{}
	cfg.Media.PublicBaseURL = base
	return &Uploader{cfg: cfg}
}

const sha = "9ccfd0e5cff61324f7ad72438d19c9d1300000000000000000000000000000000"

func TestBasenameFromPublicURLIgnoresHost(t *testing.T) {
	// public_base_url still says apex; the PHP gateway writes api.* since
	// 2026-06-25. Both must resolve, or outbound attachments fail.
	u := uploaderWithBase("https://enviadores.com.mx/wa_media")

	cases := []struct{ name, url, want string }{
		{"apex (bridge-written)", "https://enviadores.com.mx/wa_media/" + sha + ".pdf", sha + ".pdf"},
		{"api host (gateway-written)", "https://api.enviadores.com.mx/wa_media/" + sha + ".pdf", sha + ".pdf"},
		{"trailing slash in base", "https://api.enviadores.com.mx/wa_media/" + sha + ".jpg", sha + ".jpg"},
		{"thumbnail", "https://api.enviadores.com.mx/wa_media/" + sha + "_thumb.jpg", sha + "_thumb.jpg"},
		{"percent-encoded dot", "https://api.enviadores.com.mx/wa_media/" + sha + "%2Epdf", sha + ".pdf"},
		{"different path", "https://api.enviadores.com.mx/other/" + sha + ".pdf", ""},
		{"no path", "https://api.enviadores.com.mx/", ""},
		{"nested path segment", "https://api.enviadores.com.mx/wa_media/sub/" + sha + ".pdf", ""},
		{"traversal", "https://api.enviadores.com.mx/wa_media/..%2F..%2Fetc%2Fpasswd", ""},
		{"empty", "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := u.BasenameFromPublicURL(c.url); got != c.want {
				t.Fatalf("BasenameFromPublicURL(%q) = %q, want %q", c.url, got, c.want)
			}
		})
	}
}

func TestBasenameFromPublicURLBaseForms(t *testing.T) {
	want := sha + ".pdf"
	for _, base := range []string{
		"https://enviadores.com.mx/wa_media",
		"https://enviadores.com.mx/wa_media/",
		"https://api.enviadores.com.mx/wa_media",
		"/wa_media",
	} {
		u := uploaderWithBase(base)
		if got := u.BasenameFromPublicURL("https://api.enviadores.com.mx/wa_media/" + want); got != want {
			t.Fatalf("base %q: got %q, want %q", base, got, want)
		}
	}
}
