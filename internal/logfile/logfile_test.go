package logfile

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestRotatingFileStampsLinesAndRotates(t *testing.T) {
	path := filepath.Join(t.TempDir(), FileName)
	rf, err := openRotating(path)
	if err != nil {
		t.Fatal(err)
	}
	// A line split across writes gets one date stamp.
	rf.Write([]byte("12:00:00 [wa-client ERROR] Logged out"))
	rf.Write([]byte(": 401\nsecond line\n"))

	b, _ := os.ReadFile(path)
	lines := strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
	stamp := regexp.MustCompile(`^\d{4}-\d{2}-\d{2} `)
	if len(lines) != 2 {
		t.Fatalf("want 2 lines, got %q", b)
	}
	for _, l := range lines {
		if !stamp.MatchString(l) {
			t.Fatalf("line not date-stamped: %q", l)
		}
	}
	if !strings.HasSuffix(lines[0], "Logged out: 401") {
		t.Fatalf("split write mangled: %q", lines[0])
	}

	rf.size = maxSize
	rf.Write([]byte("after rotation\n"))
	rf.Close()
	if old, _ := os.ReadFile(path + ".1"); !strings.Contains(string(old), "second line") {
		t.Fatalf("rotated file missing old content: %q", old)
	}
	if cur, _ := os.ReadFile(path); !strings.Contains(string(cur), "after rotation") || strings.Contains(string(cur), "second line") {
		t.Fatalf("current file wrong after rotation: %q", cur)
	}
}
