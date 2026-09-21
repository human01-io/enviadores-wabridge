// Package logfile captures everything wabridge prints (std log, whatsmeow's
// stdout loggers, panics on stderr) into a size-capped file next to the
// binary. Under the Windows service manager stdout goes nowhere, so without
// this the reason for a dropped WhatsApp session is lost.
package logfile

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

const (
	FileName = "wabridge.log"
	maxSize  = 10 << 20 // rotate to wabridge.log.1 past 10 MB
)

// Setup redirects os.Stdout, os.Stderr and the std logger into path. When
// tee is true (foreground `run`), output is still echoed to the original
// stdout. The returned func flushes and closes the file.
func Setup(path string, tee bool) (func(), error) {
	rf, err := openRotating(path)
	if err != nil {
		return nil, err
	}
	var dst io.Writer = rf
	if tee {
		dst = io.MultiWriter(os.Stdout, rf)
	}

	r, w, err := os.Pipe()
	if err != nil {
		rf.Close()
		return nil, err
	}
	os.Stdout, os.Stderr = w, w
	log.SetOutput(w)

	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(dst, r)
		close(done)
	}()

	var once sync.Once
	return func() {
		once.Do(func() {
			w.Close()
			<-done
			rf.Close()
		})
	}, nil
}

// rotatingFile prefixes each line with a full date (whatsmeow's loggers only
// print the time of day) and rotates once the file passes maxSize. Writes
// arrive from the single io.Copy goroutine, so no locking is needed.
type rotatingFile struct {
	path        string
	f           *os.File
	size        int64
	atLineStart bool
}

func openRotating(path string) (*rotatingFile, error) {
	rf := &rotatingFile{path: path, atLineStart: true}
	if err := rf.open(); err != nil {
		return nil, err
	}
	return rf, nil
}

func (rf *rotatingFile) open() error {
	f, err := os.OpenFile(rf.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open log file: %w", err)
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	rf.f, rf.size = f, fi.Size()
	return nil
}

func (rf *rotatingFile) rotate() {
	rf.f.Close()
	_ = os.Remove(rf.path + ".1")
	_ = os.Rename(rf.path, rf.path+".1")
	if err := rf.open(); err != nil {
		// Can't reopen — fall back to discarding rather than crashing the bridge.
		rf.f, _ = os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	}
}

func (rf *rotatingFile) Write(p []byte) (int, error) {
	if rf.size >= maxSize && rf.atLineStart {
		rf.rotate()
	}
	stamp := []byte(time.Now().Format("2006-01-02 "))
	written := 0
	for len(p) > 0 {
		if rf.atLineStart {
			n, _ := rf.f.Write(stamp)
			rf.size += int64(n)
			rf.atLineStart = false
		}
		line := p
		for i, c := range p {
			if c == '\n' {
				line = p[:i+1]
				rf.atLineStart = true
				break
			}
		}
		n, err := rf.f.Write(line)
		rf.size += int64(n)
		written += n
		if err != nil {
			return written, err
		}
		p = p[len(line):]
	}
	return written, nil
}

func (rf *rotatingFile) Close() error { return rf.f.Close() }
