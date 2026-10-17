package wabridge

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"go.mau.fi/whatsmeow/types"
)

// check-number: ask WhatsApp whether a phone has an account, without sending
// anything and without writing to MySQL.
//
// The lookup runs inside the RUNNING bridge. A second process connecting
// with the same whatsmeow session would replace the service's stream
// (events.StreamReplaced: whatsmeow does not reconnect, the watchdog only
// restarts after 5 min) and would consume any message that arrives while it
// is connected, which the inbox would then never see. So the CLI drops a
// request file next to config.yaml, the bridge answers with a result file,
// and the CLI prints it.

const (
	checkRequestFile = "check-number.req"
	checkResultFile  = "check-number.res"
)

// CheckNumberRequest is the request file's JSON.
type CheckNumberRequest struct {
	ID    string `json:"id"`
	Phone string `json:"phone"`
}

// CheckNumberResult is the result file's JSON, printed by the CLI.
type CheckNumberResult struct {
	ID           string `json:"id"`
	Phone        string `json:"phone"`
	Error        string `json:"error,omitempty"`
	IsIn         bool   `json:"is_in"`
	Query        string `json:"query,omitempty"`
	JID          string `json:"jid,omitempty"`
	PhoneNumber  string `json:"phone_number,omitempty"`
	VerifiedName string `json:"verified_name,omitempty"`
	// StoreJID is the wa_chats key a new chat to this number would use.
	StoreJID string `json:"store_jid,omitempty"`
}

var checkPhoneRE = regexp.MustCompile(`^\+[0-9]{8,15}$`)

// NormalizeCheckPhone accepts "+52...", "52..." or with spaces/dashes and
// returns the "+<digits>" form IsOnWhatsApp takes.
func NormalizeCheckPhone(raw string) (string, error) {
	s := strings.NewReplacer(" ", "", "-", "", "(", "", ")", "").Replace(strings.TrimSpace(raw))
	if !strings.HasPrefix(s, "+") {
		s = "+" + s
	}
	if !checkPhoneRE.MatchString(s) {
		return "", fmt.Errorf("phone %q: want international form, e.g. +525512345678 or +5215512345678", raw)
	}
	return s, nil
}

// WatchCheckRequests answers check-number request files dropped in dir
// until ctx ends. It never returns an error of its own.
func (b *Bridge) WatchCheckRequests(ctx context.Context, dir string) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			b.answerCheckRequest(ctx, dir)
		}
	}
}

func (b *Bridge) answerCheckRequest(ctx context.Context, dir string) {
	reqPath := filepath.Join(dir, checkRequestFile)
	data, err := os.ReadFile(reqPath)
	if err != nil {
		return // no request (the normal case)
	}
	_ = os.Remove(reqPath)
	var req CheckNumberRequest
	res := CheckNumberResult{}
	if err := json.Unmarshal(data, &req); err != nil {
		res.Error = "bad request file: " + err.Error()
	} else {
		res = b.checkNumber(ctx, req)
	}
	if err := writeFileAtomic(filepath.Join(dir, checkResultFile), res); err != nil {
		b.logger.Warnf("check-number: write result: %v", err)
	}
}

// checkNumber runs one lookup. It writes nothing to MySQL.
func (b *Bridge) checkNumber(ctx context.Context, req CheckNumberRequest) CheckNumberResult {
	res := CheckNumberResult{ID: req.ID, Phone: req.Phone}
	phone, err := NormalizeCheckPhone(req.Phone)
	if err != nil {
		res.Error = err.Error()
		return res
	}
	if b.client != nil && !b.client.IsConnected() {
		res.Error = "bridge is not connected to WhatsApp"
		return res
	}
	lookupCtx, cancel := context.WithTimeout(ctx, newChatCallTimeout)
	out, err := b.isOnWhatsApp(lookupCtx, []string{phone})
	cancel()
	if err != nil && len(out) == 0 {
		res.Error = "lookup: " + err.Error()
		return res
	}
	if err != nil {
		res.Error = "lookup warning: " + err.Error()
	}
	if len(out) == 0 {
		return res
	}
	hit := out[0]
	res.IsIn = hit.IsIn
	res.Query = hit.Query
	if !hit.JID.IsEmpty() {
		res.JID = hit.JID.String()
	}
	if !hit.PhoneNumber.IsEmpty() {
		res.PhoneNumber = hit.PhoneNumber.String()
	}
	if hit.VerifiedName != nil && hit.VerifiedName.Details != nil {
		res.VerifiedName = hit.VerifiedName.Details.GetVerifiedName()
	}
	if hit.IsIn && hit.JID.User != "" {
		t := hit.JID
		if t.Server == types.DefaultUserServer {
			t = b.canonicalChatJID(ctx, t)
		}
		res.StoreJID = t.String()
	}
	b.logger.Infof("check-number %s: is_in=%v jid=%s store_jid=%s", phone, res.IsIn, res.JID, res.StoreJID)
	return res
}

// RequestCheckNumber is the CLI side: it drops a request for the running
// bridge in dir and waits up to timeout for the answer.
func RequestCheckNumber(dir, rawPhone string, timeout time.Duration) (CheckNumberResult, error) {
	phone, err := NormalizeCheckPhone(rawPhone)
	if err != nil {
		return CheckNumberResult{}, err
	}
	var nonce [8]byte
	_, _ = rand.Read(nonce[:])
	req := CheckNumberRequest{ID: hex.EncodeToString(nonce[:]), Phone: phone}
	resPath := filepath.Join(dir, checkResultFile)
	_ = os.Remove(resPath)
	if err := writeFileAtomic(filepath.Join(dir, checkRequestFile), req); err != nil {
		return CheckNumberResult{}, err
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
		data, err := os.ReadFile(resPath)
		if err != nil {
			continue
		}
		var res CheckNumberResult
		if json.Unmarshal(data, &res) != nil || res.ID != req.ID {
			continue
		}
		_ = os.Remove(resPath)
		return res, nil
	}
	_ = os.Remove(filepath.Join(dir, checkRequestFile))
	return CheckNumberResult{}, errors.New("no answer from the bridge: it must be running (service or `wabridge run`) and connected")
}

// writeFileAtomic writes v as JSON through a temp file + rename, so the
// reader never sees half a file.
func writeFileAtomic(path string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
