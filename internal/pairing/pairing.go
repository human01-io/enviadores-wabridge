// Package pairing writes wabridge's pairing state into the wa_pairing
// MySQL row so the React app can show a QR-code pairing UI without needing
// terminal access to the bridge host.
package pairing

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

type Status string

const (
	StatusIdle          Status = "idle"
	StatusAwaitingScan  Status = "awaiting_scan"
	StatusPaired        Status = "paired"
	StatusError         Status = "error"
)

type State struct {
	Status            Status
	QRCode            string
	QRExpiresAt       time.Time
	DeviceJID         string
	LastEvent         string
	ResetRequestedAt  time.Time
	PairedAt          time.Time
}

type Writer struct {
	db *sql.DB
}

func New(db *sql.DB) *Writer { return &Writer{db: db} }

// SetQRCode records that whatsmeow emitted a new QR code; whatsmeow rotates
// these every ~20s, so we set an expiry to help the UI grey out stale codes.
func (w *Writer) SetQRCode(ctx context.Context, code string, validFor time.Duration) error {
	expires := time.Now().Add(validFor).UTC()
	_, err := w.db.ExecContext(ctx, `
		UPDATE wa_pairing
		SET status = 'awaiting_scan',
		    qr_code = ?,
		    qr_expires_at = ?,
		    last_event = 'qr emitted'
		WHERE id = 1
	`, code, expires)
	return err
}

// SetPaired records a successful pairing.
func (w *Writer) SetPaired(ctx context.Context, deviceJID string) error {
	_, err := w.db.ExecContext(ctx, `
		UPDATE wa_pairing
		SET status = 'paired',
		    qr_code = NULL,
		    qr_expires_at = NULL,
		    device_jid = ?,
		    paired_at = NOW(),
		    last_event = 'paired'
		WHERE id = 1
	`, deviceJID)
	return err
}

// SetEvent records a status transition for diagnostics.
func (w *Writer) SetEvent(ctx context.Context, status Status, msg string) error {
	_, err := w.db.ExecContext(ctx, `
		UPDATE wa_pairing
		SET status = ?,
		    last_event = ?
		WHERE id = 1
	`, string(status), msg)
	return err
}

// ConsumeResetRequest atomically clears reset_requested_at and returns true
// when a reset was requested by the web app since we last looked. The bridge
// should call this in its supervise loop and, when it returns true, log out
// the current session and restart the QR-pair flow.
func (w *Writer) ConsumeResetRequest(ctx context.Context) (bool, error) {
	tx, err := w.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	var ts sql.NullTime
	if err := tx.QueryRowContext(ctx,
		`SELECT reset_requested_at FROM wa_pairing WHERE id = 1 FOR UPDATE`,
	).Scan(&ts); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	if !ts.Valid {
		return false, tx.Commit()
	}

	if _, err := tx.ExecContext(ctx,
		`UPDATE wa_pairing SET reset_requested_at = NULL, last_event = 'reset consumed' WHERE id = 1`,
	); err != nil {
		return false, err
	}
	return true, tx.Commit()
}
