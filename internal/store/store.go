// Package store wraps writes to the wa_chats / wa_messages tables in
// env_producto. All queries go through the SSH tunnel.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/enviadores/wabridge/internal/config"
	"github.com/go-sql-driver/mysql"
)

type Store struct {
	db *sql.DB

	// logf receives diagnostics (ASCII only: the Windows box). Nil discards.
	logf func(format string, args ...any)

	// off holds the names of features whose newer columns the database does
	// not have yet (see Fallback). Guarded by mu.
	mu  sync.Mutex
	off map[string]bool
}

// NewFromDB wraps an existing connection. Open uses it; tests use it with a
// fake driver.
func NewFromDB(db *sql.DB) *Store { return &Store{db: db} }

// SetLogger installs the diagnostics sink (ASCII-only messages).
func (s *Store) SetLogger(logf func(format string, args ...any)) { s.logf = logf }

func (s *Store) log(format string, args ...any) {
	if s.logf != nil {
		s.logf(format, args...)
	}
}

// mysqlErrBadField is MySQL error 1054 (ER_BAD_FIELD_ERROR): "Unknown column".
const mysqlErrBadField = 1054

// IsUnknownColumn reports whether err is MySQL's "Unknown column" error, which
// is what a statement touching a column of a not-yet-applied migration raises.
func IsUnknownColumn(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == mysqlErrBadField
}

// FeatureOff reports whether Fallback already switched the feature to its
// legacy statement.
func (s *Store) FeatureOff(feature string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.off[feature]
}

// Fallback runs newFn (a statement that uses columns from a newer migration).
// If the database answers "unknown column", the feature is switched off for
// the life of the process, one line is logged, and oldFn (the statement the
// previous release ran) runs instead, now and on every later call. oldFn may
// be nil when there is no legacy equivalent; then the call is skipped.
//
// The migration is applied before a release ships, so this is only a safety
// net for a bridge that is started against an older schema.
func (s *Store) Fallback(feature string, newFn, oldFn func() error) error {
	if !s.FeatureOff(feature) {
		err := newFn()
		if err == nil || !IsUnknownColumn(err) {
			return err
		}
		s.mu.Lock()
		first := !s.off[feature]
		if s.off == nil {
			s.off = map[string]bool{}
		}
		s.off[feature] = true
		s.mu.Unlock()
		if first {
			s.log("schema is missing columns for %q (%v) - using the legacy statement until restart", feature, err)
		}
	}
	if oldFn == nil {
		return nil
	}
	return oldFn()
}

func Open(ctx context.Context, cfg *config.Config, tunnelAddr string) (*Store, error) {
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s)/%s?parseTime=true&loc=UTC&charset=utf8mb4,utf8&collation=utf8mb4_general_ci",
		cfg.MySQL.User,
		url.QueryEscape(cfg.MySQL.Password),
		tunnelAddr,
		cfg.MySQL.Database,
	)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("sql.Open: %w", err)
	}
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)

	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	return NewFromDB(db), nil
}

func (s *Store) Close() error { return s.db.Close() }

// DB returns the underlying connection. Use sparingly — most writes should
// go through methods on Store. Currently used by internal/pairing for the
// wa_pairing single-row state bus.
func (s *Store) DB() *sql.DB { return s.db }

type Chat struct {
	JID                string
	DisplayName        sql.NullString
	PhoneE164          sql.NullString
	IsGroup            bool
	ProfilePicURL      sql.NullString
	LastMessageAt      sql.NullTime
	LastMessagePreview sql.NullString
	LastMessageFromMe  bool
	IncrementUnread    bool // when a new inbound message arrives
}

// UpsertChat inserts or updates the wa_chats row for this conversation.
// last_message_* fields are always overwritten when LastMessageAt is non-NULL,
// and unread_count is incremented when IncrementUnread is true (resetting
// to the message's from_me flag, i.e. an outbound message clears it).
//
// profile_pic_updated_at is stamped to NOW() whenever we actually wrote a
// new URL — needed so the periodic refresher can find stale entries.
func (s *Store) UpsertChat(ctx context.Context, c Chat) error {
	const q = `
		INSERT INTO wa_chats (
			jid, display_name, phone_e164, is_group, profile_pic_url,
			profile_pic_updated_at,
			last_message_at, last_message_preview, last_message_from_me,
			unread_count
		)
		VALUES (?, ?, ?, ?, ?,
		        CASE WHEN ? IS NOT NULL THEN UTC_TIMESTAMP() ELSE NULL END,
		        ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			display_name          = COALESCE(VALUES(display_name), display_name),
			phone_e164            = COALESCE(VALUES(phone_e164), phone_e164),
			profile_pic_url       = COALESCE(VALUES(profile_pic_url), profile_pic_url),
			profile_pic_updated_at = CASE
				WHEN VALUES(profile_pic_url) IS NOT NULL THEN UTC_TIMESTAMP()
				ELSE profile_pic_updated_at
			END,
			last_message_at       = COALESCE(VALUES(last_message_at), last_message_at),
			last_message_preview  = COALESCE(VALUES(last_message_preview), last_message_preview),
			last_message_from_me  = VALUES(last_message_from_me),
			unread_count          = CASE
				WHEN VALUES(last_message_from_me) = 1 THEN 0
				WHEN ? = 1 THEN unread_count + 1
				ELSE unread_count
			END
	`
	initialUnread := 0
	if c.IncrementUnread && !c.LastMessageFromMe {
		initialUnread = 1
	}
	_, err := s.db.ExecContext(ctx, q,
		c.JID, c.DisplayName, c.PhoneE164, boolInt(c.IsGroup), c.ProfilePicURL,
		c.ProfilePicURL, // marker for the insert-time stamp
		c.LastMessageAt, c.LastMessagePreview, boolInt(c.LastMessageFromMe),
		initialUnread,
		boolInt(c.IncrementUnread),
	)
	return err
}

// SetPeerTyping stamps (or clears) wa_chats.peer_typing_until. Pass a
// future time when the contact starts composing, nil to clear (paused
// or any non-composing state).
func (s *Store) SetPeerTyping(ctx context.Context, jid string, until *time.Time) error {
	var v sql.NullTime
	if until != nil {
		v = sql.NullTime{Time: *until, Valid: true}
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE wa_chats SET peer_typing_until = ? WHERE jid = ?
	`, v, jid)
	return err
}

// ClearAllPeerTyping wipes the column across every chat. Used on
// (re)connect — peer typing state from before a disconnect is no
// longer meaningful and would otherwise stick around until its TTL
// naturally expires.
func (s *Store) ClearAllPeerTyping(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE wa_chats SET peer_typing_until = NULL
		WHERE peer_typing_until IS NOT NULL
	`)
	return err
}

// TypingRequest is one row from wa_typing_outbound.
type TypingRequest struct {
	ChatJID string
	State   string // 'composing' | 'paused'
}

// DrainTypingOutbound returns up to `limit` unprocessed rows from
// wa_typing_outbound and stamps processed_at = NOW() to claim them.
// Frontend upserts via typing.php reset processed_at to NULL on every
// new request, so the same row oscillates pending → processed → pending
// without ever needing a DELETE (wabridge has no DELETE privilege on
// this table — cpanel grant constraint).
//
// Rows ordered by requested_at to flush the oldest pending request
// first when bursts arrive.
func (s *Store) DrainTypingOutbound(ctx context.Context, limit int) ([]TypingRequest, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(ctx, `
		SELECT chat_jid, state FROM wa_typing_outbound
		WHERE processed_at IS NULL
		ORDER BY requested_at LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	var batch []TypingRequest
	for rows.Next() {
		var r TypingRequest
		if err := rows.Scan(&r.ChatJID, &r.State); err != nil {
			rows.Close()
			return nil, err
		}
		batch = append(batch, r)
	}
	rows.Close()
	if len(batch) == 0 {
		return nil, tx.Commit()
	}

	placeholders := make([]string, len(batch))
	args := make([]any, 0, len(batch))
	for i, r := range batch {
		placeholders[i] = "?"
		args = append(args, r.ChatJID)
	}
	if _, err := tx.ExecContext(ctx,
		fmt.Sprintf(
			"UPDATE wa_typing_outbound SET processed_at = CURRENT_TIMESTAMP "+
				"WHERE chat_jid IN (%s) AND processed_at IS NULL",
			strings.Join(placeholders, ","),
		),
		args...,
	); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return batch, nil
}

// FindStaleProfilePics returns JIDs of non-group chats whose profile_pic_url
// was last refreshed more than `olderThan` ago (or has never been
// stamped). Used by the periodic refresher to keep avatars fresh in
// quiet chats that no longer trigger UpsertChat on each message.
func (s *Store) FindStaleProfilePics(ctx context.Context, olderThan time.Duration, limit int) ([]string, error) {
	const q = `
		SELECT jid FROM wa_chats
		WHERE is_group = 0
		  AND archived = 0
		  AND (profile_pic_updated_at IS NULL OR profile_pic_updated_at < (UTC_TIMESTAMP() - INTERVAL ? SECOND))
		ORDER BY profile_pic_updated_at IS NULL DESC, profile_pic_updated_at ASC
		LIMIT ?
	`
	rows, err := s.db.QueryContext(ctx, q, int(olderThan.Seconds()), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var jid string
		if err := rows.Scan(&jid); err != nil {
			return nil, err
		}
		out = append(out, jid)
	}
	return out, rows.Err()
}

// UpdateProfilePic writes a new profile_pic_url and stamps
// profile_pic_updated_at. Passing an empty URL clears it (which marks the
// row as recently checked, so we don't re-poll a permanently unavailable
// pic every cycle).
func (s *Store) UpdateProfilePic(ctx context.Context, jid string, url string) error {
	var urlVal sql.NullString
	if url != "" {
		urlVal = sql.NullString{String: url, Valid: true}
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE wa_chats
		SET profile_pic_url = ?, profile_pic_updated_at = UTC_TIMESTAMP()
		WHERE jid = ?
	`, urlVal, jid)
	return err
}

type Message struct {
	ID                string
	ChatJID           string
	SenderJID         sql.NullString
	FromMe            bool
	MessageType       string
	Body              sql.NullString
	MediaURL          sql.NullString
	MediaMime         sql.NullString
	MediaSize         sql.NullInt64
	MediaFilename     sql.NullString
	MediaSHA256       sql.NullString
	MediaThumbnailURL sql.NullString
	QuotedMessageID   sql.NullString
	QuotedPreview     sql.NullString // short text of the quoted message
	QuotedSenderJID   sql.NullString // who wrote the quoted message
	TargetMessageID   sql.NullString // message_type='reaction': the message reacted to
	ClientRequestID   sql.NullString // set on outbound sends so the UI can match its optimistic bubble
	Timestamp         time.Time
	Status            sql.NullString
}

// featureQuotes names the Fallback feature for the reply / reaction columns
// of wa_messages (quoted_preview, quoted_sender_jid, target_message_id).
const featureQuotes = "wa_messages.quotes"

// featureEdits names the Fallback feature for edited_at / original_body /
// revoked_at.
const featureEdits = "wa_messages.edits"

// featureHeartbeat names the Fallback feature for wa_pairing.last_seen_at.
const featureHeartbeat = "wa_pairing.last_seen_at"

// InsertMessage writes a single message. INSERT IGNORE so a re-delivered
// message (same ID) doesn't error. Against a schema without the reply columns
// it degrades to the legacy column list (quote preview and reaction target
// are dropped, everything else is stored).
func (s *Store) InsertMessage(ctx context.Context, m Message) error {
	return s.Fallback(featureQuotes, func() error {
		const q = `
			INSERT IGNORE INTO wa_messages (
				id, chat_jid, sender_jid, from_me, message_type, body,
				media_url, media_mime, media_size, media_filename, media_sha256,
				media_thumbnail_url,
				quoted_message_id, quoted_preview, quoted_sender_jid, target_message_id,
				client_request_id, timestamp, status
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`
		_, err := s.db.ExecContext(ctx, q,
			m.ID, m.ChatJID, m.SenderJID, boolInt(m.FromMe), m.MessageType, m.Body,
			m.MediaURL, m.MediaMime, m.MediaSize, m.MediaFilename, m.MediaSHA256,
			m.MediaThumbnailURL,
			m.QuotedMessageID, m.QuotedPreview, m.QuotedSenderJID, m.TargetMessageID,
			m.ClientRequestID, m.Timestamp.UTC(), m.Status,
		)
		return err
	}, func() error {
		const q = `
			INSERT IGNORE INTO wa_messages (
				id, chat_jid, sender_jid, from_me, message_type, body,
				media_url, media_mime, media_size, media_filename, media_sha256,
				media_thumbnail_url,
				quoted_message_id, client_request_id, timestamp, status
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`
		_, err := s.db.ExecContext(ctx, q,
			m.ID, m.ChatJID, m.SenderJID, boolInt(m.FromMe), m.MessageType, m.Body,
			m.MediaURL, m.MediaMime, m.MediaSize, m.MediaFilename, m.MediaSHA256,
			m.MediaThumbnailURL,
			m.QuotedMessageID, m.ClientRequestID, m.Timestamp.UTC(), m.Status,
		)
		return err
	})
}

// ApplyEdit rewrites the body of an existing message after the sender edited
// it: original_body keeps the text from before the FIRST edit (an address a
// customer later changes is worth auditing), body takes the new text and
// edited_at is stamped. No new row is created. It returns false when the
// original message is not in the table (or belongs to another chat or
// sender direction), so the caller can ignore the edit.
func (s *Store) ApplyEdit(ctx context.Context, chatJID, targetID string, fromMe bool, newBody sql.NullString) (bool, error) {
	err := s.Fallback(featureEdits, func() error {
		_, err := s.db.ExecContext(ctx, `
			UPDATE wa_messages
			SET original_body = COALESCE(original_body, body),
			    body          = ?,
			    edited_at     = UTC_TIMESTAMP()
			WHERE id = ? AND chat_jid = ? AND from_me = ?
		`, newBody, targetID, chatJID, boolInt(fromMe))
		return err
	}, func() error {
		_, err := s.db.ExecContext(ctx, `
			UPDATE wa_messages SET body = ?
			WHERE id = ? AND chat_jid = ? AND from_me = ?
		`, newBody, targetID, chatJID, boolInt(fromMe))
		return err
	})
	if err != nil {
		return false, err
	}
	// RowsAffected counts changed rows only (an identical re-delivered edit
	// within the same second changes nothing), so existence is its own query.
	return s.messageExists(ctx, chatJID, targetID)
}

// RevokeMessage marks the original message as deleted for everyone, keeping
// its body for audit (the PHP layer hides it). It returns false when the
// original is not stored, or when the schema predates revoked_at; the caller
// then keeps the old behaviour and inserts a 'revoked' row.
func (s *Store) RevokeMessage(ctx context.Context, chatJID, targetID string) (bool, error) {
	if s.FeatureOff(featureEdits) {
		return false, nil
	}
	err := s.Fallback(featureEdits, func() error {
		_, err := s.db.ExecContext(ctx, `
			UPDATE wa_messages
			SET revoked_at = COALESCE(revoked_at, UTC_TIMESTAMP())
			WHERE id = ? AND chat_jid = ?
		`, targetID, chatJID)
		return err
	}, nil)
	if err != nil {
		return false, err
	}
	if s.FeatureOff(featureEdits) {
		return false, nil
	}
	return s.messageExists(ctx, chatJID, targetID)
}

func (s *Store) messageExists(ctx context.Context, chatJID, id string) (bool, error) {
	var one int
	err := s.db.QueryRowContext(ctx,
		`SELECT 1 FROM wa_messages WHERE id = ? AND chat_jid = ? LIMIT 1`, id, chatJID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// Reaction is one reaction event. Emoji == "" means the sender removed theirs.
type Reaction struct {
	ID        string // id of the reaction message itself
	ChatJID   string
	SenderJID sql.NullString
	FromMe    bool
	IsGroup   bool
	TargetID  string // the message reacted to
	Emoji     string
	Timestamp time.Time
}

// UpsertReaction keeps one reaction per (sender, target message): a new
// reaction replaces the sender's previous one, and an empty emoji removes it.
//
// The bridge's MySQL user has no DELETE grant, so "remove" is a soft delete:
// the row flips to message_type='system' (hidden everywhere, excluded from
// the reaction aggregation) and keeps its target_message_id so a later
// reaction by the same sender revives it. In a 1:1 chat there are only two
// possible reactors, so the match is on from_me alone: the contact's sender
// JID can flip between phone and LID form between events.
//
// The reacted-to message gets its status_updated_at bumped so the web
// client's incremental refresh picks the new chip up.
func (s *Store) UpsertReaction(ctx context.Context, r Reaction) error {
	if r.TargetID == "" {
		return nil
	}
	var existing string
	err := s.Fallback(featureQuotes, func() error {
		q := `SELECT id FROM wa_messages
			WHERE chat_jid = ? AND target_message_id = ?
			  AND message_type IN ('reaction', 'system') AND from_me = ?`
		args := []any{r.ChatJID, r.TargetID, boolInt(r.FromMe)}
		if !r.FromMe && r.IsGroup {
			q += ` AND sender_jid = ?`
			args = append(args, r.SenderJID)
		}
		q += ` ORDER BY timestamp DESC LIMIT 1`
		e := s.db.QueryRowContext(ctx, q, args...).Scan(&existing)
		if errors.Is(e, sql.ErrNoRows) {
			return nil
		}
		return e
	}, func() error {
		// No target_message_id column: nothing to match on or to aggregate.
		// Keep the legacy behaviour (store the reaction as a plain row).
		if r.Emoji == "" {
			return nil
		}
		return s.insertLegacyReaction(ctx, r)
	})
	if err != nil {
		return err
	}
	if s.FeatureOff(featureQuotes) {
		return nil
	}

	switch {
	case existing != "" && r.Emoji == "":
		if _, err = s.db.ExecContext(ctx, `
			UPDATE wa_messages SET message_type = 'system', body = NULL, timestamp = ?
			WHERE id = ? AND timestamp <= ?
		`, r.Timestamp.UTC(), existing, r.Timestamp.UTC()); err != nil {
			return err
		}
	case existing != "":
		if _, err = s.db.ExecContext(ctx, `
			UPDATE wa_messages SET message_type = 'reaction', body = ?, timestamp = ?
			WHERE id = ? AND timestamp <= ?
		`, r.Emoji, r.Timestamp.UTC(), existing, r.Timestamp.UTC()); err != nil {
			return err
		}
	case r.Emoji == "":
		return nil // removal of a reaction we never stored
	default:
		if err = s.InsertMessage(ctx, Message{
			ID:              r.ID,
			ChatJID:         r.ChatJID,
			SenderJID:       r.SenderJID,
			FromMe:          r.FromMe,
			MessageType:     "reaction",
			Body:            sql.NullString{String: r.Emoji, Valid: true},
			TargetMessageID: sql.NullString{String: r.TargetID, Valid: true},
			Timestamp:       r.Timestamp,
			Status:          sql.NullString{String: reactionStatus(r.FromMe), Valid: true},
		}); err != nil {
			return err
		}
	}
	_, err = s.db.ExecContext(ctx, `
		UPDATE wa_messages SET status_updated_at = UTC_TIMESTAMP()
		WHERE id = ? AND chat_jid = ?
	`, r.TargetID, r.ChatJID)
	return err
}

func (s *Store) insertLegacyReaction(ctx context.Context, r Reaction) error {
	return s.InsertMessage(ctx, Message{
		ID:          r.ID,
		ChatJID:     r.ChatJID,
		SenderJID:   r.SenderJID,
		FromMe:      r.FromMe,
		MessageType: "reaction",
		Body:        sql.NullString{String: r.Emoji, Valid: true},
		Timestamp:   r.Timestamp,
		Status:      sql.NullString{String: reactionStatus(r.FromMe), Valid: true},
	})
}

func reactionStatus(fromMe bool) string {
	if fromMe {
		return "sent"
	}
	return "received"
}

// QuotedRow is what an outbound reply or reaction needs to know about the
// message it points at.
type QuotedRow struct {
	SenderJID     sql.NullString
	FromMe        bool
	MessageType   string
	Body          sql.NullString
	MediaFilename sql.NullString
}

// GetQuotedRow loads the message a reply or reaction targets. found=false
// means the row is not in the table.
func (s *Store) GetQuotedRow(ctx context.Context, chatJID, id string) (row QuotedRow, found bool, err error) {
	var fromMe int
	err = s.db.QueryRowContext(ctx, `
		SELECT sender_jid, from_me, message_type, body, media_filename
		FROM wa_messages WHERE id = ? AND chat_jid = ? LIMIT 1
	`, id, chatJID).Scan(&row.SenderJID, &fromMe, &row.MessageType, &row.Body, &row.MediaFilename)
	if errors.Is(err, sql.ErrNoRows) {
		return QuotedRow{}, false, nil
	}
	if err != nil {
		return QuotedRow{}, false, err
	}
	row.FromMe = fromMe == 1
	return row, true, nil
}

// ClearUnread zeroes a chat's unread counter. Used when the staff read the
// chat on the phone or WhatsApp Web (a read receipt from one of our own
// devices).
func (s *Store) ClearUnread(ctx context.Context, chatJID string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE wa_chats SET unread_count = 0 WHERE jid = ? AND unread_count > 0`, chatJID)
	return err
}

// Heartbeat stamps wa_pairing.last_seen_at so the web app can tell a live
// bridge from a dead one. Missing column: logged once, then skipped.
func (s *Store) Heartbeat(ctx context.Context) error {
	return s.Fallback(featureHeartbeat, func() error {
		_, err := s.db.ExecContext(ctx, `UPDATE wa_pairing SET last_seen_at = NOW() WHERE id = 1`)
		return err
	}, nil)
}

// MarkMessagesStatus updates wa_messages.status for the given IDs, but
// only upgrades it (received < sent < delivered < read; failed is sticky).
// Receipts from WhatsApp can arrive out of order — a "delivered" event may
// land after "read" — so we only allow forward transitions.
//
// Limited to outbound messages (from_me=1); inbound rows stay at 'received'.
func (s *Store) MarkMessagesStatus(ctx context.Context, ids []string, newStatus string) error {
	if len(ids) == 0 {
		return nil
	}
	rank := map[string]int{
		"received": 0, "sent": 1, "delivered": 2, "read": 3,
	}
	target, ok := rank[newStatus]
	if !ok {
		return fmt.Errorf("unknown status %q", newStatus)
	}
	allowedCurrent := make([]string, 0, len(rank))
	for cur, r := range rank {
		if r < target {
			allowedCurrent = append(allowedCurrent, cur)
		}
	}
	if len(allowedCurrent) == 0 {
		return nil
	}

	idPlaceholders := make([]string, len(ids))
	args := make([]any, 0, len(ids)+len(allowedCurrent)+1)
	args = append(args, newStatus)
	for i, id := range ids {
		idPlaceholders[i] = "?"
		args = append(args, id)
	}
	curPlaceholders := make([]string, len(allowedCurrent))
	for i, c := range allowedCurrent {
		curPlaceholders[i] = "?"
		args = append(args, c)
	}
	q := fmt.Sprintf(`
		UPDATE wa_messages
		SET status = ?
		WHERE id IN (%s)
		  AND from_me = 1
		  AND (status IS NULL OR status IN (%s))
	`, strings.Join(idPlaceholders, ","), strings.Join(curPlaceholders, ","))
	_, err := s.db.ExecContext(ctx, q, args...)
	return err
}

// RecomputeChatTail refreshes last_message_at / preview / from_me on a chat
// from whatever is currently in wa_messages. Used after backfilling history
// in bulk, and after an edit or a revoke changed what the tail row says.
// Reaction and system rows never count as the tail (WhatsApp does not move a
// chat for a reaction). A chat with no countable row is left as it is.
func (s *Store) RecomputeChatTail(ctx context.Context, chatJID string) error {
	return s.Fallback(featureEdits, func() error {
		_, err := s.db.ExecContext(ctx, tailQuery(`CASE WHEN tail.revoked_at IS NOT NULL THEN 'Mensaje eliminado' ELSE LEFT(COALESCE(tail.body, ''), 160) END`), chatJID, chatJID)
		return err
	}, func() error {
		_, err := s.db.ExecContext(ctx, tailQuery(`LEFT(COALESCE(tail.body, ''), 160)`), chatJID, chatJID)
		return err
	})
}

func tailQuery(previewExpr string) string {
	return `
		UPDATE wa_chats c
		LEFT JOIN (
			SELECT chat_jid,
			       MAX(timestamp) AS max_ts
			FROM wa_messages
			WHERE chat_jid = ?
			  AND message_type NOT IN ('reaction', 'system')
			GROUP BY chat_jid
		) agg ON agg.chat_jid = c.jid
		LEFT JOIN wa_messages tail
		       ON tail.chat_jid = c.jid AND tail.timestamp = agg.max_ts
		      AND tail.message_type NOT IN ('reaction', 'system')
		SET c.last_message_at      = COALESCE(agg.max_ts, c.last_message_at),
		    c.last_message_preview = CASE WHEN agg.max_ts IS NULL THEN c.last_message_preview ELSE ` + previewExpr + ` END,
		    c.last_message_from_me = CASE WHEN agg.max_ts IS NULL THEN c.last_message_from_me ELSE COALESCE(tail.from_me, 0) END
		WHERE c.jid = ?
	`
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
