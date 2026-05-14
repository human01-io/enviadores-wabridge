// Package store wraps writes to the wa_chats / wa_messages tables in
// env_producto. All queries go through the SSH tunnel.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/enviadores/wabridge/internal/config"
	_ "github.com/go-sql-driver/mysql"
)

type Store struct {
	db *sql.DB
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
	return &Store{db: db}, nil
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
	ID              string
	ChatJID         string
	SenderJID       sql.NullString
	FromMe          bool
	MessageType     string
	Body            sql.NullString
	MediaURL        sql.NullString
	MediaMime       sql.NullString
	MediaSize       sql.NullInt64
	MediaFilename   sql.NullString
	MediaSHA256     sql.NullString
	MediaThumbnailURL sql.NullString
	QuotedMessageID sql.NullString
	ClientRequestID sql.NullString // set on outbound sends so the UI can match its optimistic bubble
	Timestamp       time.Time
	Status          sql.NullString
}

// InsertMessage writes a single message. INSERT IGNORE so a re-delivered
// message (same ID) doesn't error.
func (s *Store) InsertMessage(ctx context.Context, m Message) error {
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
// in bulk so the chat list reflects the most recent timestamp without each
// insert paying the cost of an UpsertChat.
func (s *Store) RecomputeChatTail(ctx context.Context, chatJID string) error {
	const q = `
		UPDATE wa_chats c
		LEFT JOIN (
			SELECT chat_jid,
			       MAX(timestamp) AS max_ts
			FROM wa_messages
			WHERE chat_jid = ?
			GROUP BY chat_jid
		) agg ON agg.chat_jid = c.jid
		LEFT JOIN wa_messages tail
		       ON tail.chat_jid = c.jid AND tail.timestamp = agg.max_ts
		SET c.last_message_at      = agg.max_ts,
		    c.last_message_preview = LEFT(COALESCE(tail.body, ''), 160),
		    c.last_message_from_me = COALESCE(tail.from_me, 0)
		WHERE c.jid = ?
	`
	_, err := s.db.ExecContext(ctx, q, chatJID, chatJID)
	return err
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
