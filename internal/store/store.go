// Package store wraps writes to the wa_chats / wa_messages tables in
// env_producto. All queries go through the SSH tunnel.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
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
func (s *Store) UpsertChat(ctx context.Context, c Chat) error {
	const q = `
		INSERT INTO wa_chats (
			jid, display_name, phone_e164, is_group, profile_pic_url,
			last_message_at, last_message_preview, last_message_from_me,
			unread_count
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			display_name          = COALESCE(VALUES(display_name), display_name),
			phone_e164            = COALESCE(VALUES(phone_e164), phone_e164),
			profile_pic_url       = COALESCE(VALUES(profile_pic_url), profile_pic_url),
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
		c.LastMessageAt, c.LastMessagePreview, boolInt(c.LastMessageFromMe),
		initialUnread,
		boolInt(c.IncrementUnread),
	)
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
			quoted_message_id, client_request_id, timestamp, status
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
	_, err := s.db.ExecContext(ctx, q,
		m.ID, m.ChatJID, m.SenderJID, boolInt(m.FromMe), m.MessageType, m.Body,
		m.MediaURL, m.MediaMime, m.MediaSize, m.MediaFilename, m.MediaSHA256,
		m.QuotedMessageID, m.ClientRequestID, m.Timestamp.UTC(), m.Status,
	)
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
