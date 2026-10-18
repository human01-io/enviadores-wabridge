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
	"unicode/utf8"

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
//
// When the error names a column that columnFeature assigns to a different
// feature, it is returned untouched: a nested Fallback for that feature (see
// InsertMessage) switches the right one off.
func (s *Store) Fallback(feature string, newFn, oldFn func() error) error {
	if !s.FeatureOff(feature) {
		err := newFn()
		if err == nil || !IsUnknownColumn(err) {
			return err
		}
		if owner, ok := columnFeature[unknownColumnName(err)]; ok && owner != feature {
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

// columnFeature names the Fallback feature that owns a column of a newer
// migration, for features whose statements nest inside another feature's
// Fallback. An unknown-column error naming one of these columns only switches
// its own feature off; any other column (or an error whose column cannot be
// read) is taken to belong to the feature whose Fallback sees it first.
var columnFeature = map[string]string{
	"location_name":    featureLocation,
	"location_address": featureLocation,
}

// unknownColumnName reads the column out of MySQL's "Unknown column 'x' in
// 'field list'" message (a qualified 'm.x' gives "x"). Empty when the message
// has another shape.
func unknownColumnName(err error) string {
	var me *mysql.MySQLError
	if !errors.As(err, &me) {
		return ""
	}
	msg := me.Message
	i := strings.IndexByte(msg, '\'')
	if i < 0 {
		return ""
	}
	j := strings.IndexByte(msg[i+1:], '\'')
	if j < 0 {
		return ""
	}
	col := msg[i+1 : i+1+j]
	if k := strings.LastIndexByte(col, '.'); k >= 0 {
		col = col[k+1:]
	}
	return col
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
	// LocationName / LocationAddress are the place a message_type='location'
	// row points at (WhatsApp's LocationMessage.Name / .Address). Both NULL
	// for a bare pin or a live location; the coordinates stay in Body.
	LocationName    sql.NullString
	LocationAddress sql.NullString
	Timestamp       time.Time
	Status          sql.NullString
}

// Widths of wa_messages.location_name / location_address (characters).
const (
	LocationNameMax    = 255
	LocationAddressMax = 500
)

// featureQuotes names the Fallback feature for the reply / reaction columns
// of wa_messages (quoted_preview, quoted_sender_jid, target_message_id).
const featureQuotes = "wa_messages.quotes"

// featureEdits names the Fallback feature for edited_at / original_body /
// revoked_at.
const featureEdits = "wa_messages.edits"

// featureLocation names the Fallback feature for location_name /
// location_address.
const featureLocation = "wa_messages.location"

// featureHeartbeat names the Fallback feature for wa_pairing.last_seen_at.
const featureHeartbeat = "wa_pairing.last_seen_at"

// InsertMessage writes a single message. INSERT IGNORE so a re-delivered
// message (same ID) doesn't error. Against a schema without the reply columns
// it degrades to the legacy column list (quote preview and reaction target
// are dropped, everything else is stored). A location with a place name or
// address also writes location_name / location_address; against a schema
// without them it degrades to the statement without (the pin, its
// coordinates and its map preview are still stored).
func (s *Store) InsertMessage(ctx context.Context, m Message) error {
	if !m.LocationName.Valid && !m.LocationAddress.Valid {
		return s.insertMessage(ctx, m, false)
	}
	return s.Fallback(featureLocation,
		func() error { return s.insertMessage(ctx, m, true) },
		func() error { return s.insertMessage(ctx, m, false) })
}

// insertMessage runs the INSERT with or without the location columns, itself
// falling back to the legacy reply columns.
func (s *Store) insertMessage(ctx context.Context, m Message, withLocation bool) error {
	locCols, locPH := "", ""
	var locArgs []any
	if withLocation {
		locCols, locPH = ", location_name, location_address", ", ?, ?"
		locArgs = []any{m.LocationName, m.LocationAddress}
	}
	return s.Fallback(featureQuotes, func() error {
		q := `
			INSERT IGNORE INTO wa_messages (
				id, chat_jid, sender_jid, from_me, message_type, body,
				media_url, media_mime, media_size, media_filename, media_sha256,
				media_thumbnail_url,
				quoted_message_id, quoted_preview, quoted_sender_jid, target_message_id,
				client_request_id, timestamp, status` + locCols + `
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?` + locPH + `)
		`
		args := append([]any{
			m.ID, m.ChatJID, m.SenderJID, boolInt(m.FromMe), m.MessageType, m.Body,
			m.MediaURL, m.MediaMime, m.MediaSize, m.MediaFilename, m.MediaSHA256,
			m.MediaThumbnailURL,
			m.QuotedMessageID, m.QuotedPreview, m.QuotedSenderJID, m.TargetMessageID,
			m.ClientRequestID, m.Timestamp.UTC(), m.Status,
		}, locArgs...)
		_, err := s.db.ExecContext(ctx, q, args...)
		return err
	}, func() error {
		q := `
			INSERT IGNORE INTO wa_messages (
				id, chat_jid, sender_jid, from_me, message_type, body,
				media_url, media_mime, media_size, media_filename, media_sha256,
				media_thumbnail_url,
				quoted_message_id, client_request_id, timestamp, status` + locCols + `
			)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?` + locPH + `)
		`
		args := append([]any{
			m.ID, m.ChatJID, m.SenderJID, boolInt(m.FromMe), m.MessageType, m.Body,
			m.MediaURL, m.MediaMime, m.MediaSize, m.MediaFilename, m.MediaSHA256,
			m.MediaThumbnailURL,
			m.QuotedMessageID, m.ClientRequestID, m.Timestamp.UTC(), m.Status,
		}, locArgs...)
		_, err := s.db.ExecContext(ctx, q, args...)
		return err
	})
}

// ErrNotAuthor is returned when an edit (or a 1:1 delete) names a message the
// sender did not write. Official clients never send that; the event is ignored.
var ErrNotAuthor = errors.New("message belongs to the other side")

// ErrEditsUnsupported is returned by ApplyEdit when the schema predates the
// edit columns: the original is left untouched and the caller stores the new
// text as a row of its own (what the previous release did), so neither text
// is lost.
var ErrEditsUnsupported = errors.New("schema has no edit columns")

// MessageRef says where a stored message lives.
type MessageRef struct {
	ChatJID string
	FromMe  bool
}

// LocateMessage finds a message by its id among the forms its chat may be
// stored under (canonical LID, raw phone JID...). wa_messages.id is the
// primary key; the chat list only stops an event from one chat touching a
// message of another. A chat whose LID mapping was learned after the
// message was stored keeps that message under the phone JID, so callers
// pass every alias they know.
func (s *Store) LocateMessage(ctx context.Context, id string, chats []string) (MessageRef, bool, error) {
	if id == "" || len(chats) == 0 {
		return MessageRef{}, false, nil
	}
	ph := make([]string, len(chats))
	args := make([]any, 0, len(chats)+1)
	args = append(args, id)
	for i, c := range chats {
		ph[i] = "?"
		args = append(args, c)
	}
	var ref MessageRef
	var fromMe int
	err := s.db.QueryRowContext(ctx,
		`SELECT chat_jid, from_me FROM wa_messages WHERE id = ? AND chat_jid IN (`+strings.Join(ph, ",")+`) LIMIT 1`,
		args...).Scan(&ref.ChatJID, &fromMe)
	if errors.Is(err, sql.ErrNoRows) {
		return MessageRef{}, false, nil
	}
	if err != nil {
		return MessageRef{}, false, err
	}
	ref.FromMe = fromMe == 1
	return ref, true, nil
}

// ApplyEdit rewrites the body of an existing message after its author edited
// it: original_body keeps the text from before the FIRST edit (an address a
// customer later changes is worth auditing), body takes the new text and
// edited_at is stamped. No new row is created. found=false means the original
// is not stored under any of chats; ErrNotAuthor means it was written by the
// other side; ErrEditsUnsupported means the schema has no edit columns (the
// original is left as it was). ref.ChatJID is the chat the row lives in.
func (s *Store) ApplyEdit(ctx context.Context, chats []string, targetID string, fromMe bool, newBody sql.NullString) (MessageRef, bool, error) {
	ref, found, err := s.LocateMessage(ctx, targetID, chats)
	if err != nil || !found {
		return ref, found, err
	}
	if ref.FromMe != fromMe {
		return ref, true, ErrNotAuthor
	}
	err = s.Fallback(featureEdits, func() error {
		_, err := s.db.ExecContext(ctx, `
			UPDATE wa_messages
			SET original_body = COALESCE(original_body, body),
			    body          = ?,
			    edited_at     = UTC_TIMESTAMP()
			WHERE id = ? AND chat_jid = ?
		`, newBody, targetID, ref.ChatJID)
		return err
	}, nil)
	if err != nil {
		return ref, true, err
	}
	if s.FeatureOff(featureEdits) {
		return ref, true, ErrEditsUnsupported
	}
	return ref, true, nil
}

// RevokeMessage marks the original message as deleted for everyone, keeping
// its body for audit (the PHP layer hides it). In a 1:1 chat only the author
// can delete (ErrNotAuthor otherwise); in a group an admin may delete anyone's
// message. found=false when the original is not stored, or when the schema
// predates revoked_at; the caller then keeps the old behaviour and inserts a
// 'revoked' row.
func (s *Store) RevokeMessage(ctx context.Context, chats []string, targetID string, fromMe, isGroup bool) (MessageRef, bool, error) {
	if s.FeatureOff(featureEdits) {
		return MessageRef{}, false, nil
	}
	ref, found, err := s.LocateMessage(ctx, targetID, chats)
	if err != nil || !found {
		return ref, found, err
	}
	if !isGroup && ref.FromMe != fromMe {
		return ref, true, ErrNotAuthor
	}
	err = s.Fallback(featureEdits, func() error {
		_, err := s.db.ExecContext(ctx, `
			UPDATE wa_messages
			SET revoked_at = COALESCE(revoked_at, UTC_TIMESTAMP())
			WHERE id = ? AND chat_jid = ?
		`, targetID, ref.ChatJID)
		return err
	}, nil)
	if err != nil {
		return ref, true, err
	}
	if s.FeatureOff(featureEdits) {
		return ref, false, nil
	}
	return ref, true, nil
}

// Reaction is one reaction event. Emoji == "" means the sender removed theirs.
type Reaction struct {
	ID        string // id of the reaction message itself
	ChatJID   string
	SenderJID sql.NullString
	// SenderAliases are every form of the sender's JID (without a device
	// suffix: phone and LID) that an earlier reaction of theirs may be stored
	// under. Group chats match on them; empty means SenderJID alone.
	SenderAliases []string
	FromMe        bool
	IsGroup       bool
	TargetID      string // the message reacted to
	Emoji         string
	Timestamp     time.Time
}

// UpsertReaction keeps one reaction per (sender, target message): a new
// reaction replaces the sender's previous one, and an empty emoji removes it.
//
// The bridge's MySQL user has no DELETE grant, so "remove" is a soft delete:
// the row flips to message_type='system' (hidden everywhere, excluded from
// the reaction aggregation) and keeps its target_message_id so a later
// reaction by the same sender revives it. In a 1:1 chat there are only two
// possible reactors, so the match is on from_me alone: the contact's sender
// JID can flip between phone and LID form between events. In a group the
// match is on every known form of the sender (SenderAliases), so a member who
// reacts from another device or under their LID still has one reaction.
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
			senders := r.SenderAliases
			if len(senders) == 0 {
				senders = []string{r.SenderJID.String}
			}
			ph := make([]string, len(senders))
			for i, sj := range senders {
				ph[i] = "?"
				args = append(args, sj)
			}
			q += ` AND sender_jid IN (` + strings.Join(ph, ",") + `)`
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
	// Revoked is true when the message was deleted for everyone (revoked_at
	// set, or a legacy 'revoked' row). Its body is kept for audit only and
	// must never be sent again inside a quote.
	Revoked bool
}

// Quotable reports whether a reply or a reaction may point at the row:
// never at a deleted message, a reaction or a protocol row.
func (r QuotedRow) Quotable() bool {
	if r.Revoked {
		return false
	}
	switch r.MessageType {
	case "revoked", "reaction", "system":
		return false
	}
	return true
}

// GetQuotedRow loads the message a reply or reaction targets. found=false
// means the row is not in the table.
func (s *Store) GetQuotedRow(ctx context.Context, chatJID, id string) (row QuotedRow, found bool, err error) {
	scan := func(q string) func() error {
		return func() error {
			var fromMe, revoked int
			e := s.db.QueryRowContext(ctx, q, id, chatJID).Scan(
				&row.SenderJID, &fromMe, &row.MessageType, &row.Body, &row.MediaFilename, &revoked)
			row.FromMe = fromMe == 1
			row.Revoked = revoked == 1
			return e
		}
	}
	err = s.Fallback(featureEdits,
		scan(`SELECT sender_jid, from_me, message_type, body, media_filename, revoked_at IS NOT NULL
			FROM wa_messages WHERE id = ? AND chat_jid = ? LIMIT 1`),
		scan(`SELECT sender_jid, from_me, message_type, body, media_filename, 0
			FROM wa_messages WHERE id = ? AND chat_jid = ? LIMIT 1`))
	if errors.Is(err, sql.ErrNoRows) {
		return QuotedRow{}, false, nil
	}
	if err != nil {
		return QuotedRow{}, false, err
	}
	return row, true, nil
}

// ClearUnreadUpTo handles a read receipt from one of our own devices (the
// staff read the chat on the phone or WhatsApp Web). The receipt names the
// messages that were read; inbound messages newer than the newest of them
// arrived after that read and stay unread. When none of the ids is stored the
// whole chat is cleared, as before. The count never goes up.
func (s *Store) ClearUnreadUpTo(ctx context.Context, chatJID string, readIDs []string) error {
	if len(readIDs) == 0 {
		return nil
	}
	ph := make([]string, len(readIDs))
	args := make([]any, 0, len(readIDs)+1)
	for i, id := range readIDs {
		ph[i] = "?"
		args = append(args, id)
	}
	args = append(args, chatJID)
	q := `
		UPDATE wa_chats c
		SET c.unread_count = LEAST(c.unread_count, (
			SELECT COUNT(*) FROM wa_messages m
			WHERE m.chat_jid = c.jid
			  AND m.from_me = 0
			  AND m.message_type NOT IN ('reaction', 'system')
			  AND m.timestamp > COALESCE(
			      (SELECT MAX(r.timestamp) FROM wa_messages r WHERE r.id IN (` + strings.Join(ph, ",") + `)),
			      '9999-12-31')
		))
		WHERE c.jid = ? AND c.unread_count > 0`
	_, err := s.db.ExecContext(ctx, q, args...)
	return err
}

// BridgeFeatures is the comma list the heartbeat writes to
// wa_pairing.bridge_features. The web app enables a feature only while a
// bridge that has it is heartbeating, so a rolled-back bridge (which stamps
// last_seen_at without this column) switches it off within minutes. Later
// features append to it ("new_chat,foo").
const BridgeFeatures = "new_chat"

// featureBridgeFeatures names the Fallback feature for
// wa_pairing.bridge_features / bridge_features_at.
const featureBridgeFeatures = "wa_pairing.bridge_features"

// Heartbeat stamps wa_pairing.last_seen_at so the web app can tell a live
// bridge from a dead one, and in the same UPDATE announces BridgeFeatures.
// Without the features columns it stamps last_seen_at alone; without that
// column too, it logs once and skips.
func (s *Store) Heartbeat(ctx context.Context) error {
	return s.Fallback(featureBridgeFeatures, func() error {
		_, err := s.db.ExecContext(ctx, `
			UPDATE wa_pairing
			SET last_seen_at = UTC_TIMESTAMP(),
			    bridge_features = ?,
			    bridge_features_at = UTC_TIMESTAMP()
			WHERE id = 1`, BridgeFeatures)
		return err
	}, func() error {
		return s.Fallback(featureHeartbeat, func() error {
			_, err := s.db.ExecContext(ctx, `UPDATE wa_pairing SET last_seen_at = UTC_TIMESTAMP() WHERE id = 1`)
			return err
		}, nil)
	})
}

// featureErrorCode names the Fallback feature for wa_outbound.error_code.
const featureErrorCode = "wa_outbound.error_code"

// outboundMessageMax is the longest error_message the bridge writes.
const outboundMessageMax = 500

// FailOutbound marks a wa_outbound row permanently failed. code is the
// machine reason (NOT_ON_WHATSAPP, INVALID_PHONE, SEND_FAILED, ...) written
// to error_code; error_message is always "<code>: <detail>", so the web app
// can read the code even from a row written against a schema without the
// column (the legacy statement then runs instead).
func (s *Store) FailOutbound(ctx context.Context, id int64, attempts int, code, detail string) error {
	msg := clip(code+": "+detail, outboundMessageMax)
	return s.Fallback(featureErrorCode, func() error {
		_, err := s.db.ExecContext(ctx, `
			UPDATE wa_outbound
			SET status = 'failed',
			    attempts = ?,
			    last_attempt_at = UTC_TIMESTAMP(),
			    error_message = ?,
			    error_code = ?
			WHERE id = ?`, attempts, msg, code, id)
		return err
	}, func() error {
		_, err := s.db.ExecContext(ctx, `
			UPDATE wa_outbound
			SET status = 'failed',
			    attempts = ?,
			    last_attempt_at = UTC_TIMESTAMP(),
			    error_message = ?
			WHERE id = ?`, attempts, msg, id)
		return err
	})
}

// ClaimNewChatMessageID stores the WhatsApp message ID a new-chat row will be
// sent with, before the send. A bridge that restarts between the send and the
// stamp finds the ID on the row and sends again with the same ID, which the
// recipient's WhatsApp drops as a duplicate, instead of a second message.
// false means the row is no longer 'resolve' or already has an ID (someone
// failed it by hand meanwhile): the caller must not send.
func (s *Store) ClaimNewChatMessageID(ctx context.Context, id int64, waMessageID string) (bool, error) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE wa_outbound
		SET whatsapp_message_id = ?
		WHERE id = ?
		  AND status = 'resolve'
		  AND whatsapp_message_id IS NULL`, waMessageID, id)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// MarkNewChatSent stamps a new-chat row (status='resolve') as sent. It is
// the first write after the send, so the row leaves 'resolve' as soon as
// possible; chat_jid stays NULL until SetOutboundChat, because the FK needs
// the wa_chats row the bridge writes next.
func (s *Store) MarkNewChatSent(ctx context.Context, id int64, waMessageID string) error {
	return s.Fallback(featureErrorCode, func() error {
		_, err := s.db.ExecContext(ctx, `
			UPDATE wa_outbound
			SET status = 'sent',
			    whatsapp_message_id = ?,
			    sent_at = UTC_TIMESTAMP(),
			    error_message = NULL,
			    error_code = NULL
			WHERE id = ?`, waMessageID, id)
		return err
	}, func() error {
		_, err := s.db.ExecContext(ctx, `
			UPDATE wa_outbound
			SET status = 'sent',
			    whatsapp_message_id = ?,
			    sent_at = UTC_TIMESTAMP(),
			    error_message = NULL
			WHERE id = ?`, waMessageID, id)
		return err
	})
}

// SetOutboundChat points a sent new-chat row at the wa_chats row its message
// was stored under. A row whose chat could not be written keeps chat_jid
// NULL; the customer's reply or a receipt creates the chat later.
func (s *Store) SetOutboundChat(ctx context.Context, id int64, chatJID string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE wa_outbound
		SET chat_jid = ?
		WHERE id = ?`, chatJID, id)
	return err
}

// clip cuts s to at most n bytes without splitting a UTF-8 sequence.
func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
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

// TailRow is the newest countable message of a chat.
type TailRow struct {
	MessageType   string
	Body          sql.NullString
	MediaFilename sql.NullString
	Revoked       bool
	FromMe        bool
	Timestamp     time.Time
	// LocationName is the place of a location row (NULL for a bare pin, or
	// when the schema has no location columns yet).
	LocationName sql.NullString
}

// RecomputeChatTail refreshes last_message_at / preview / from_me on a chat
// from whatever is currently in wa_messages. Used after backfilling history
// in bulk, and after an edit or a revoke changed what the tail row says.
// Reaction and system rows never count as the tail (WhatsApp does not move a
// chat for a reaction). preview builds the list label from the tail row with
// the same rules the live path uses (a caption-less photo is "Imagen", not
// blank). A chat with no countable row is left as it is.
func (s *Store) RecomputeChatTail(ctx context.Context, chatJID string, preview func(TailRow) string) error {
	var t TailRow
	const tail = `
		FROM wa_messages
		WHERE chat_jid = ?
		  AND message_type NOT IN ('reaction', 'system')
		ORDER BY timestamp DESC, id DESC
		LIMIT 1`
	// scan selects the tail with or without the edit and location columns
	// (a literal stands in for a column the schema does not have yet).
	scan := func(edits, location bool) func() error {
		revoked, loc := "0", "NULL"
		if edits {
			revoked = "revoked_at IS NOT NULL"
		}
		if location {
			loc = "location_name"
		}
		q := `SELECT message_type, body, media_filename, ` + revoked + `, from_me, timestamp, ` + loc + tail
		return func() error {
			var fromMe, revoked int
			e := s.db.QueryRowContext(ctx, q, chatJID).Scan(
				&t.MessageType, &t.Body, &t.MediaFilename, &revoked, &fromMe, &t.Timestamp, &t.LocationName)
			t.FromMe = fromMe == 1
			t.Revoked = revoked == 1
			return e
		}
	}
	err := s.Fallback(featureLocation,
		func() error { return s.Fallback(featureEdits, scan(true, true), scan(false, true)) },
		func() error { return s.Fallback(featureEdits, scan(true, false), scan(false, false)) })
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
		UPDATE wa_chats
		SET last_message_at = ?, last_message_preview = ?, last_message_from_me = ?
		WHERE jid = ?
	`, t.Timestamp.UTC(), preview(t), boolInt(t.FromMe), chatJID)
	return err
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
