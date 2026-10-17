# enviadores-wabridge

A small Go service that mirrors WhatsApp messages from a single linked device
into the `env_producto` MySQL database, so the platform's `/whatsapp` inbox
page (gated to `role=admin_user @ PDV0000001`) can read them.

Designed to run on **one Windows machine** at the PDV0000001 office, with one
linked WhatsApp Business number. It captures inbound chats and media and sends
what the web inbox queues in `wa_outbound` (text, media, quoted replies,
reactions).

## What it does

1. Opens an **SSH tunnel** to the shared host (`enviadores.com.mx`) using your
   private key. Both MySQL traffic and SFTP media uploads ride this one
   connection.
2. Connects to **MySQL** through the tunnel (no remote-MySQL firewall rule
   needed).
3. Connects to **WhatsApp** via [whatsmeow](https://github.com/tulir/whatsmeow).
   First run prints a QR code — scan it from WhatsApp → Linked Devices.
4. On every inbound message: upserts `wa_chats`, downloads any media,
   **SFTPs it to** `<remote_path>/<sha256>.<ext>` on the shared host,
   and inserts a `wa_messages` row.

The React app at `/whatsapp` polls the PHP gateway every ~7s for new messages
on the active chat, so latency end-to-end is single-digit seconds.

## Prerequisites

- Go 1.22+ on Windows
- An SSH key pair authorized on the shared host. If `ssh <your-host> ls`
  works from the office machine, you're set.
- A MySQL user that can read and write **only the bridge's own tables**:
  `wa_chats`, `wa_messages`, `wa_outbound`, `wa_pairing` and
  `wa_typing_outbound` (`SELECT, INSERT, UPDATE`; the bridge never deletes).
  Its password lives in `config.yaml` on the office machine, so do not give it
  database-wide access: a lost or compromised office PC would then expose every
  customer, password hash and ledger row. On a box where you control MySQL:
  ```sql
  CREATE USER 'wabridge'@'localhost' IDENTIFIED BY '<STRONG_PASSWORD>';
  GRANT SELECT, INSERT, UPDATE ON <db>.wa_chats           TO 'wabridge'@'localhost';
  GRANT SELECT, INSERT, UPDATE ON <db>.wa_messages        TO 'wabridge'@'localhost';
  GRANT SELECT, INSERT, UPDATE ON <db>.wa_outbound        TO 'wabridge'@'localhost';
  GRANT SELECT, INSERT, UPDATE ON <db>.wa_pairing         TO 'wabridge'@'localhost';
  GRANT SELECT, INSERT, UPDATE ON <db>.wa_typing_outbound TO 'wabridge'@'localhost';
  ```
  Production (Enviadores, Vultr) runs exactly these grants since 2026-10-01.
  This release (edits, deletes, replies, reactions, read sync, heartbeat) needs
  **no new grant**: it only adds columns to `wa_messages`, `wa_outbound` and
  `wa_pairing`, which the grants above already cover. `wa_ui_events` and
  `wa_quick_replies` are written by the PHP API only, so the bridge user must
  not get them. The bridge still never issues a `DELETE`: removing a reaction
  flips its row to `message_type='system'` instead.
  v0.3.0 (new chats by phone) also needs **no new grant**: it reads and
  updates `wa_outbound` and `wa_pairing` only. `customer_contacts` (the
  retention contact log) is PHP-only; never grant it to the bridge user.
  When a new release touches another table, add that table's grant before
  deploying it. A host that can't do per-table grants (some shared cPanel
  plans) is not a supported target.
- The PHP gateway needs `wa_media/` to be web-readable. By default Apache
  serves anything under `public_html/` — no extra config required. URLs are
  unguessable SHA-256 hashes, but if you want to harden, add `.htaccess` to
  block listing.

## Setup

### Windows (production target)

```cmd
git clone https://github.com/human01-io/enviadores-wabridge.git
cd enviadores-wabridge
go mod tidy
copy config.example.yaml config.yaml
notepad config.yaml   :: fill in SSH user, key path, MySQL password
go build -o wabridge.exe ./cmd/wabridge
```

### macOS / Linux (same recipe, same binary semantics)

```bash
git clone https://github.com/human01-io/enviadores-wabridge.git
cd enviadores-wabridge
go mod tidy
cp config.example.yaml config.yaml
$EDITOR config.yaml   # ~ in paths is expanded to your home dir
go build -o wabridge ./cmd/wabridge
```

The same `config.yaml` works on every platform — `~/.ssh/id_ed25519` is
expanded per-OS. All platform-specific service plumbing (launchd, systemd,
Windows SCM) is handled by `kardianos/service` behind `wabridge install`.

## Edits, deletes, replies, reactions

Behaviour of the `feat/edits-replies-reactions` release. It needs the
`wa_messages` / `wa_outbound` / `wa_pairing` column migration applied first
(`docs/env_producto_db/migrations/` in the platform repo); a bridge started
against an older schema logs one line per missing feature and keeps working
with the old statements.

- **Edits** (`ProtocolMessage` MESSAGE_EDIT, also the encrypted
  `SecretEncryptedMessage` form) rewrite the original row: `original_body`
  keeps the text from before the first edit, `body` is the new text,
  `edited_at` is stamped. No new row. Only the author's edits apply. An edit
  of a message the bridge never stored, or one whose secret is gone, is
  ignored (logged at info). On a schema without the edit columns the new text
  is stored as a row of its own and the original is left as it was.
- **Deletes for everyone** set `revoked_at` on the original (the body stays for
  audit; the PHP API hides it). In a 1:1 chat only the author can delete; in a
  group an admin may delete anyone's message. If the original is not stored,
  a `revoked` row is inserted as before.
- **Finding the original**: edits, deletes and reactions look the target up by
  its id across every form the chat may be stored under (the canonical LID,
  the raw JID, and the phone JID of a LID chat), because a message stored
  before the chat's LID mapping was learned keeps the phone JID. Edit and
  delete writes are retried 3 times with fresh 20 s deadlines (the event is
  never re-delivered); a failure after that is logged at error level.
- **Chat preview** after an edit or delete is rebuilt with the live path's
  labels (a caption-less photo stays "Imagen", a deleted tail is "Mensaje
  eliminado").
- **Reactions** are stored as `message_type='reaction'` rows with
  `target_message_id`, in the chat row of the message they target. One per
  sender per message: a new emoji replaces the old one, an empty one removes
  it. In groups the sender is matched without its device suffix and in both
  phone and LID form. They never change the chat's preview, order or unread
  count; the reacted-to message gets its `status_updated_at` bumped so the web
  client refreshes it.
- **Replies** are read from the `ContextInfo` of every content type into
  `quoted_message_id`, `quoted_sender_jid` and `quoted_preview`. Outbound
  `wa_outbound.quoted_message_id` sends a quoted reply (text or media);
  `kind='reaction'` sends a reaction (body = emoji, empty = remove). A message
  deleted for everyone (or a reaction/system row) is never quoted: the reply
  goes out without the quote, and a reaction to it fails permanently.
- **Noise**: `@bot` (Meta AI) chats and WhatsApp Status are skipped; protocol
  plumbing is never stored; album children are stored as normal images.
- **Read sync**: a read receipt from one of our own devices marks the chat
  read up to the newest message the receipt names; inbound messages newer
  than that stay unread.
- **Heartbeat**: `wa_pairing.last_seen_at` is stamped on connect and every
  30 s while connected (UTC).

Tests: `go test ./...` (SQL paths run against `internal/testdb`, an in-memory
driver that records statements; no MySQL needed). Production is Windows, so
also check `GOOS=windows go build ./...`.

## New chats by phone (v0.3.0)

The web inbox can write to a customer who never wrote to the office number
(from "Clientes frecuentes"). It needs the `2026_10_18_retention_whatsapp_inbox`
migration (`wa_outbound.to_phone/to_name/error_code`, status `resolve`,
`wa_pairing.bridge_features/bridge_features_at`). Without it the feature stays
off (one log line) and everything else works as before.

- **Queue**: the PHP API writes `status='resolve'`, `chat_jid=NULL`,
  `to_phone='+52XXXXXXXXXX'`, `to_name`, `kind='text'`. The existing-chat poll
  still selects only `status='pending'`, so older bridges never see these rows.
- **Order**: validate `to_phone` (`+52` and 10 digits, else `INVALID_PHONE`) ->
  `IsOnWhatsApp` (it returns the canonical JID, normally `@lid`, and caches
  the LID mapping; a PN answer is canonicalized like every chat) -> send the
  text -> `wa_chats` upsert -> `wa_messages` row -> `wa_outbound` stamped
  `sent` with its `chat_jid`. A number without WhatsApp fails with
  `NOT_ON_WHATSAPP` and writes no chat or message: a chat row exists only
  after a send to a JID WhatsApp confirmed. An existing chat row for that JID
  (the customer wrote meanwhile) is updated, not duplicated.
- **Retries**: a lookup or send error keeps the row in `resolve` with the
  normal backoff; at the cap it fails with `LOOKUP_FAILED` / `SEND_FAILED`.
  After a successful send the row is never sent again: if the stamp fails
  the bridge retries only the writes (in memory, until restart). If the chat
  row cannot be written the stamp keeps `chat_jid=NULL`; the customer's reply
  creates the chat.
- **Pacing**: at most one new-chat lookup every 20 s. Existing-chat sends are
  not paced.
- **error_code**: every permanent failure writes `wa_outbound.error_code`
  (`NOT_ON_WHATSAPP`, `INVALID_PHONE`, `SEND_FAILED`, `LOOKUP_FAILED`,
  `UNSUPPORTED_KIND`, `INVALID_JID`, `MEDIA`, `REACTION_TARGET`), and
  `error_message` always starts with `<CODE>: `.
- **Readiness**: every heartbeat also writes `bridge_features='new_chat'` and
  `bridge_features_at`. The PHP API queues new chats only while those are
  fresh, so rolling the bridge back switches the feature off by itself.

**`wabridge check-number +525512345678`** asks WhatsApp whether a phone has
an account and prints `is_in`, `jid`, `phone_number`, `verified_name` and the
`store_jid` a new chat would use. It sends nothing and writes nothing to
MySQL. It runs inside the running bridge (service or `wabridge run`): the
command drops `check-number.req` next to `config.yaml` and waits up to 60 s
for `check-number.res`. It does not open a second WhatsApp connection,
because that would replace the service's session and swallow the messages
that arrive meanwhile. Try both `+52` and `+521` forms on a staff number:
they should give the same `store_jid`.

## First run (QR pairing)

```cmd
wabridge.exe run        :: or `./wabridge run` on macOS/Linux
```

The bridge writes its QR codes to the `wa_pairing` MySQL row, where the
web app reads them. Open **https://app.enviadores.com.mx/whatsapp** as an
admin with active PDV `PDV0000001` — the page renders the QR. Scan it with
**WhatsApp → ⋮ → Linked devices → Link a device** on the phone holding the
business number. The card flips to the chat list as soon as whatsmeow
fires `PairSuccess`.

The same QR string also prints to stdout as a terminal fallback (one-line
encoded string, not a rendered code).

`Ctrl+C` to stop.

## Install as a service (Windows / macOS / Linux)

After pairing has been done once and `whatsmeow.db` exists, install the
service. `kardianos/service` picks the right backend per OS:

| OS      | Backend                | Auto-starts on |
|---------|------------------------|----------------|
| Windows | Service Control Manager | Boot          |
| macOS   | launchd (LaunchDaemon)  | Boot          |
| Linux   | systemd                 | Boot          |

```bash
sudo ./wabridge install     # macOS/Linux
sudo ./wabridge start
```
```cmd
wabridge.exe install        :: Windows (run elevated cmd)
wabridge.exe start
```

Service control:

```bash
./wabridge stop
./wabridge start
./wabridge uninstall
```

Logs:
- Windows: Event Log under `Enviadores WhatsApp Bridge`
- macOS:   `/var/log/system.log`, and stderr in `/Library/LaunchDaemons` plist
- Linux:   `journalctl -u enviadores-wabridge`

For verbose output, set `whatsmeow.log_level: DEBUG` in `config.yaml` and
run `wabridge run` in the foreground.

## Layout

```
cmd/wabridge/main.go          entry point + Windows service wiring
internal/config/              YAML loader
internal/tunnel/              SSH tunnel + SFTP client (one shared connection)
internal/store/               MySQL writes (wa_chats, wa_messages) + unknown-column fallback
internal/testdb/              in-memory database/sql driver for unit tests
internal/media/               SFTP media upload + sha256 dedup
internal/wabridge/            whatsmeow event handler — the orchestration glue
```

## What this bridge does NOT do (yet)

- Group chat subjects. Only DM `display_name` is populated (from `PushName`).
- Forward profile-picture URLs through SFTP — they're stored as temporary
  WhatsApp signed URLs and may 404. The React UI falls back to initials.
- Backfill history older than what whatsmeow's history sync provides on
  pairing.

If any of those become important, they're small additions to `internal/wabridge`.

## Troubleshooting

- **Logs.** Both `wabridge run` and the service write to `wabridge.log`
  next to `config.yaml` (rotated to `wabridge.log.1` at 10 MB). Under the
  Windows service this is the only place output goes.
- **The page asks for a QR again (session dropped).** The reason is kept in
  `wa_pairing.last_event` as `session lost: … @ <UTC time>` until the next
  successful pairing — visible via `GET /v2/whatsapp/pairing`. "Logged out
  by WhatsApp (stream error…)" usually means the device was removed on the
  phone; a connect-time code or "client outdated" points at WhatsApp
  rejecting the client — bump the whatsmeow pin.
- **QR code never appears.** Check `whatsmeow.db` permissions and delete it
  if corrupted. Re-pair.
- **`open tunnel: ssh dial: ...`** — make sure `ssh enviadores echo ok`
  works from cmd.exe as the same user the service will run as. On Windows,
  the SYSTEM account doesn't share your user's SSH keys — install the
  service with `wabridge.exe install --user .\YourUsername` or copy
  `id_ed25519` to a path the service account can read.
- **Messages arrive but the inbox is empty.** Check the user has
  `default_pdv_id = 'PDV0000001'` in `users` — the PHP endpoint gates on
  that.
- **Media doesn't render.** Verify `/wa_media/<sha>.<ext>` is reachable via
  HTTPS and that Apache isn't blocking it. `curl -I https://enviadores.com.mx/wa_media/<sha>.pdf`
  should return `200`.

## Pinning whatsmeow

whatsmeow comes from the fork `human01-io/whatsmeow-fork` through a `replace`
in `go.mod`. The fork's `bizwrapper-fix` branch is upstream `tulir/whatsmeow`
plus one patch that skips the `<biz>` wrapper for
`InteractiveResponseMessage` (without it WhatsApp rejects Meta Flow
responses). celeste pins the same fork, so bump both together.

To pull in upstream fixes:

```bash
cd whatsmeow-fork
git fetch origin && git checkout bizwrapper-fix && git rebase origin/main
go build ./... && git push --force-with-lease fork bizwrapper-fix
cd ../enviadores-wabridge
GOPROXY=direct go mod edit -replace go.mau.fi/whatsmeow=github.com/human01-io/whatsmeow-fork@<new-sha>
GOPROXY=direct go mod tidy && go build ./... && go test ./...
```

Upstream sometimes raises the minimum Go version, so let `go mod tidy` move
the `go` directive; CI reads it via `go-version-file`. If `go build` breaks on
API changes, the call sites are in `internal/wabridge/wabridge.go`.
