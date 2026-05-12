# enviadores-wabridge

A small Go service that mirrors WhatsApp messages from a single linked device
into the `env_producto` MySQL database, so the platform's `/whatsapp` inbox
page (gated to `role=admin_user @ PDV0000001`) can read them.

Designed to run on **one Windows machine** at the PDV0000001 office, with one
linked WhatsApp Business number. It's a read-mostly bridge — it doesn't send
replies, just captures inbound chats and media.

## What it does

1. Opens an **SSH tunnel** to the shared host (`enviadores.com.mx`) using your
   private key. Both MySQL traffic and SFTP media uploads ride this one
   connection.
2. Connects to **MySQL** through the tunnel (no remote-MySQL firewall rule
   needed).
3. Connects to **WhatsApp** via [whatsmeow](https://github.com/tulir/whatsmeow).
   First run prints a QR code — scan it from WhatsApp → Linked Devices.
4. On every inbound message: upserts `wa_chats`, downloads any media,
   **SFTPs it to** `/home/cxcc76i48ff4/public_html/wa_media/<sha256>.<ext>`,
   and inserts a `wa_messages` row.

The React app at `/whatsapp` polls the PHP gateway every ~7s for new messages
on the active chat, so latency end-to-end is single-digit seconds.

## Prerequisites

- Go 1.22+ on Windows
- An SSH key pair authorized on the shared host. If `ssh enviadores ls` works
  from the office machine, you're set.
- A MySQL user on `env_producto` with `SELECT, INSERT, UPDATE` privileges.
  On this shared cPanel host the `alex` admin account cannot run
  `CREATE USER` directly — use the cPanel UAPI from SSH instead:
  ```bash
  ssh enviadores "uapi Mysql create_user name=wabridge password='<STRONG_PASSWORD>'"
  ssh enviadores "uapi Mysql set_privileges_on_database user=wabridge \
      database=env_producto privileges='SELECT,INSERT,UPDATE'"
  ```
  The resulting account is `wabridge@localhost` (no cPanel prefix on this
  host) with database-wide SELECT/INSERT/UPDATE on `env_producto`. The bridge
  only touches `wa_chats` and `wa_messages`, but per-table grants require
  privileges this account doesn't have.
- The PHP gateway needs `wa_media/` to be web-readable. By default Apache
  serves anything under `public_html/` — no extra config required. URLs are
  unguessable SHA-256 hashes, but if you want to harden, add `.htaccess` to
  block listing.

## Setup

```cmd
git clone https://github.com/<your-org>/enviadores-wabridge.git
cd enviadores-wabridge
go mod tidy
copy config.example.yaml config.yaml
notepad config.yaml   :: fill in SSH user, key path, MySQL password
go build -o wabridge.exe ./cmd/wabridge
```

## First run (QR pairing)

```cmd
wabridge.exe run
```

A QR code prints to stdout. Open WhatsApp on the phone holding the business
number → ⋮ → **Linked devices** → **Link a device** → scan. The bridge picks
up the session in `whatsmeow.db` and starts mirroring on the next message.

`Ctrl+C` to stop.

## Install as a Windows service (run on boot)

After the QR pairing has been done once and `whatsmeow.db` exists, install
the service:

```cmd
wabridge.exe install
wabridge.exe start
```

Service control:

```cmd
wabridge.exe stop
wabridge.exe start
wabridge.exe uninstall
```

Service logs land in the Windows Event Log under
`Enviadores WhatsApp Bridge`. For more verbose output, set
`whatsmeow.log_level: DEBUG` in `config.yaml` and run `wabridge.exe run`
in the foreground.

## Layout

```
cmd/wabridge/main.go          entry point + Windows service wiring
internal/config/              YAML loader
internal/tunnel/              SSH tunnel + SFTP client (one shared connection)
internal/store/               MySQL writes (wa_chats, wa_messages)
internal/media/               SFTP media upload + sha256 dedup
internal/wabridge/            whatsmeow event handler — the orchestration glue
```

## What this bridge does NOT do (yet)

- Send replies. Read-only by design — you reply from the phone.
- Group chat subjects. Only DM `display_name` is populated (from `PushName`).
- Forward profile-picture URLs through SFTP — they're stored as temporary
  WhatsApp signed URLs and may 404. The React UI falls back to initials.
- Backfill history older than what whatsmeow's history sync provides on
  pairing.

If any of those become important, they're small additions to `internal/wabridge`.

## Troubleshooting

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

`go.mod` lists `whatsmeow v0.0.0-...` as a placeholder; `go mod tidy` will
resolve to the current main. The library's API does change occasionally — if
a `go build` errors with method signature changes, check the
[whatsmeow changelog](https://github.com/tulir/whatsmeow/commits/main) and
update the small set of call sites in `internal/wabridge/wabridge.go`.
