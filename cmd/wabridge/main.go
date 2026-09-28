// wabridge — runs a whatsmeow client on the PDV0000001 office machine and
// mirrors incoming WhatsApp messages into env_producto over an SSH tunnel.
//
// Usage:
//   wabridge run             # run in foreground (use this for QR pairing)
//   wabridge install         # install as a Windows service
//   wabridge uninstall       # remove the service
//   wabridge start | stop    # service control
//   wabridge version         # print the build tag baked in by CI
//
// Configuration is read from ./config.yaml relative to the binary.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/enviadores/wabridge/internal/config"
	"github.com/enviadores/wabridge/internal/logfile"
	"github.com/enviadores/wabridge/internal/media"
	"github.com/enviadores/wabridge/internal/store"
	"github.com/enviadores/wabridge/internal/tunnel"
	"github.com/enviadores/wabridge/internal/wabridge"

	"github.com/kardianos/service"
)

// version is overridden at build time by CI with -ldflags
// "-X main.version=<git-tag>". An unstamped local build (go build ./...)
// keeps the "dev" sentinel so the version subcommand still works.
var version = "dev"

func main() {
	configPath := flag.String("config", "", "path to config.yaml (default: <exe dir>/config.yaml)")
	flag.Parse()

	// "version" is a config-free subcommand — report what's in the binary
	// without needing a tunnel target or even a config file to exist.
	if flag.Arg(0) == "version" {
		fmt.Println(version)
		return
	}

	cfgPath := *configPath
	if cfgPath == "" {
		exe, err := os.Executable()
		if err != nil {
			log.Fatalf("locate executable: %v", err)
		}
		cfgPath = filepath.Join(filepath.Dir(exe), "config.yaml")
	}

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	svcConfig := &service.Config{
		Name:        cfg.Service.Name,
		DisplayName: cfg.Service.DisplayName,
		Description: cfg.Service.Description,
		Arguments:   []string{"run"},
	}

	prg := &program{cfg: cfg}
	svc, err := service.New(prg, svcConfig)
	if err != nil {
		log.Fatalf("service.New: %v", err)
	}

	if len(flag.Args()) > 0 {
		cmd := flag.Arg(0)
		if cmd == "run" {
			// Foreground execution — bypasses kardianos so logs go straight to stdout
			// and the QR-code pairing flow is visible (and are teed to the log file).
			closeLog := startLogging(cfgPath, cfg, true)
			defer closeLog()
			runForeground(cfg)
			return
		}
		if err := service.Control(svc, cmd); err != nil {
			log.Fatalf("service %s: %v", cmd, err)
		}
		fmt.Printf("service: %s OK\n", cmd)
		return
	}

	// Invoked with no args: assume we're being launched by the Windows
	// service manager. Its stdout is discarded, so the log file is the only
	// record of why a session dropped.
	closeLog := startLogging(cfgPath, cfg, false)
	defer closeLog()
	if err := svc.Run(); err != nil {
		log.Fatalf("service run: %v", err)
	}
}

// startLogging sends all output to wabridge.log next to config.yaml. A
// failure to open it is logged and ignored — the bridge still runs.
func startLogging(cfgPath string, cfg *config.Config, tee bool) func() {
	closeLog, err := logfile.Setup(filepath.Join(filepath.Dir(cfgPath), logfile.FileName), tee)
	if err != nil {
		log.Printf("wabridge: log file unavailable: %v", err)
		closeLog = func() {}
	}
	log.Printf("wabridge %s starting - whatsmeow store: %s", version, cfg.Whatsmeow.StorePath)
	if cfg.Whatsmeow.LegacyStorePath {
		log.Printf("wabridge: using legacy store path relative to the working directory; move it next to the binary to silence this")
	}
	return closeLog
}

type program struct {
	cfg    *config.Config
	cancel context.CancelFunc
	done   chan struct{}
}

func (p *program) Start(s service.Service) error {
	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	p.done = make(chan struct{})
	go func() {
		defer close(p.done)
		supervise(ctx, p.cfg)
	}()
	return nil
}

func (p *program) Stop(s service.Service) error {
	if p.cancel != nil {
		p.cancel()
	}
	if p.done != nil {
		select {
		case <-p.done:
		case <-time.After(10 * time.Second):
		}
	}
	return nil
}

func runForeground(cfg *config.Config) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	supervise(ctx, cfg)
}

// supervise runs one attempt per loop iteration. If the tunnel or whatsmeow
// connection drops, we tear down and reconnect with exponential backoff.
func supervise(ctx context.Context, cfg *config.Config) {
	backoff := 2 * time.Second
	const maxBackoff = 60 * time.Second

	for {
		if ctx.Err() != nil {
			return
		}

		started := time.Now()
		err := runOnce(ctx, cfg)
		if ctx.Err() != nil {
			return
		}
		// An attempt that ran a while was healthy (or cycling QR batches);
		// don't make the next one wait out an old backoff.
		if time.Since(started) > 2*time.Minute {
			backoff = 2 * time.Second
		}
		log.Printf("wabridge: lost connection: %v - reconnect in %s", err, backoff)

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func runOnce(ctx context.Context, cfg *config.Config) error {
	// Scope every watcher goroutine below to this attempt. When runOnce
	// returns (a tunnel/whatsmeow drop), cancel() stops them all instead of
	// leaking them onto the long-lived supervise ctx — otherwise each retry
	// piles up more watchers that hot-loop on the now-closed DB.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tun := tunnel.New(cfg)
	if err := tun.Open(); err != nil {
		return fmt.Errorf("open tunnel: %w", err)
	}
	defer tun.Close()

	st, err := store.Open(ctx, cfg, tun.LocalMySQLAddr())
	if err != nil {
		return fmt.Errorf("open mysql via tunnel: %w", err)
	}
	defer st.Close()

	up := media.New(cfg, tun.SFTP())

	bridge, err := wabridge.New(ctx, cfg, st, up)
	if err != nil {
		return fmt.Errorf("init whatsmeow: %w", err)
	}
	defer bridge.Stop()

	if err := bridge.Start(ctx); err != nil {
		return fmt.Errorf("start whatsmeow: %w", err)
	}

	// Block until any of: tunnel dies, web UI requests reset, outbound
	// watcher dies, connection watchdog escalates, fatal bridge event
	// (e.g. LoggedOut), or we're cancelled.
	tunnelDone := make(chan error, 1)
	go func() { tunnelDone <- tun.Wait() }()

	resetDone := make(chan error, 1)
	go func() { resetDone <- bridge.WatchResetRequests(ctx) }()

	outboundDone := make(chan error, 1)
	go func() { outboundDone <- bridge.WatchOutbound(ctx) }()

	connDone := make(chan error, 1)
	go func() { connDone <- bridge.WatchConnection(ctx) }()

	picsDone := make(chan error, 1)
	go func() { picsDone <- bridge.WatchProfilePics(ctx) }()

	typingDone := make(chan error, 1)
	go func() { typingDone <- bridge.WatchTypingOutbound(ctx) }()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-tunnelDone:
		if err == nil {
			err = fmt.Errorf("ssh tunnel closed")
		}
		return err
	case err := <-resetDone:
		return err
	case err := <-outboundDone:
		return err
	case err := <-connDone:
		return err
	case err := <-picsDone:
		return err
	case err := <-typingDone:
		return err
	case err := <-bridge.Fatal():
		return err
	}
}
