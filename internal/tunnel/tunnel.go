// Package tunnel manages the SSH connection used for:
//   - A MySQL TCP port forward (local 127.0.0.1:<port> → enviadores:3306)
//   - SFTP uploads of WhatsApp media to the shared host
//
// One SSH connection is shared between both. If it drops, the caller should
// call Close() and re-establish the tunnel; wabridge does this with backoff
// in the main loop.
package tunnel

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/enviadores/wabridge/internal/config"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"
)

type Tunnel struct {
	cfg    *config.Config
	client *ssh.Client
	sftp   *sftp.Client

	listener net.Listener
	stop     chan struct{}
	wg       sync.WaitGroup
}

func New(cfg *config.Config) *Tunnel {
	return &Tunnel{cfg: cfg, stop: make(chan struct{})}
}

func (t *Tunnel) Open() error {
	key, err := os.ReadFile(t.cfg.SSH.PrivateKeyPath)
	if err != nil {
		return fmt.Errorf("read ssh key: %w", err)
	}

	var signer ssh.Signer
	if t.cfg.SSH.PrivateKeyPassphrase != "" {
		signer, err = ssh.ParsePrivateKeyWithPassphrase(key, []byte(t.cfg.SSH.PrivateKeyPassphrase))
	} else {
		signer, err = ssh.ParsePrivateKey(key)
	}
	if err != nil {
		return fmt.Errorf("parse ssh key: %w", err)
	}

	var hostKeyCallback ssh.HostKeyCallback
	if t.cfg.SSH.KnownHostsPath != "" {
		hostKeyCallback, err = knownhosts.New(t.cfg.SSH.KnownHostsPath)
		if err != nil {
			return fmt.Errorf("load known_hosts: %w", err)
		}
	} else {
		// First-run trust. Once paired, set known_hosts_path in config.yaml.
		hostKeyCallback = ssh.InsecureIgnoreHostKey()
	}

	sshCfg := &ssh.ClientConfig{
		User:            t.cfg.SSH.User,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: hostKeyCallback,
		Timeout:         15 * time.Second,
	}

	addr := net.JoinHostPort(t.cfg.SSH.Host, strconv.Itoa(t.cfg.SSH.Port))
	client, err := ssh.Dial("tcp", addr, sshCfg)
	if err != nil {
		return fmt.Errorf("ssh dial %s: %w", addr, err)
	}
	t.client = client

	// SFTP for media uploads.
	sftpClient, err := sftp.NewClient(client)
	if err != nil {
		_ = client.Close()
		return fmt.Errorf("sftp init: %w", err)
	}
	t.sftp = sftpClient

	// Local MySQL port forward.
	localAddr := fmt.Sprintf("127.0.0.1:%d", t.cfg.MySQL.LocalTunnelPort)
	ln, err := net.Listen("tcp", localAddr)
	if err != nil {
		_ = sftpClient.Close()
		_ = client.Close()
		return fmt.Errorf("listen %s: %w", localAddr, err)
	}
	t.listener = ln

	t.wg.Add(1)
	go t.acceptLoop()

	return nil
}

func (t *Tunnel) acceptLoop() {
	defer t.wg.Done()
	for {
		localConn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.stop:
				return
			default:
				if errors.Is(err, net.ErrClosed) {
					return
				}
				// transient error
				time.Sleep(200 * time.Millisecond)
				continue
			}
		}
		go t.handleConn(localConn)
	}
}

func (t *Tunnel) handleConn(local net.Conn) {
	defer local.Close()

	remote, err := t.client.Dial("tcp", "127.0.0.1:3306")
	if err != nil {
		return
	}
	defer remote.Close()

	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(remote, local); done <- struct{}{} }()
	go func() { _, _ = io.Copy(local, remote); done <- struct{}{} }()
	<-done
}

// SFTP returns the persistent SFTP client over the same SSH connection.
func (t *Tunnel) SFTP() *sftp.Client { return t.sftp }

// LocalMySQLAddr is the host:port the MySQL driver should connect to.
func (t *Tunnel) LocalMySQLAddr() string {
	return fmt.Sprintf("127.0.0.1:%d", t.cfg.MySQL.LocalTunnelPort)
}

// Close tears down the listener, SFTP client, and SSH client.
func (t *Tunnel) Close() error {
	close(t.stop)
	if t.listener != nil {
		_ = t.listener.Close()
	}
	if t.sftp != nil {
		_ = t.sftp.Close()
	}
	if t.client != nil {
		_ = t.client.Close()
	}
	t.wg.Wait()
	return nil
}

// Wait blocks until the SSH connection's underlying socket closes. Useful in
// the supervisor loop to trigger reconnect.
func (t *Tunnel) Wait() error {
	if t.client == nil {
		return errors.New("tunnel not open")
	}
	return t.client.Wait()
}
