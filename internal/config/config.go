package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// ExpandHome turns a leading "~" into the current user's home directory.
// Works on macOS, Linux, and Windows; no-op for absolute or bare paths.
func ExpandHome(p string) string {
	if p == "" || !strings.HasPrefix(p, "~") {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return p
	}
	if p == "~" {
		return home
	}
	if strings.HasPrefix(p, "~/") || strings.HasPrefix(p, `~\`) {
		return filepath.Join(home, p[2:])
	}
	return p
}

type Config struct {
	SSH       SSHConfig       `yaml:"ssh"`
	MySQL     MySQLConfig     `yaml:"mysql"`
	Media     MediaConfig     `yaml:"media"`
	Whatsmeow WhatsmeowConfig `yaml:"whatsmeow"`
	Service   ServiceConfig   `yaml:"service"`
}

type SSHConfig struct {
	Host                 string `yaml:"host"`
	Port                 int    `yaml:"port"`
	User                 string `yaml:"user"`
	PrivateKeyPath       string `yaml:"private_key_path"`
	PrivateKeyPassphrase string `yaml:"private_key_passphrase"`
	KnownHostsPath       string `yaml:"known_hosts_path"`
}

type MySQLConfig struct {
	User            string `yaml:"user"`
	Password        string `yaml:"password"`
	Database        string `yaml:"database"`
	LocalTunnelPort int    `yaml:"local_tunnel_port"`
}

type MediaConfig struct {
	RemotePath    string `yaml:"remote_path"`
	PublicBaseURL string `yaml:"public_base_url"`
}

type WhatsmeowConfig struct {
	StorePath string `yaml:"store_path"`
	LogLevel  string `yaml:"log_level"`

	// LegacyStorePath is set when a relative store_path was found relative
	// to the working directory instead of next to config.yaml (see
	// resolveStorePath).
	LegacyStorePath bool `yaml:"-"`
}

type ServiceConfig struct {
	Name        string `yaml:"name"`
	DisplayName string `yaml:"display_name"`
	Description string `yaml:"description"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if c.SSH.Port == 0 {
		c.SSH.Port = 22
	}
	if c.MySQL.LocalTunnelPort == 0 {
		c.MySQL.LocalTunnelPort = 53306
	}
	if c.Whatsmeow.StorePath == "" {
		c.Whatsmeow.StorePath = "whatsmeow.db"
	}
	if c.Whatsmeow.LogLevel == "" {
		c.Whatsmeow.LogLevel = "INFO"
	}
	c.SSH.PrivateKeyPath = ExpandHome(c.SSH.PrivateKeyPath)
	c.SSH.KnownHostsPath = ExpandHome(c.SSH.KnownHostsPath)
	c.Whatsmeow.StorePath, c.Whatsmeow.LegacyStorePath =
		resolveStorePath(ExpandHome(c.Whatsmeow.StorePath), filepath.Dir(path))
	return &c, nil
}

// resolveStorePath anchors a relative store_path to the config directory.
// It used to be resolved against the working directory, which differs
// between `wabridge run` (the install dir) and the Windows service
// (C:\Windows\System32) — so the two modes silently used different session
// databases. An existing database at the old working-directory location is
// still honoured when none exists next to the config, so upgrading doesn't
// unlink a paired device.
func resolveStorePath(p, baseDir string) (string, bool) {
	if filepath.IsAbs(p) {
		return p, false
	}
	if abs, err := filepath.Abs(baseDir); err == nil {
		baseDir = abs
	}
	anchored := filepath.Join(baseDir, p)
	if fileExists(anchored) {
		return anchored, false
	}
	if legacy, err := filepath.Abs(p); err == nil && legacy != anchored && fileExists(legacy) {
		return legacy, true
	}
	return anchored, false
}

func fileExists(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && !fi.IsDir()
}
