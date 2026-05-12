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
	c.Whatsmeow.StorePath = ExpandHome(c.Whatsmeow.StorePath)
	return &c, nil
}
