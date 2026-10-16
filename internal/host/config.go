package host

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/JFSAS/wechat-runtime/internal/instances"
)

type Reverse struct {
	Name     string `json:"name"`
	URL      string `json:"url"`
	TokenEnv string `json:"token_env"`
	Role     string `json:"role,omitempty"`
}
type Post struct {
	Name      string `json:"name"`
	URL       string `json:"url"`
	SecretEnv string `json:"secret_env"`
}
type Account struct {
	Name     string    `json:"name"`
	Instance string    `json:"instance"`
	Mode     string    `json:"mode"`
	Listen   string    `json:"listen"`
	TokenEnv string    `json:"token_env"`
	Reverse  []Reverse `json:"reverse,omitempty"`
	HTTPPost []Post    `json:"http_post,omitempty"`
}
type Config struct {
	StateDir string    `json:"state_dir"`
	Listen   string    `json:"management_listen"`
	TokenEnv string    `json:"management_token_env"`
	Accounts []Account `json:"accounts"`
}

func ReadConfig(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer file.Close()
	var c Config
	raw, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil || len(raw) > 1<<20 {
		return c, errors.New("runtime_config_too_large")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&c) != nil || decoder.Decode(new(any)) != io.EOF {
		return c, errors.New("invalid_runtime_config")
	}
	return effectiveConfig(c)
}
func validName(name string) bool {
	return name != "" && len(name) <= 64 && strings.Trim(name, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_") == ""
}
func loopbackListen(address string) bool {
	host, port, err := net.SplitHostPort(address)
	number, portErr := strconv.ParseUint(port, 10, 16)
	return err == nil && portErr == nil && number <= 65535 && net.ParseIP(host) != nil && net.ParseIP(host).IsLoopback()
}
func token(name string) (string, error) {
	if name == "" {
		return "", errors.New("token_env_required")
	}
	value := os.Getenv(name)
	if len(value) < 16 || len(value) > 4096 || strings.ContainsAny(value, "\r\n") {
		return "", errors.New("token_env_unavailable_or_invalid")
	}
	return value, nil
}
func (c Config) Validate() error {
	return c.validate(true)
}
func (c Config) validate(networks bool) error {
	if c.StateDir == "" || !loopbackListen(c.Listen) || len(c.Accounts) < 1 || len(c.Accounts) > 16 {
		return errors.New("invalid_runtime_topology")
	}
	if _, err := token(c.TokenEnv); err != nil {
		return err
	}
	names := map[string]bool{}
	keys := map[string]bool{}
	addresses := map[string]bool{c.Listen: true}
	for _, a := range c.Accounts {
		key, err := instances.NormalizeKey(a.Instance)
		if !validName(a.Name) || err != nil || names[a.Name] || keys[key] || !loopbackListen(a.Listen) || (a.Mode != "offline" && a.Mode != "online") {
			return errors.New("invalid_account_configuration")
		}
		names[a.Name] = true
		keys[key] = true
		_, port, _ := net.SplitHostPort(a.Listen)
		if addresses[a.Listen] && port != "0" {
			return errors.New("duplicate_listener")
		}
		addresses[a.Listen] = true
		if _, err := token(a.TokenEnv); err != nil {
			return err
		}
		if !networks {
			continue
		}
		endpoints := map[string]bool{}
		if len(a.HTTPPost) > 16 {
			return errors.New("connection_limit")
		}
		for _, p := range a.HTTPPost {
			u, e := url.Parse(p.URL)
			if !validName(p.Name) || endpoints[p.Name] || e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.Fragment != "" {
				return errors.New("invalid_http_post_configuration")
			}
			if _, e = token(p.SecretEnv); e != nil {
				return e
			}
			endpoints[p.Name] = true
		}
		if len(a.Reverse) > 16 {
			return errors.New("connection_limit")
		}
		for _, r := range a.Reverse {
			u, e := url.Parse(r.URL)
			if !validName(r.Name) || endpoints[r.Name] || e != nil || u.Host == "" || (u.Scheme != "ws" && u.Scheme != "wss") || u.User != nil || u.Fragment != "" || (r.Role != "" && r.Role != "Universal" && r.Role != "Event" && r.Role != "API") {
				return errors.New("invalid_reverse_configuration")
			}
			if _, e = token(r.TokenEnv); e != nil {
				return e
			}
			endpoints[r.Name] = true
		}
	}
	return nil
}
