//go:build integrationfixtures

// This is a disposable deployment fixture, never a production entry point.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/JFSAS/wechat-runtime/internal/host"
)

const token = "synthetic-deployment-control-token"

var deploymentSourceSHA string

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	reverse := flag.String("reverse", "", "numeric loopback framework websocket")
	root := flag.String("state-dir", "", "marked disposable state root")
	metadata := flag.Bool("metadata", false, "print build input binding only")
	flag.Parse()
	if len(deploymentSourceSHA) != 64 || strings.Trim(deploymentSourceSHA, "0123456789abcdef") != "" {
		return errors.New("deployment_fixture_build_source_stamp_required")
	}
	if *metadata {
		return json.NewEncoder(os.Stdout).Encode(map[string]string{"source_manifest_sha256": deploymentSourceSHA})
	}
	u, err := url.Parse(*reverse)
	if err != nil || u.Scheme != "ws" || u.User != nil || net.ParseIP(u.Hostname()) == nil || !net.ParseIP(u.Hostname()).IsLoopback() || *root == "" {
		return errors.New("deployment_fixture_requires_loopback_and_private_root")
	}
	c := host.Config{StateDir: *root, Listen: "127.0.0.1:0", TokenEnv: "DEPLOYMENT_ADMIN_TOKEN", Accounts: []host.Account{}}
	for _, name := range []string{"synthetic-a", "synthetic-b"} {
		c.Accounts = append(c.Accounts, host.Account{Name: name, Instance: "instance:deployment-" + name, Mode: "offline", Listen: "127.0.0.1:0", TokenEnv: "DEPLOYMENT_BOT_TOKEN", Reverse: []host.Reverse{{Name: "framework", URL: u.String(), TokenEnv: "DEPLOYMENT_REVERSE_TOKEN", Role: "Universal"}}})
	}
	h, err := host.NewDeploymentFixture(c)
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	defer h.Close(context.Background())
	if err = h.Start(ctx); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("Authorization") != "Bearer "+token {
			w.WriteHeader(401)
			return
		}
		if r.URL.Path == "/fixture/identity" && r.Method == "GET" {
			_ = json.NewEncoder(w).Encode(map[string]string{"source_manifest_sha256": deploymentSourceSHA})
			return
		}
		if r.URL.Path == "/fixture/inspect" && r.Method == "GET" {
			value, err := h.DeploymentInspect(r.URL.Query().Get("account"))
			if err != nil {
				w.WriteHeader(409)
				_ = json.NewEncoder(w).Encode(map[string]string{"error": "fixture_account_unavailable"})
				return
			}
			_ = json.NewEncoder(w).Encode(value)
			return
		}
		if r.URL.Path != "/fixture/inject" || r.Method != "POST" {
			w.WriteHeader(404)
			return
		}
		var q struct {
			Account   string `json:"account"`
			Kind      string `json:"kind"`
			Text      string `json:"text"`
			Mentioned bool   `json:"mentioned"`
			Media     string `json:"media,omitempty"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, 4096)
		defer r.Body.Close()
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if decoder.Decode(&q) != nil || decoder.Decode(new(any)) != io.EOF {
			w.WriteHeader(400)
			return
		}
		var err error
		if q.Media != "" {
			err = h.DeploymentInjectMedia(q.Account, q.Kind, q.Media)
		} else {
			err = h.DeploymentInject(q.Account, q.Kind, q.Text, q.Mentioned)
		}
		if err != nil {
			w.WriteHeader(409)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "fixture_injection_failed"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]bool{"injected": true})
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, MaxHeaderBytes: 8192}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	fmt.Println("control: http://" + listener.Addr().String())
	fmt.Println("management: http://" + h.Address())
	select {
	case <-ctx.Done():
	case err = <-served:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	stop, done := context.WithTimeout(context.Background(), 15*time.Second)
	defer done()
	if err = server.Shutdown(stop); err != nil {
		return err
	}
	if err = h.Close(stop); err != nil {
		return err
	}
	if strings.TrimSpace(os.Getenv("DEPLOYMENT_ADMIN_TOKEN")) == "" {
		return errors.New("fixture_admin_environment_lost")
	}
	return nil
}
