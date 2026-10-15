//go:build (darwin || linux) && cgo

package native

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func testCertificate(t *testing.T) (tls.Certificate, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
func TestOpenSSLExchangeAndCertificateVerification(t *testing.T) {
	for _, valid := range []bool{true, false} {
		t.Run(map[bool]string{true: "trusted", false: "untrusted"}[valid], func(t *testing.T) {
			certificate, roots := testCertificate(t)
			if !valid {
				_, roots = testCertificate(t)
			}
			path := filepath.Join(t.TempDir(), "roots.pem")
			if err := os.WriteFile(path, roots, 0600); err != nil {
				t.Fatal(err)
			}
			listener, err := tls.Listen("tcp4", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13})
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			done := make(chan struct{})
			go func() {
				defer close(done)
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				data := make([]byte, 4)
				if _, err = io.ReadFull(conn, data); err == nil && string(data) == "ping" {
					_, _ = conn.Write([]byte("pong"))
				}
			}()
			_, port, _ := net.SplitHostPort(listener.Addr().String())
			result, err := exchange(context.Background(), "localhost", port, path, []byte("ping"))
			if valid {
				if err != nil || string(result) != "pong" {
					t.Fatal("native TLS exchange failed", err)
				}
			} else {
				var diagnostic *ExchangeError
				if !errors.As(err, &diagnostic) || diagnostic.Stage != "verify_certificate" {
					t.Fatal("untrusted server accepted", err)
				}
			}
			select {
			case <-done:
			case <-time.After(6 * time.Second):
				t.Fatal("test peer did not close")
			}
		})
	}
}
func TestCancelledNativeExchangeDoesNotConnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Exchange(ctx, "localhost", "unused", []byte("request")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
