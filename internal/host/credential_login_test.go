package host

import (
	"bytes"
	"context"
	"io"
	"mime/multipart"
	"net"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/JFSAS/wispcat/internal/credentials"
	"github.com/JFSAS/wispcat/internal/native"
	"github.com/JFSAS/wispcat/internal/protocol"
)

func uploadBody(t *testing.T, file []byte, password string, extra string) ([]byte, string) {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", "login.wxlogin")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = part.Write(file)
	_ = writer.WriteField("passphrase", password)
	if extra != "" {
		_ = writer.WriteField(extra, "unexpected")
	}
	_ = writer.Close()
	return body.Bytes(), writer.FormDataContentType()
}

func TestManagementCredentialUploadQueuesOnlySelectedAccount(t *testing.T) {
	if !native.Supported() {
		t.Skip("native transport unavailable")
	}
	cfg := testConfig(t)
	h, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	// Bind an address for the Host guard without starting the real network
	// workers. This test can only enqueue; it cannot contact WeChat.
	h.listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	first, second := bytes.Repeat([]byte{7}, 24), []byte("synthetic-second")
	b := protocol.Bootstrap{Username: []byte("synthetic-owner"), UIN: 123, DeviceID: []byte("synthetic-device-000"), LocalID: 42, KeyInfo: []byte("synthetic-key-info"), First: first, Second: second, AAK: protocol.Join(protocol.Blob(1, protocol.Buffer(first)), protocol.Blob(2, protocol.Buffer(second)))}
	password := "synthetic-export-password"
	file, err := credentials.Seal(b, password)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, token, origin, host, password, extra string
		file                                       []byte
		code                                       int
	}{
		{name: "unauthorized", password: password, file: file, code: 401},
		{name: "wrong origin", token: "synthetic-admin-token", origin: "http://foreign.invalid", password: password, file: file, code: 403},
		{name: "wrong host", token: "synthetic-admin-token", host: "foreign.invalid", password: password, file: file, code: 403},
		{name: "wrong password", token: "synthetic-admin-token", password: "synthetic-wrong-password", file: file, code: 400},
		{name: "invalid file", token: "synthetic-admin-token", password: password, file: []byte("plaintext"), code: 400},
		{name: "duplicate", token: "synthetic-admin-token", password: password, file: file, extra: "file", code: 400},
		{name: "unknown field", token: "synthetic-admin-token", password: password, file: file, extra: "unexpected", code: 400},
		{name: "too large", token: "synthetic-admin-token", password: password, file: make([]byte, credentials.MaxSize+1), code: 400},
		{name: "accepted", token: "synthetic-admin-token", password: password, file: file, code: 202},
		{name: "busy", token: "synthetic-admin-token", password: password, file: file, code: 409},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, kind := uploadBody(t, tc.file, tc.password, tc.extra)
			r := httptest.NewRequest("POST", "http://"+h.Address()+"/accounts/first/login/import", bytes.NewReader(body))
			r.Header.Set("Content-Type", kind)
			if tc.token != "" {
				r.Header.Set("Authorization", "Bearer "+tc.token)
			}
			if tc.origin != "" {
				r.Header.Set("Origin", tc.origin)
			}
			if tc.host != "" {
				r.Host = tc.host
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.code {
				t.Fatal(w.Code, w.Body.String())
			}
			if strings.Contains(w.Body.String(), password) || strings.Contains(w.Body.String(), string(b.Username)) {
				t.Fatal("credentials disclosed in response")
			}
		})
	}
	if h.accounts[0].channel.Status()["state"] != "logging_in" || h.accounts[1].channel.Status()["state"] == "logging_in" {
		t.Fatal("wrong account queued")
	}
	for _, a := range h.accounts {
		status := a.channel.Status()
		if status["auth_calls"] != uint64(0) || status["sync_calls"] != uint64(0) || status["send_calls"] != uint64(0) {
			t.Fatal("HTTP handler performed protocol work")
		}
	}
}

func TestCredentialMultipartRejectsMissingFieldsAndTrailingOversize(t *testing.T) {
	for _, body := range []string{"not multipart", strings.Repeat("x", credentials.MaxSize+(16<<10)+1)} {
		r := httptest.NewRequest("POST", "http://127.0.0.1/", strings.NewReader(body))
		r.Header.Set("Content-Type", "multipart/form-data; boundary=synthetic")
		if _, err := readCredentialUpload(httptest.NewRecorder(), r); err == nil {
			t.Fatal("malformed upload accepted")
		}
	}
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	_ = w.WriteField("passphrase", "synthetic-export-password")
	_ = w.Close()
	r := httptest.NewRequest("POST", "http://127.0.0.1/", io.NopCloser(&body))
	r.Header.Set("Content-Type", w.FormDataContentType())
	if _, err := readCredentialUpload(httptest.NewRecorder(), r); err == nil {
		t.Fatal("missing file accepted")
	}
}
