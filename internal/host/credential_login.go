package host

import (
	"bytes"
	"io"
	"mime"
	"mime/multipart"
	"net/http"

	"github.com/JFSAS/wispcat/internal/credentials"
	"github.com/JFSAS/wispcat/internal/native"
	"github.com/JFSAS/wispcat/internal/protocol"
)

func readCredentialUpload(w http.ResponseWriter, r *http.Request) (protocol.Bootstrap, error) {
	var empty protocol.Bootstrap
	r.Body = http.MaxBytesReader(w, r.Body, credentials.MaxSize+(16<<10))
	defer r.Body.Close()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return empty, credentials.ErrFile
	}
	defer clear(body)
	kind, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || kind != "multipart/form-data" || params["boundary"] == "" {
		return empty, credentials.ErrFile
	}
	reader := multipart.NewReader(bytes.NewReader(body), params["boundary"])
	var file, password []byte
	defer func() { clear(file); clear(password) }()
	seen := map[string]bool{}
	for {
		part, err := reader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return empty, credentials.ErrFile
		}
		name := part.FormName()
		if seen[name] || (name != "file" && name != "passphrase") {
			part.Close()
			return empty, credentials.ErrFile
		}
		seen[name] = true
		limit := int64(credentials.MaxSize)
		if name == "passphrase" {
			limit = 4096
		}
		data, err := io.ReadAll(io.LimitReader(part, limit+1))
		part.Close()
		if err != nil || int64(len(data)) > limit {
			return empty, credentials.ErrFile
		}
		if name == "file" {
			file = data
		} else {
			password = data
		}
	}
	if !seen["file"] || !seen["passphrase"] {
		return empty, credentials.ErrFile
	}
	return credentials.Open(file, string(password))
}

func (h *Host) serveCredentialLogin(w http.ResponseWriter, r *http.Request, a *accountRuntime, reply func(int, any)) {
	if !native.Supported() {
		reply(409, map[string]string{"error": "native_transport_unavailable"})
		return
	}
	b, err := readCredentialUpload(w, r)
	if err != nil {
		reply(400, map[string]string{"error": "credential_file_invalid_or_password_incorrect"})
		return
	}
	if r.Context().Err() != nil {
		reply(408, map[string]string{"error": "upload_cancelled"})
		return
	}
	if err = a.channel.QueueCredentialLogin(b); err != nil {
		reply(409, map[string]string{"error": "credential_login_unavailable"})
		return
	}
	reply(202, map[string]string{"status": "queued"})
}
