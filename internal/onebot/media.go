package onebot

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/JFSAS/wechat-runtime/pkg/bot"
)

func fetchMedia(ctx context.Context, source string) ([]byte, error) {
	u, err := url.Parse(source)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Fragment != "" {
		return nil, errors.New("invalid_media_url")
	}
	request, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(request)
	if err != nil {
		return nil, errors.New("media_fetch_failed")
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return nil, errors.New("media_fetch_rejected")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, (16<<20)+1))
	if err != nil || len(body) == 0 || len(body) > 16<<20 {
		return nil, errors.New("invalid_media_size")
	}
	return body, nil
}
func (s *Server) mediaURL(account, conversation, file string) string {
	if s.Host == "" {
		return ""
	}
	q := url.Values{"account": {account}, "conversation": {conversation}, "file": {file}, "expires": {strconv.FormatInt(time.Now().Add(5*time.Minute).Unix(), 10)}}
	q.Set("signature", s.mediaSignature(q))
	return "http://" + s.Host + "/_media?" + q.Encode()
}
func (s *Server) mediaSignature(q url.Values) string {
	mac := hmac.New(sha256.New, []byte(s.Token))
	for _, key := range []string{"account", "conversation", "file", "expires"} {
		_, _ = mac.Write([]byte(key + "\x00" + q.Get(key) + "\x00"))
	}
	return hex.EncodeToString(mac.Sum(nil))
}
func (s *Server) serveMedia(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" || (s.Host != "" && r.Host != s.Host) {
		http.Error(w, "media_request_denied", 403)
		return
	}
	q := r.URL.Query()
	expiration, err := strconv.ParseInt(q.Get("expires"), 10, 64)
	now := time.Now().Unix()
	if err != nil || expiration < now || expiration > now+301 || s.Token == "" || !hmac.Equal([]byte(q.Get("signature")), []byte(s.mediaSignature(q))) {
		http.Error(w, "media_link_invalid", 403)
		return
	}
	if s.Runtime.Channel.Snapshot().Account != q.Get("account") {
		http.Error(w, "account_scope_changed", 409)
		return
	}
	select {
	case s.httpSlots <- struct{}{}:
		defer func() { <-s.httpSlots }()
	default:
		http.Error(w, "media_concurrency_limit", 503)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	stop := context.AfterFunc(s.shutdown, cancel)
	defer stop()
	content, err := s.Runtime.Channel.Download(ctx, q.Get("account"), q.Get("conversation"), q.Get("file"))
	if err != nil {
		if errors.Is(err, bot.ErrStorage) {
			http.Error(w, "runtime_storage_unavailable", 503)
			return
		}
		http.Error(w, "media_unavailable", 404)
		return
	}
	if len(content.Data) > 16<<20 {
		http.Error(w, "media_limit", 413)
		return
	}
	w.Header().Set("Content-Type", content.MIME)
	w.Header().Set("Content-Length", strconv.Itoa(len(content.Data)))
	_, _ = w.Write(content.Data)
}
func (r *Runtime) fileReference(account, file string) (string, string, error) {
	id, err := strconv.ParseInt(file, 10, 64)
	if err != nil {
		return "", "", errors.New("file_unavailable")
	}
	ref, ok, err := r.State.resolveChecked(account, "file", id)
	if err != nil {
		return "", "", errStateUnavailable
	}
	if !ok {
		return "", "", errors.New("file_unavailable")
	}
	conversation, source, ok := strings.Cut(ref.Key, "\x00")
	if !ok {
		return "", "", errors.New("file_unavailable")
	}
	return conversation, source, nil
}

func validUploadName(name string) bool {
	if name == "" || name == "." || name == ".." || len(name) > 255 || !utf8.ValidString(name) || strings.ContainsAny(name, "/\\") {
		return false
	}
	for _, r := range name {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf) {
			return false
		}
	}
	return true
}
