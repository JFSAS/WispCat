package onebot

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/JFSAS/wechat-runtime/pkg/bot"
)

type PostConfig struct{ Name, URL, Secret string }

// RunPost reports each event independently of other consumers. A successful
// HTTP response is a transport acknowledgement, not proof of bot processing.
func (s *Server) RunPost(parent context.Context, cfg PostConfig) (runErr error) {
	u, err := url.Parse(cfg.URL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || cfg.Name == "" {
		return errors.New("invalid_http_post_configuration")
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	stop := context.AfterFunc(s.shutdown, cancel)
	defer stop()
	s.mu.Lock()
	if _, exists := s.statuses[cfg.Name]; exists || s.closing {
		s.mu.Unlock()
		return errors.New("duplicate_or_closed_connection")
	}
	s.statuses[cfg.Name] = ConnectionStatus{Name: cfg.Name}
	s.wg.Add(1)
	s.mu.Unlock()
	defer s.wg.Done()
	defer func() { s.finish(cfg.Name, "httpClient", runErr) }()
	client := &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	account := ""
	cursor := uint64(0)
	self := int64(0)
	heartbeat := time.NewTicker(15 * time.Second)
	defer heartbeat.Stop()
	for {
		if ctx.Err() != nil {
			return nil
		}
		snap := s.Runtime.Channel.Snapshot()
		if snap.Account == "" {
			s.status(cfg.Name, "httpClient", "waiting_account", "", false)
			if !waitContext(ctx, time.Second) {
				return nil
			}
			continue
		}
		if snap.Account != account {
			account = snap.Account
			self, err = s.Runtime.State.ID(account, "user", snap.Self.ID)
			if err != nil {
				return err
			}
			cursor = s.Runtime.State.cursor(account, cfg.Name, snap.Cursor)
			if err = s.Runtime.State.setCursor(account, cfg.Name, cursor); err != nil {
				return err
			}
			_, err = postJSON(ctx, client, cfg, self, map[string]any{"time": time.Now().Unix(), "self_id": self, "post_type": "meta_event", "meta_event_type": "lifecycle", "sub_type": "enable"})
			if err != nil {
				s.status(cfg.Name, "httpClient", "degraded", "http_report_failed", false)
			}
		}
		events, e := s.Runtime.Channel.Events(account, cursor, 100)
		if e != nil {
			s.status(cfg.Name, "httpClient", "degraded", "event_cursor_unavailable", false)
			return e
		}
		failed := false
		for _, ev := range events {
			if !ev.LocalEcho && (!ev.Own || ev.Conversation.Kind == "filehelper") {
				value, e := s.Runtime.Convert(ev)
				if e != nil {
					return e
				}
				body, e := postJSON(ctx, client, cfg, self, value)
				if e != nil {
					s.status(cfg.Name, "httpClient", "degraded", "http_report_failed", true)
					failed = true
					break
				}
				s.status(cfg.Name, "httpClient", "connected", "", false)
				// Checkpoint the reported event before a quick operation. Its send
				// has a separate durable idempotency key, so reconnect cannot replay it.
				if e = s.Runtime.State.setCursor(account, cfg.Name, ev.Sequence); e != nil {
					return e
				}
				cursor = ev.Sequence
				if len(bytes.TrimSpace(body)) > 0 {
					if e = s.quickReply(ctx, cfg.Name, ev, value, body); e != nil {
						s.status(cfg.Name, "httpClient", "degraded", "quick_operation_failed", false)
					}
				}
			} else {
				cursor = ev.Sequence
				if e = s.Runtime.State.setCursor(account, cfg.Name, cursor); e != nil {
					return e
				}
			}
		}
		if failed {
			if !waitContext(ctx, time.Second) {
				return nil
			}
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-heartbeat.C:
			_, e := postJSON(ctx, client, cfg, self, map[string]any{"time": time.Now().Unix(), "self_id": self, "post_type": "meta_event", "meta_event_type": "heartbeat", "interval": 15000, "status": map[string]bool{"online": snap.Online, "good": snap.Online}})
			if e != nil {
				s.status(cfg.Name, "httpClient", "degraded", "heartbeat_report_failed", false)
			}
		case <-time.After(100 * time.Millisecond):
		}
	}
}
func postJSON(ctx context.Context, client *http.Client, cfg PostConfig, self int64, value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	q, err := http.NewRequestWithContext(ctx, "POST", cfg.URL, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	q.Header.Set("Content-Type", "application/json")
	q.Header.Set("X-Self-ID", strconv.FormatInt(self, 10))
	q.Header.Set("User-Agent", "OneBot/11")
	if cfg.Secret != "" {
		mac := hmac.New(sha1.New, []byte(cfg.Secret))
		_, _ = mac.Write(raw)
		q.Header.Set("X-Signature", "sha1="+hex.EncodeToString(mac.Sum(nil)))
	}
	res, err := client.Do(q)
	if err != nil {
		return nil, errors.New("http_report_failed")
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return nil, errors.New("http_report_rejected")
	}
	body, err := io.ReadAll(io.LimitReader(res.Body, (64<<10)+1))
	if err != nil || len(body) > 64<<10 {
		return nil, errors.New("quick_operation_too_large")
	}
	return body, nil
}
func (s *Server) quickReply(ctx context.Context, name string, ev bot.Event, event map[string]any, body []byte) error {
	var operation map[string]json.RawMessage
	if json.Unmarshal(body, &operation) != nil {
		return errors.New("invalid_quick_operation")
	}
	for key := range operation {
		if key != "reply" && key != "auto_escape" && key != "at_sender" {
			return bot.ErrUnsupported
		}
	}
	reply, ok := operation["reply"]
	if !ok {
		return nil
	}
	params := map[string]json.RawMessage{"message": reply}
	if auto, ok := operation["auto_escape"]; ok {
		params["auto_escape"] = auto
	}
	action := "send_private_msg"
	if ev.Conversation.Kind == "group" {
		action = "send_group_msg"
		params["group_id"], _ = json.Marshal(event["group_id"])
		at := true
		if raw, ok := operation["at_sender"]; ok {
			if json.Unmarshal(raw, &at) != nil {
				return errors.New("invalid_at_sender")
			}
		}
		if at {
			auto, err := paramBool(params, "auto_escape")
			if err != nil {
				return err
			}
			segments, err := parseMessage(reply, auto)
			if err != nil {
				return err
			}
			segments = append([]bot.Segment{{Type: "at", Data: map[string]string{"qq": strconv.FormatInt(event["user_id"].(int64), 10)}}}, segments...)
			params["message"], _ = json.Marshal(segments)
		}
	} else {
		params["user_id"], _ = json.Marshal(event["user_id"])
	}
	hash := sha256.Sum256([]byte(name + "\x00" + ev.ID))
	params["wechat_request_id"], _ = json.Marshal("quick/" + hex.EncodeToString(hash[:]))
	result := s.Runtime.Invoke(ctx, Request{Action: action, Params: params})
	if result.Retcode != 0 {
		return errors.New("quick_reply_failed")
	}
	return nil
}
