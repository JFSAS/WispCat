// Package client calls a separately running OneBot implementation. It imports
// no protocol, native transport, login, storage or model implementation.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Client struct {
	base  *url.URL
	token string
	http  *http.Client
}
type Response struct {
	Status  string          `json:"status"`
	Retcode int             `json:"retcode"`
	Data    json.RawMessage `json:"data"`
	Message string          `json:"message,omitempty"`
	Wording string          `json:"wording,omitempty"`
	Stream  string          `json:"stream,omitempty"`
}
type ActionError struct{ Response Response }

func (e *ActionError) Error() string { return "onebot_action_failed: " + e.Response.Message }
func New(endpoint, token string) (*Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, errors.New("invalid_onebot_endpoint")
	}
	if token == "" || strings.ContainsAny(token, "\r\n") {
		return nil, errors.New("invalid_access_token")
	}
	u.Path = strings.TrimRight(u.Path, "/")
	return &Client{base: u, token: token, http: &http.Client{Timeout: 40 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (c *Client) Call(ctx context.Context, action string, params any) (Response, error) {
	var result Response
	response, err := c.doAction(ctx, action, params)
	if err != nil {
		return result, err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, (24<<20)+1))
	if err != nil || len(raw) > 24<<20 || json.Unmarshal(raw, &result) != nil {
		return result, errors.New("invalid_onebot_response")
	}
	if result.Status != "ok" || result.Retcode != 0 {
		return result, &ActionError{Response: result}
	}
	return result, nil
}
func (c *Client) doAction(ctx context.Context, action string, params any) (*http.Response, error) {
	if action == "" || strings.ContainsAny(action, "/\\?#\r\n") {
		return nil, errors.New("invalid_action_name")
	}
	body, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	if params == nil || bytes.Equal(bytes.TrimSpace(body), []byte("null")) {
		body = []byte("{}")
	}
	u := *c.base
	u.Path += "/" + action
	q, err := http.NewRequestWithContext(ctx, "POST", u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	q.Header.Set("Authorization", "Bearer "+c.token)
	q.Header.Set("Content-Type", "application/json")
	response, err := c.http.Do(q)
	if err != nil {
		return nil, errors.New("onebot_transport_failed")
	}
	if response.StatusCode != 200 {
		_ = response.Body.Close()
		return nil, errors.New("onebot_http_failed")
	}
	return response, nil
}
