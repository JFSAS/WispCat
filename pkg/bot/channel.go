// Package bot is the platform-neutral interface of a message runtime.
// Callers never receive credentials, protocol keys or raw platform identities.
package bot

import (
	"context"
	"errors"
)

var (
	ErrUnavailable = errors.New("channel_unavailable")
	ErrScope       = errors.New("account_scope_changed")
	ErrCursor      = errors.New("event_cursor_expired")
	ErrUnsupported = errors.New("unsupported_operation")
	ErrConflict    = errors.New("idempotency_conflict")
	ErrNotFound    = errors.New("not_found")
	ErrStorage     = errors.New("runtime_storage_unavailable")
)

type User struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}
type Conversation struct {
	ID   string `json:"id"`
	Kind string `json:"kind"`
	Name string `json:"name"`
}
type Roster struct {
	Users    []User `json:"users"`
	Complete bool   `json:"complete"`
	Source   string `json:"source"`
}
type Snapshot struct {
	Account     string `json:"account_id"`
	Self        User   `json:"self"`
	Online      bool   `json:"online"`
	State       string `json:"state"`
	Epoch       uint64 `json:"epoch"`
	Cursor      uint64 `json:"cursor"`
	FirstCursor uint64 `json:"first_cursor"`
}
type Segment struct {
	Type string            `json:"type"`
	Data map[string]string `json:"data"`
}
type Event struct {
	ID           string       `json:"event_id"`
	Sequence     uint64       `json:"sequence"`
	Account      string       `json:"account_id"`
	Conversation Conversation `json:"conversation"`
	Sender       User         `json:"sender"`
	Time         int64        `json:"time"`
	Message      []Segment    `json:"message"`
	Own          bool         `json:"own"`
	LocalEcho    bool         `json:"local_echo"`
}
type Upload struct {
	Kind, Name string
	Data       []byte
}
type Output struct {
	Text     string
	Mentions []string
	Upload   *Upload
}
type Send struct {
	Account      string
	Epoch        uint64
	Conversation string
	Key          string
	Text         string
	Mentions     []string
	Upload       *Upload
	Outputs      []Output `json:"outputs,omitempty"`
}
type Delivery struct {
	ID           string     `json:"id"`
	MessageID    string     `json:"message_id,omitempty"`
	Message      []Segment  `json:"message,omitempty"`
	State        string     `json:"state"`
	Error        string     `json:"error,omitempty"`
	Conversation string     `json:"conversation_id"`
	Time         int64      `json:"time"`
	Text         string     `json:"text,omitempty"`
	Parts        []Delivery `json:"parts,omitempty"`
}
type Content struct {
	Data       []byte
	MIME, Name string
}

// Channel owns the facts of one account. Submit durably queues an operation;
// accepted requires a verified platform receipt, unknown must never be retried
// automatically. Key is idempotent within Account, independently of Epoch.
type Channel interface {
	Snapshot() Snapshot
	Users() []User
	Members(account, conversation string) (Roster, error)
	Conversations() []Conversation
	Events(account string, after uint64, limit int) ([]Event, error)
	Message(account, id string) (Event, error)
	Submit(context.Context, Send) (Delivery, error)
	Delivery(account, id string) (Delivery, error)
	Download(context.Context, string, string, string) (Content, error)
}
