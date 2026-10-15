// Package messaging defines the account-scoped contract of a message channel.
// It contains no model configuration, reply policy or provider implementation.
package messaging

import (
	"context"
	"encoding/json"
	"errors"
)

var (
	ErrUnavailable   = errors.New("channel_unavailable")
	ErrScopeChanged  = errors.New("account_scope_changed")
	ErrCursorExpired = errors.New("event_cursor_expired")
	ErrConflict      = errors.New("idempotency_conflict")
	ErrCancelled     = errors.New("delivery_cancelled")
)

type Snapshot struct {
	FirstCursor uint64 `json:"first_cursor"`
	Account     string `json:"account_id"`
	Online      bool   `json:"online"`
	// Recovering permits an in-flight consumer to finish and save its work.
	// It does not permit sending or starting a new generation.
	Recovering bool   `json:"recovering"`
	Epoch      uint64 `json:"epoch"`
	Cursor     uint64 `json:"cursor"`
}

type Event struct {
	Sequence     uint64 `json:"sequence"`
	ID           string `json:"event_id"`
	Account      string `json:"account_id"`
	Conversation string `json:"conversation_id"`
	Message      string `json:"message_id"`
	Kind         string `json:"conversation_kind"`
	Sender       string `json:"sender_id,omitempty"`
	SenderLabel  string `json:"sender_label,omitempty"`
	Timestamp    uint32 `json:"timestamp"`
	Text         string `json:"text"`
	MediaVariant string `json:"media_variant,omitempty"`
	Own          bool   `json:"own"`
	Addressed    bool   `json:"addressed"`
	LocalEcho    bool   `json:"local_echo"`
	Mentioned    bool   `json:"mentioned_self"`
}

type Conversation struct {
	ID       string
	Label    string
	Nickname string
	Kind     string
}

type Content struct {
	Data             []byte
	MIME, Name, Kind string
}

func (Content) String() string     { return "messaging.Content(redacted)" }
func (c Content) GoString() string { return c.String() }

type Artifact struct {
	CacheID string `json:"cache_id"`
	Name    string `json:"name"`
	MIME    string `json:"mime"`
	Size    int64  `json:"size"`
	Digest  string `json:"digest"`
}

type Output struct {
	Text     string    `json:"text,omitempty"`
	Artifact *Artifact `json:"artifact,omitempty"`
}

type SendRequest struct {
	Account      string   `json:"account_id"`
	Conversation string   `json:"conversation_id"`
	Key          string   `json:"idempotency_key"`
	Scope        string   `json:"dispatch_scope,omitempty"`
	Generation   uint64   `json:"generation,omitempty"`
	Epoch        uint64   `json:"epoch"`
	Source       string   `json:"-"`
	Outputs      []Output `json:"outputs"`
}

type DeliveryBatch struct {
	Fingerprint string   `json:"fingerprint"`
	IDs         []string `json:"ids"`
}

// Channel supports both an in-process scheduler and independent HTTP consumers.
// State is an opaque encrypted consumer checkpoint; the channel never interprets it.
type Channel interface {
	Snapshot() Snapshot
	Events(account string, after uint64, limit int) ([]Event, error)
	Conversations() []Conversation
	BackupState(account, label string) error
	ReadState(account, consumer string) (json.RawMessage, error)
	WriteState(account, consumer string, state json.RawMessage) error
	ResolveLegacyConversation(account, peer string) (string, error)
	Download(account, conversation, message, variant string) (string, error)
	DownloadStatus(account, id string) (string, string)
	Content(account, id string) (Content, error)
	StoreArtifacts(account, key string, content []Content) ([]Artifact, error)
	ReleaseArtifacts(account string, artifacts []Artifact) error
	Send(context.Context, SendRequest) ([]string, error)
	CancelScope(account, scope, conversation string, generation uint64) (uint64, error)
	AdoptDeliveries(account, scope, conversation string, generation uint64, ids []string, cancelPending bool) error
	Deliveries() []map[string]any
}
