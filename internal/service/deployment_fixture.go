//go:build integrationfixtures

package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/JFSAS/wispcat/internal/protocol"
	"github.com/JFSAS/wispcat/internal/store"
)

// DeploymentDriver is compiled only into explicitly tagged offline fixtures.
// It owns real Service state/history/queues, but NEVER calls Service.Start,
// Authenticate, Sync or text transport. Terminal receipts are synthetic facts.
type DeploymentDriver struct {
	service            *Service
	label              string
	done               chan struct{}
	mu                 sync.Mutex
	err                error
	sends              []DeploymentSend
	generation         string
	pendingMedia       map[string]DeploymentSend
	downloads, uploads uint64
}

type DeploymentObservation struct {
	Generation string           `json:"generation"`
	Sends      []DeploymentSend `json:"sends"`
	Error      string           `json:"error"`
	Downloads  uint64           `json:"synthetic_downloads"`
	Uploads    uint64           `json:"synthetic_uploads"`
}

type DeploymentSend struct {
	ID           string   `json:"id"`
	Text         string   `json:"text"`
	Kind         string   `json:"kind"`
	Mentions     []string `json:"mentions"`
	ServerID     uint64   `json:"synthetic_server_id"`
	Media        string   `json:"media,omitempty"`
	PayloadBytes int      `json:"payload_bytes,omitempty"`
	PayloadSHA   string   `json:"payload_sha256,omitempty"`
	EncodedBytes int      `json:"encoded_bytes,omitempty"`
	EncodedSHA   string   `json:"encoded_sha256,omitempty"`
}

func NewDeploymentDriver(s *Service, label string) (*DeploymentDriver, error) {
	if !strings.HasPrefix(label, "synthetic-") || len(label) > 48 || s.source.App != "instance:deployment-"+label {
		return nil, errors.New("deployment_fixture_requires_synthetic_instance")
	}
	seed := sha256.Sum256([]byte("wechat-runtime/offline-deployment/" + label))
	bootstrap := protocol.Bootstrap{Username: []byte("fixture-owner-" + label), UIN: 8000 + uint32(seed[0]), DeviceID: bytes.Clone(seed[:16]), AAK: []byte("synthetic-not-login-material")}
	if err := ValidateDeploymentDirectory(s.directory, label); err != nil {
		return nil, err
	}
	_, exists, err := deploymentCheckpoint(filepath.Join(s.directory, "checkpoints"))
	if err != nil {
		return nil, err
	}
	if !exists {
		s.data = &store.Data{Schema: 1, Binding: bootstrap.Binding(), SourceApp: s.source.App, APIKey: bytes.Clone(seed[:]), NextClientID: 1, Jobs: map[string]*store.Job{}, Session: &protocol.Session{Bootstrap: bootstrap, Original: bytes.Repeat([]byte{2}, 16), ClientKey: bytes.Repeat([]byte{3}, 24), ServerKey: bytes.Repeat([]byte{4}, 24), Profile: protocol.MacCompatibilityProfile()}}
		s.checkpoint = store.Checkpoint{Directory: filepath.Join(s.directory, "checkpoints"), Binding: s.data.Binding}
		if err := s.saveLocked(); err != nil {
			return nil, err
		}
	}
	if err := s.prepare(false, false); err != nil {
		return nil, err
	}
	s.state, s.mode, s.sessionState = "connected", "online", "validated"
	friend, room := "Synthetic Friend "+label, "Synthetic Room "+label
	s.ingestLocked(protocol.SyncResult{Contacts: []protocol.Contact{{Username: []byte("fixture-friend-" + label), Nickname: &friend}, {Username: []byte("fixture-room-" + label + "@chatroom"), Nickname: &room}}})
	if err := s.saveLocked(); err != nil {
		return nil, err
	}
	nonce, err := protocol.Random(16)
	if err != nil {
		return nil, err
	}
	return &DeploymentDriver{service: s, label: label, done: make(chan struct{}), generation: hex.EncodeToString(nonce)}, nil
}

// A foreign binding is rejected from its filename before any checkpoint/key
// load. Only the known dummy namespace is decrypted for key validation, before
// archive/intent/history recovery is allowed to touch that account directory.
func ValidateDeploymentDirectory(directory, label string) error {
	if !strings.HasPrefix(label, "synthetic-") || len(label) > 48 {
		return errors.New("deployment_fixture_invalid_label")
	}
	seed := sha256.Sum256([]byte("wechat-runtime/offline-deployment/" + label))
	bootstrap := protocol.Bootstrap{Username: []byte("fixture-owner-" + label), UIN: 8000 + uint32(seed[0]), DeviceID: seed[:16]}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0700 {
		return errors.New("deployment_fixture_invalid_directory")
	}
	cp, exists, err := deploymentCheckpoint(filepath.Join(directory, "checkpoints"))
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if cp.Binding != bootstrap.Binding() {
		return errors.New("deployment_fixture_refuses_foreign_binding")
	}
	data, err := cp.Load()
	if err != nil || !bytes.Equal(data.APIKey, seed[:]) || !bytes.Equal(data.Session.Original, bytes.Repeat([]byte{2}, 16)) || !bytes.Equal(data.Session.ClientKey, bytes.Repeat([]byte{3}, 24)) || !bytes.Equal(data.Session.ServerKey, bytes.Repeat([]byte{4}, 24)) || !bytes.Equal(data.Session.Bootstrap.AAK, []byte("synthetic-not-login-material")) {
		return errors.New("deployment_fixture_refuses_nonfixture_checkpoint")
	}
	return nil
}

// store.Unique intentionally creates missing directories in normal startup;
// preflight must remain read-only, including when it refuses a foreign root.
func deploymentCheckpoint(directory string) (store.Checkpoint, bool, error) {
	info, err := os.Lstat(directory)
	if os.IsNotExist(err) {
		return store.Checkpoint{}, false, nil
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0700 {
		return store.Checkpoint{}, false, errors.New("deployment_fixture_invalid_checkpoint_directory")
	}
	paths, err := filepath.Glob(filepath.Join(directory, "*.checkpoint"))
	if err != nil || len(paths) > 1 {
		return store.Checkpoint{}, false, errors.New("deployment_fixture_ambiguous_checkpoint")
	}
	if len(paths) == 0 {
		return store.Checkpoint{}, false, nil
	}
	return store.Checkpoint{Directory: directory, Binding: strings.TrimSuffix(filepath.Base(paths[0]), ".checkpoint")}, true, nil
}

func (d *DeploymentDriver) Start(ctx context.Context, mode string) {
	s := d.service
	s.SetMediaDownloader(d.downloadMedia)
	(BotAdapter{Service: s, Archive: s.history.deliveryArchive}).wrapArchiveMediaDownloader()
	s.SetMediaSender(d.sendMedia)
	go func() {
		defer close(d.done)
		if mode != "offline" {
			d.fail(errors.New("deployment_fixture_requires_offline_config"))
			return
		}
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := d.complete(ctx); err != nil {
					d.fail(err)
					return
				}
			}
		}
	}()
}

func (d *DeploymentDriver) fail(err error) {
	d.mu.Lock()
	d.err = err
	d.mu.Unlock()
}
func (d *DeploymentDriver) Wait() { <-d.done }
func (d *DeploymentDriver) Inspect() DeploymentObservation {
	d.mu.Lock()
	defer d.mu.Unlock()
	v := DeploymentObservation{Generation: d.generation, Sends: append([]DeploymentSend{}, d.sends...), Downloads: d.downloads, Uploads: d.uploads}
	if d.err != nil {
		v.Error = "deployment_fixture_failed"
	}
	return v
}

func (d *DeploymentDriver) complete(ctx context.Context) error {
	s := d.service
	s.mu.Lock()
	media := []string{}
	completed := []DeploymentSend{}
	for id, job := range s.data.Jobs {
		if job.Status != "queued" {
			continue
		}
		if job.Origin != "onebot" {
			s.mu.Unlock()
			return errors.New("deployment_fixture_refuses_other_producers")
		}
		if job.Media != nil {
			media = append(media, id)
			continue
		}
		mentions, err := protocol.ParseMentions([]byte(job.Source))
		if err != nil {
			s.mu.Unlock()
			return err
		}
		if mentions == nil {
			mentions = []string{}
		}
		job.Status, job.ServerID = "accepted", uint64(s.data.Session.Bootstrap.UIN)<<32|uint64(job.ClientID)
		kind := "private"
		if protocol.IsGroup([]byte(job.Recipient)) {
			kind = "group"
		}
		completed = append(completed, DeploymentSend{ID: id, Text: job.Content, Kind: kind, Mentions: mentions, ServerID: job.ServerID})
	}
	if len(completed) > 0 {
		if err := s.saveLocked(); err != nil {
			s.mu.Unlock()
			return err
		}
	}
	s.mu.Unlock()
	d.mu.Lock()
	d.sends = append(d.sends, completed...)
	d.mu.Unlock()
	for _, id := range media {
		if err := s.sendMediaQueued(ctx, id); err != nil {
			return err
		}
		s.mu.RLock()
		binding := s.data.Binding
		s.mu.RUnlock()
		state, err := (BotAdapter{Service: s, Archive: s.history.deliveryArchive}).Delivery(binding, id)
		if err != nil {
			return err
		}
		if state.State == "accepted" || state.State == "echo_observed" {
			d.mu.Lock()
			row, ok := d.pendingMedia[id]
			if ok {
				d.sends = append(d.sends, row)
				delete(d.pendingMedia, id)
			}
			d.mu.Unlock()
		}
	}
	return s.downloadQueued(ctx)
}

func (d *DeploymentDriver) Inject(kind, text string, mentioned bool) error {
	if (kind != "private" && kind != "group") || len(text) > 1024 || strings.TrimSpace(text) == "" {
		return errors.New("invalid_fixture_event")
	}
	return d.injectMessage(kind, protocol.Message{Type: 1, Content: []byte(text)}, mentioned)
}

func (d *DeploymentDriver) injectMessage(kind string, message protocol.Message, mentioned bool) error {
	if kind != "private" && kind != "group" {
		return errors.New("invalid_fixture_event")
	}
	s := d.service
	s.mu.Lock()
	defer s.mu.Unlock()
	stats, err := s.history.Stats(s.data.Binding)
	if err != nil {
		return err
	}
	message.Sender = []byte("fixture-friend-" + d.label)
	message.Recipient = s.data.Session.Bootstrap.Username
	message.ServerID = 10000000 + stats.Cursor + 1
	message.Timestamp = uint32(time.Now().Unix())
	if kind == "group" {
		message.Sender = []byte("fixture-room-" + d.label + "@chatroom")
		message.Content = append([]byte("fixture-friend-"+d.label+":\n"), message.Content...)
		if mentioned {
			source, err := protocol.MentionSource([]string{string(s.data.Session.Bootstrap.Username)})
			if err != nil {
				return err
			}
			message.Source = []byte(source)
		}
	}
	s.ingestLocked(protocol.SyncResult{Messages: []protocol.Message{message}})
	return s.saveLocked()
}
