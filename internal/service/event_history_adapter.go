package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/JFSAS/wispcat/internal/media"
	"github.com/JFSAS/wispcat/internal/protocol"
	"github.com/JFSAS/wispcat/internal/store"
	"github.com/JFSAS/wispcat/pkg/bot"
	"os"
)

func (s *Service) adoptHistoryLoginLocked(client *protocol.Client, result protocol.SyncResult, candidate *store.Data, cp store.Checkpoint) error {
	// QR validation has already merged the first Sync cursor. Save session keys
	// and the stable namespace with the cursor from BEFORE that Sync, so a failed
	// history commit/crash still permits the same messages to be fetched again.
	// Deep copying avoids mutating the previous account cache through shared jobs.
	raw, err := json.Marshal(candidate)
	if err != nil {
		return ErrEventHistory
	}
	var data store.Data
	if json.Unmarshal(raw, &data) != nil {
		return ErrEventHistory
	}
	// A prior failed Sync can have uncommitted messages in s.data. Never put
	// those into the recovery anchor: native ingestion would then deduplicate
	// their server replay without staging them into history after a crash.
	durable, err := cp.Load()
	if err == nil {
		if durable.Binding != candidate.Binding || !bytes.Equal(durable.APIKey, candidate.APIKey) {
			return ErrEventHistory
		}
		data = *durable
		data.Session = cloneSession(candidate.Session)
		data.BindingDeviceID = bytes.Clone(candidate.BindingDeviceID)
		data.LoginSource, data.SourceApp, data.SessionInvalid = candidate.LoginSource, candidate.SourceApp, false
	} else if !errors.Is(err, os.ErrNotExist) {
		return ErrEventHistory
	}
	if err := s.history.restoreContactAnchor(&data); err != nil {
		return err
	}
	anchor := data
	anchor.Session = cloneSession(data.Session)
	anchor.Session.Cursor = bytes.Clone(client.SavedCursor)
	restoreContacts, err := s.history.prepareContactCheckpoint(&anchor)
	if err != nil {
		return err
	}
	err = cp.Save(&anchor)
	restoreContacts()
	if err != nil {
		return &protocol.Error{Category: "checkpoint_save_failed"}
	}
	s.data, s.client, s.checkpoint = &data, client, cp
	s.ingestLocked(result)
	return s.saveLocked()
}

func (a BotAdapter) CloseHistory() error {
	if a.Service.history == nil {
		return nil
	}
	return a.Service.history.Close()
}
func (a BotAdapter) HistoryStatus() (HistoryStats, error) {
	s := a.Service
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.history == nil {
		return HistoryStats{}, bot.ErrUnsupported
	}
	account := ""
	if s.mediaScopeLocked() {
		account = s.data.Binding
	}
	return s.history.Stats(account)
}
func (a BotAdapter) historyEchoLocked(row *historyRecord) error {
	s := a.Service
	if !row.Event.LocalEcho {
		row.Event.LocalEcho = s.localEchoLocked(row.Raw, row.Raw.Peer(s.data.Session.Bootstrap.Username))
	}
	if !row.Event.LocalEcho && a.Archive != nil {
		echo, err := a.Archive.localEcho(row.Event.Account, row.Raw, s.data.Session.Bootstrap.Username)
		if err != nil {
			return err
		}
		row.Event.LocalEcho = echo
	}
	return nil
}

// A stored raw reference remains private. Native media code receives exactly the
// original descriptor, with account/conversation checks before key material use.
func (h *EventHistory) mediaDescriptionLocked(s *Service, conversation, id string) (media.Descriptor, []byte, bool, error) {
	row, err := h.message(s.data.Binding, id)
	if err != nil {
		return media.Descriptor{}, nil, true, err
	}
	if row == nil {
		return media.Descriptor{}, nil, false, nil
	}
	if row.Event.Conversation.ID != conversation {
		return media.Descriptor{}, nil, true, ErrMediaUnavailable
	}
	description, err := media.Parse(row.Raw.Type, normalizedMessage(row.Raw, s.data.Session.Bootstrap.Username).Content)
	if err != nil {
		return media.Descriptor{}, nil, true, ErrMediaUnavailable
	}
	return description, row.Raw.Peer(s.data.Session.Bootstrap.Username), true, nil
}
