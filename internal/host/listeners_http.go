package host

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"time"
)

// The lock order is membership, shared listener changes, then account settings.
// Per-account save/apply uses the shared gate; no account can retire or change
// its saved plan while a coordinated apply prepares the complete cohort.
func (h *Host) lockListeners() ([]*accountRuntime, func()) {
	h.topology.mu.Lock()
	h.listenerChanges.Lock()
	h.mu.Lock()
	owners := append([]*accountRuntime(nil), h.accounts...)
	h.mu.Unlock()
	sort.Slice(owners, func(i, j int) bool { return owners[i].config.Name < owners[j].config.Name })
	for _, owner := range owners {
		owner.inbound.mu.Lock()
	}
	return owners, func() {
		for i := len(owners) - 1; i >= 0; i-- {
			owners[i].inbound.mu.Unlock()
		}
		h.listenerChanges.Unlock()
		h.topology.mu.Unlock()
	}
}

func listenerBatchRevision(owners []*accountRuntime) string {
	type entry struct{ Name, Instance, Revision string }
	entries := []entry{}
	for _, owner := range owners {
		entries = append(entries, entry{owner.config.Name, instanceKey(owner.config), owner.inbound.revision})
	}
	return configRevision(entries)
}

func (h *Host) listenerBatchView() map[string]any {
	owners, unlock := h.lockListeners()
	defer unlock()
	h.mu.Lock()
	applied := h.started && !h.closed
	h.mu.Unlock()
	accounts := []any{}
	for _, owner := range owners {
		state := owner.inbound.viewLocked()
		applied = applied && state["applied"] == true
		accounts = append(accounts, map[string]any{"name": owner.config.Name, "instance": owner.config.Instance, "configuration": state})
	}
	return map[string]any{"revision": listenerBatchRevision(owners), "accounts": accounts, "applied": applied, "error": h.listenerBatchError}
}

func (h *Host) applySavedListeners(ctx context.Context, revision string) error {
	owners, unlock := h.lockListeners()
	defer unlock()
	h.mu.Lock()
	running := h.started && !h.closed
	h.mu.Unlock()
	if !running {
		return errors.New("runtime_not_running")
	}
	if revision != listenerBatchRevision(owners) {
		return errors.New("listener_batch_revision_conflict")
	}
	targets := make([]batchListener, 0, len(owners))
	for _, owner := range owners {
		secret, err := token(owner.inbound.saved.TokenEnv)
		if err != nil {
			h.listenerBatchError = "listener_token_unavailable"
			return err
		}
		targets = append(targets, batchListener{owner.inbound.manager, owner.inbound.saved.Listen, secret})
	}
	if err := applyListenerBatch(ctx, targets); err != nil {
		h.listenerBatchError = "listener_batch_apply_failed"
		return err
	}
	// Retirement also closes this reference. It must follow the socket's new
	// owner, otherwise removing an account could close its sibling's swapped port.
	h.mu.Lock()
	for _, owner := range owners {
		owner.listener = owner.inbound.manager.active.listener
		owner.inbound.active = owner.inbound.revision
		owner.inbound.lastError = ""
	}
	h.mu.Unlock()
	h.listenerBatchError = ""
	return nil
}

func (h *Host) serveListeners(w http.ResponseWriter, r *http.Request, reply func(int, any)) {
	if r.URL.Path == "/listeners" && r.Method == http.MethodGet {
		reply(200, h.listenerBatchView())
		return
	}
	if r.URL.Path != "/listeners/apply" || r.Method != http.MethodPost {
		reply(405, map[string]string{"error": "method_not_allowed"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 8192)
	defer r.Body.Close()
	var input struct {
		Revision *string `json:"revision"`
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF || input.Revision == nil {
		reply(400, map[string]string{"error": "invalid_listener_batch"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := h.applySavedListeners(ctx, *input.Revision); err != nil {
		reply(409, map[string]any{"error": "listener_batch_apply_failed", "category": topologyErrorCategory(err), "status": h.listenerBatchView()})
		return
	}
	reply(200, h.listenerBatchView())
}
