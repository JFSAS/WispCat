package host

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"sort"

	"github.com/JFSAS/wechat-runtime/internal/store"
)

// Membership -> shared changes -> management -> account settings. Individual
// editors take the same shared gate before their own settings lock. Account
// retirement is joined outside this gate by the membership implementation.
func (h *Host) lockConfiguration() ([]*accountRuntime, func()) {
	h.topology.mu.Lock()
	h.listenerChanges.Lock()
	h.management.mu.Lock()
	h.mu.Lock()
	owners := append([]*accountRuntime(nil), h.accounts...)
	h.mu.Unlock()
	sort.Slice(owners, func(i, j int) bool { return owners[i].config.Name < owners[j].config.Name })
	for _, owner := range owners {
		owner.inbound.mu.Lock()
		owner.connections.mu.Lock()
	}
	return owners, func() {
		for i := len(owners) - 1; i >= 0; i-- {
			owners[i].connections.mu.Unlock()
			owners[i].inbound.mu.Unlock()
		}
		h.management.mu.Unlock()
		h.listenerChanges.Unlock()
		h.topology.mu.Unlock()
	}
}

func (h *Host) savedConfiguration() (configurationPlan, error) {
	if journal, err := readConfigurationJournal(h.config.StateDir); err != nil {
		return configurationPlan{}, err
	} else if journal != nil {
		return journal.Plan, nil
	}
	cfg := h.config
	cfg.Listen, cfg.TokenEnv = h.management.saved.Listen, h.management.saved.TokenEnv
	cfg.Accounts = cloneTopology(h.topology.saved).Accounts
	cfg, err := effectiveInboundConfig(cfg)
	if err != nil {
		return configurationPlan{}, err
	}
	cfg, err = savedConnections(cfg)
	if err != nil {
		return configurationPlan{}, err
	}
	return configurationPlan{Version: 1, Management: h.management.saved, Accounts: cfg.Accounts}, nil
}

func (h *Host) configurationView() map[string]any {
	_, unlock := h.lockConfiguration()
	defer unlock()
	plan, err := h.savedConfiguration()
	state, category := "ready", h.configurationError
	if h.configurationPending {
		state = "recovery_pending"
	}
	if err != nil {
		state, category = "unavailable", "configuration_read_failed"
		return map[string]any{"persistence": state, "error": category}
	}
	return map[string]any{"saved": plan, "revision": configRevision(plan), "persistence": state, "error": category,
		"runtime_application": "separate_controls"}
}

func (h *Host) configurationJournal(plan configurationPlan, owners []*accountRuntime) (configurationJournal, error) {
	journal := configurationJournal{Version: 1, Plan: plan, Topology: topologyPlan{Version: 1, Accounts: []Account{}}}
	for _, desired := range plan.Accounts {
		bootstrap := desired
		bootstrap.Reverse, bootstrap.HTTPPost = nil, nil
		for _, owner := range owners {
			if instanceKey(owner.config) == instanceKey(desired) && owner.config.Name != desired.Name {
				return journal, errors.New("stop_instance_before_rename")
			}
			if owner.config.Name != desired.Name {
				continue
			}
			if instanceKey(owner.config) != instanceKey(desired) {
				return journal, errors.New("active_account_identity_immutable")
			}
			if owner.bootstrap.Mode != desired.Mode {
				return journal, errors.New("active_account_bootstrap_immutable")
			}
			bootstrap = owner.bootstrap
		}
		journal.Topology.Accounts = append(journal.Topology.Accounts, bootstrap)
	}
	raw, _ := json.Marshal(journal.Topology)
	if len(raw) > maxTopologyBytes {
		return journal, errors.New("topology_config_too_large")
	}
	return journal, nil
}

func (h *Host) publishSavedConfiguration(journal configurationJournal, owners []*accountRuntime) {
	h.management.saved = journal.Plan.Management
	h.management.revision = configRevision(journal.Plan.Management)
	h.topology.saved = cloneTopology(journal.Topology)
	h.topology.revision = configRevision(journal.Topology)
	for _, owner := range owners {
		for _, desired := range journal.Plan.Accounts {
			if owner.config.Name != desired.Name {
				continue
			}
			owner.inbound.saved = inboundPlan{Listen: desired.Listen, TokenEnv: desired.TokenEnv}
			owner.inbound.revision = inboundRevision(owner.inbound.saved)
			owner.connections.saved = connectionPlan{Reverse: desired.Reverse, HTTPPost: desired.HTTPPost}
			owner.connections.revision = planRevision(owner.connections.saved)
		}
	}
}

func (h *Host) saveConfiguration(revision string, plan configurationPlan) error {
	owners, unlock := h.lockConfiguration()
	defer unlock()
	h.mu.Lock()
	running := h.started && !h.closed
	h.mu.Unlock()
	if !running {
		return errors.New("runtime_not_running")
	}
	if err := h.configurationReady(); err != nil {
		return err
	}
	if h.management.runningOperation() {
		return errors.New("management_operation_pending")
	}
	if err := h.configurationRetirementReady(); err != nil {
		return err
	}
	previous, err := h.savedConfiguration()
	if err != nil {
		return err
	}
	if revision != configRevision(previous) {
		return errors.New("configuration_revision_conflict")
	}
	if err = plan.validate(h.config.StateDir); err != nil {
		return err
	}
	// Detach caller slices before publishing them as saved immutable snapshots.
	raw, _ := json.Marshal(plan)
	var detached configurationPlan
	_ = json.Unmarshal(raw, &detached)
	plan = detached
	journal, err := h.configurationJournal(plan, owners)
	if err != nil {
		return err
	}
	raw, _ = json.Marshal(journal)
	if len(raw) > maxConfigurationJournalBytes {
		return errors.New("configuration_too_large")
	}
	path := filepath.Join(h.config.StateDir, "configuration-transaction.json")
	write := h.configurationWrite
	if write == nil {
		write = store.Atomic
	}
	if err = write(path, raw); err != nil {
		// Atomic replacement may have happened before directory sync failed.
		if actual, e := readConfigurationJournal(h.config.StateDir); e != nil {
			h.configurationPending = true
			h.configurationError = "configuration_commit_uncertain"
		} else if actual != nil {
			h.configurationPending = true
			h.configurationError = "configuration_commit_uncertain"
			h.publishSavedConfiguration(*actual, owners)
		}
		return err
	}
	h.configurationPending = true
	h.publishSavedConfiguration(journal, owners)
	if err = projectConfiguration(h.config.StateDir, &journal, h.syncConfiguration); err != nil {
		h.configurationError = "configuration_projection_pending"
		return err
	}
	h.configurationPending = false
	h.configurationError = ""
	return nil
}

func (h *Host) resumeConfiguration() error {
	owners, unlock := h.lockConfiguration()
	defer unlock()
	h.mu.Lock()
	running := h.started && !h.closed
	h.mu.Unlock()
	if !running {
		return errors.New("runtime_not_running")
	}
	if h.management.runningOperation() {
		return errors.New("management_operation_pending")
	}
	if err := h.configurationRetirementReady(); err != nil {
		return err
	}
	journal, err := readConfigurationJournal(h.config.StateDir)
	if err != nil {
		h.configurationPending = true
		h.configurationError = "configuration_read_failed"
		return err
	}
	if journal != nil {
		h.configurationPending = true
		h.publishSavedConfiguration(*journal, owners)
		if err = projectConfiguration(h.config.StateDir, journal, h.syncConfiguration); err != nil {
			h.configurationError = "configuration_projection_pending"
			return err
		}
	} else if err = h.syncConfiguration(h.config.StateDir); err != nil {
		// An earlier unlink may have succeeded before its directory sync
		// failed. Confirm its durability before allowing profile-only edits.
		h.configurationPending = true
		h.configurationError = "configuration_projection_pending"
		return err
	}
	h.configurationPending = false
	h.configurationError = ""
	return nil
}

func (h *Host) serveConfiguration(w http.ResponseWriter, r *http.Request, reply func(int, any)) {
	if r.Method == http.MethodGet && r.URL.Path == "/configuration" {
		reply(200, h.configurationView())
		return
	}
	if r.Method != http.MethodPost {
		reply(405, map[string]string{"error": "method_not_allowed"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxConfigurationJournalBytes)
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var err error
	switch r.URL.Path {
	case "/configuration":
		var input struct {
			Revision *string            `json:"revision"`
			Plan     *configurationPlan `json:"plan"`
		}
		if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF || input.Revision == nil || input.Plan == nil {
			reply(400, map[string]string{"error": "invalid_configuration"})
			return
		}
		err = h.saveConfiguration(*input.Revision, *input.Plan)
	case "/configuration/recover":
		var empty map[string]json.RawMessage
		if decoder.Decode(&empty) != nil || empty == nil || len(empty) != 0 || decoder.Decode(new(any)) != io.EOF {
			reply(400, map[string]string{"error": "expected_empty_json_object"})
			return
		}
		err = h.resumeConfiguration()
	default:
		reply(404, map[string]string{"error": "not_found"})
		return
	}
	if err != nil {
		reply(409, map[string]any{"error": "configuration_save_failed", "category": configurationErrorCategory(err), "status": h.configurationView()})
		return
	}
	reply(200, h.configurationView())
}

func configurationErrorCategory(err error) string {
	switch err.Error() {
	case "configuration_invalid", "configuration_too_large", "configuration_revision_conflict", "configuration_recovery_pending", "active_account_identity_immutable", "active_account_bootstrap_immutable", "connections_config_too_large", "listener_config_too_large", "account_retirement_pending", "account_cleanup_failed":
		return err.Error()
	default:
		return managementErrorCategory(err)
	}
}
