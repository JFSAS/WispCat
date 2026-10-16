package host

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/JFSAS/wechat-runtime/internal/instances"
	"github.com/JFSAS/wechat-runtime/internal/store"
)

const maxConfigurationBytes = 1 << 20
const maxConfigurationJournalBytes = 4 << 20
const configurationGuard = "wechat-runtime/configuration-transaction/v1"

type configurationPlan struct {
	Version    int            `json:"version"`
	Management managementPlan `json:"management"`
	Accounts   []Account      `json:"accounts"`
}

type configurationJournal struct {
	Version  int               `json:"version"`
	Plan     configurationPlan `json:"plan"`
	Topology topologyPlan      `json:"topology"`
}

func (p configurationPlan) config(root string) Config {
	return Config{StateDir: root, Listen: p.Management.Listen, TokenEnv: p.Management.TokenEnv, Accounts: p.Accounts}
}

func (p configurationPlan) validate(root string) error {
	if p.Version != 1 || p.Management.Version != 1 {
		return errors.New("configuration_invalid")
	}
	if err := p.config(root).Validate(); err != nil {
		return err
	}
	addresses := map[string]bool{}
	for _, address := range append([]string{p.Management.Listen}, configurationAddresses(p.Accounts)...) {
		canonical, zero, err := canonicalListen(address)
		if err != nil {
			return err
		}
		if !zero && addresses[canonical] {
			return errors.New("duplicate_listener")
		}
		if !zero {
			addresses[canonical] = true
		}
	}
	raw, err := json.Marshal(p)
	if err != nil || len(raw) > maxConfigurationBytes {
		return errors.New("configuration_too_large")
	}
	raw, _ = json.Marshal(p.Management)
	if len(raw) > 8192 {
		return errors.New("management_config_too_large")
	}
	for _, account := range p.Accounts {
		raw, _ = json.Marshal(inboundPlan{Listen: account.Listen, TokenEnv: account.TokenEnv})
		if len(raw) > 8192 {
			return errors.New("listener_config_too_large")
		}
		raw, _ = json.Marshal(connectionPlan{Reverse: account.Reverse, HTTPPost: account.HTTPPost})
		if len(raw) > 64<<10 {
			return errors.New("connections_config_too_large")
		}
	}
	return nil
}

func configurationAddresses(accounts []Account) []string {
	result := make([]string, 0, len(accounts))
	for _, account := range accounts {
		result = append(result, account.Listen)
	}
	return result
}

// A present journal is authoritative, even if writing its required marker or
// projecting individual files was interrupted. A missing required journal must
// never resurrect bootstrap/partially projected configuration.
func readConfigurationJournal(root string) (*configurationJournal, error) {
	guard, guardErr := store.ReadPrivate(filepath.Join(root, "configuration.required"), 128)
	if guardErr != nil && !errors.Is(guardErr, os.ErrNotExist) {
		return nil, guardErr
	}
	if guardErr == nil && string(guard) != configurationGuard {
		return nil, errors.New("configuration_guard_invalid")
	}
	raw, err := store.ReadPrivate(filepath.Join(root, "configuration-transaction.json"), maxConfigurationJournalBytes)
	if errors.Is(err, os.ErrNotExist) {
		if guardErr == nil {
			return nil, errors.New("configuration_journal_missing")
		}
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var journal configurationJournal
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&journal) != nil || decoder.Decode(new(any)) != io.EOF || journal.Version != 1 || journal.Topology.Version != 1 || journal.Topology.Accounts == nil || len(journal.Topology.Accounts) != len(journal.Plan.Accounts) {
		return nil, errors.New("configuration_journal_invalid")
	}
	if err = journal.Plan.validate(root); err != nil {
		return nil, err
	}
	for i, account := range journal.Topology.Accounts {
		desired := journal.Plan.Accounts[i]
		if account.Name != desired.Name || instanceKey(account) == "" || instanceKey(account) != instanceKey(desired) || account.Mode != desired.Mode {
			return nil, errors.New("configuration_journal_invalid")
		}
	}
	raw, _ = json.Marshal(journal.Topology)
	if len(raw) > maxTopologyBytes {
		return nil, errors.New("configuration_journal_invalid")
	}
	return &journal, nil
}

func syncConfigurationDirectory(root string) error {
	directory, err := os.Open(root)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

// Redo only fixed, derived private configuration paths. No path from the
// journal can name a file, and no protocol worker/account session is started.
func projectConfiguration(root string, journal *configurationJournal, syncDirectory func(string) error) error {
	if err := store.Atomic(filepath.Join(root, "configuration.required"), []byte(configurationGuard)); err != nil {
		return err
	}
	if err := store.Directory(filepath.Join(root, "instances")); err != nil {
		return err
	}
	catalog := &instances.Catalog{Root: root}
	for _, account := range journal.Plan.Accounts {
		profile, err := catalog.Ensure(account.Instance)
		if err != nil {
			return err
		}
		listener, _ := json.Marshal(inboundPlan{Listen: account.Listen, TokenEnv: account.TokenEnv})
		if err = store.Atomic(filepath.Join(profile.Directory, "listener.json"), listener); err != nil {
			return err
		}
		connections, _ := json.Marshal(connectionPlan{Reverse: account.Reverse, HTTPPost: account.HTTPPost})
		if err = store.Atomic(filepath.Join(profile.Directory, "connections.json"), connections); err != nil {
			return err
		}
	}
	if err := store.Atomic(filepath.Join(root, "management.required"), []byte(managementGuard)); err != nil {
		return err
	}
	management, _ := json.Marshal(journal.Plan.Management)
	if err := store.Atomic(filepath.Join(root, "management.json"), management); err != nil {
		return err
	}
	if err := store.Atomic(filepath.Join(root, "topology.required"), []byte(topologyGuard)); err != nil {
		return err
	}
	topology, _ := json.Marshal(journal.Topology)
	if err := store.Atomic(filepath.Join(root, "topology.json"), topology); err != nil {
		return err
	}
	// Make removal of the guard durable before removing the journal. If a crash
	// retains the journal, replay is harmless; it cannot retain only the guard.
	if err := os.Remove(filepath.Join(root, "configuration.required")); err != nil {
		return err
	}
	if err := syncDirectory(root); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(root, "configuration-transaction.json")); err != nil {
		return err
	}
	return syncDirectory(root)
}

func recoverConfiguration(root string) error {
	journal, err := readConfigurationJournal(root)
	if err != nil {
		return err
	}
	if journal == nil {
		return syncConfigurationDirectory(root)
	}
	return projectConfiguration(root, journal, syncConfigurationDirectory)
}

func (h *Host) syncConfiguration(root string) error {
	if h.configurationSync != nil {
		return h.configurationSync(root)
	}
	return syncConfigurationDirectory(root)
}

// A removed account can still own an admitted editor until its done channel
// closes. Do not project a new instance configuration under that old lease.
func (h *Host) configurationRetirementReady() error {
	h.mu.Lock()
	retiring := append([]*accountRuntime(nil), h.retiring...)
	h.mu.Unlock()
	for _, owner := range retiring {
		select {
		case <-owner.done:
			if owner.closeError != nil {
				return errors.New("account_cleanup_failed")
			}
		default:
			return errors.New("account_retirement_pending")
		}
	}
	return nil
}

// Called under the shared configuration/listener gate by ordinary editors.
func (h *Host) configurationReady() error {
	if h.configurationPending {
		return errors.New("configuration_recovery_pending")
	}
	return nil
}
