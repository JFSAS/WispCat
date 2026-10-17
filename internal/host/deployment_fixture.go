//go:build integrationfixtures

package host

import (
	"errors"
	"os"
	"path/filepath"
	"strings"

	"github.com/JFSAS/wispcat/internal/instances"
	"github.com/JFSAS/wispcat/internal/service"
	"github.com/JFSAS/wispcat/internal/store"
)

const DeploymentFixtureMarker = "wechat-runtime/synthetic-deployment/v1"

// NewDeploymentFixture is absent from ordinary runtime builds. The caller must
// create a disposable private root with an explicit marker; only synthetic
// account/instance names and offline bootstrap modes are admitted.
func NewDeploymentFixture(c Config) (*Host, error) {
	effective, err := effectiveConfig(c)
	if err != nil {
		return nil, err
	}
	if err := checkDeploymentRoot(effective); err != nil {
		return nil, err
	}
	factory := &accountWorkerFactory{checkRoot: checkDeploymentRoot, check: checkDeploymentAccount, open: func(account Account, channel *service.Service) (accountWorker, error) {
		return service.NewDeploymentDriver(channel, account.Name)
	}}
	return newHost(c, factory)
}

func checkDeploymentAccount(account Account, directory string) error {
	if account.Mode != "offline" || !strings.HasPrefix(account.Name, "synthetic-") || account.Instance != "instance:deployment-"+account.Name {
		return errors.New("deployment_fixture_refuses_live_account")
	}
	return service.ValidateDeploymentDirectory(directory, account.Name)
}

func checkDeploymentRoot(c Config) error {
	info, err := os.Lstat(c.StateDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("deployment_fixture_invalid_root")
	}
	marker, err := store.ReadPrivate(filepath.Join(c.StateDir, "synthetic-deployment.required"), 128)
	if err != nil || string(marker) != DeploymentFixtureMarker {
		return errors.New("deployment_fixture_root_required")
	}
	for _, account := range c.Accounts {
		if account.Mode != "offline" || !strings.HasPrefix(account.Name, "synthetic-") || account.Instance != "instance:deployment-"+account.Name {
			return errors.New("deployment_fixture_refuses_live_account")
		}
	}
	legacy, err := os.ReadDir(filepath.Join(c.StateDir, "checkpoints"))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if len(legacy) > 0 {
		return errors.New("deployment_fixture_refuses_root_legacy_checkpoint")
	}
	if info, err := os.Lstat(filepath.Join(c.StateDir, "checkpoints")); err == nil && (info.Mode()&os.ModeSymlink != 0 || !info.IsDir()) {
		return errors.New("deployment_fixture_invalid_root_checkpoint_directory")
	}
	profiles, err := (&instances.Catalog{Root: c.StateDir}).List()
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, profile := range profiles {
		label := strings.TrimPrefix(profile.App, "instance:deployment-")
		if profile.App == label || !strings.HasPrefix(label, "synthetic-") {
			return errors.New("deployment_fixture_refuses_registered_profile")
		}
		if err := service.ValidateDeploymentDirectory(profile.Directory, label); err != nil {
			return err
		}
	}
	return nil
}

func (h *Host) fixtureLease(name string) (*service.DeploymentDriver, func(), error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || !h.started || h.workerFactory == nil {
		return nil, nil, errors.New("deployment_fixture_not_running")
	}
	for _, account := range h.accounts {
		if account.config.Name == name {
			driver, ok := account.worker.(*service.DeploymentDriver)
			if !ok {
				return nil, nil, errors.New("deployment_fixture_driver_missing")
			}
			account.requests.Add(1)
			return driver, account.requests.Done, nil
		}
	}
	return nil, nil, errors.New("deployment_fixture_account_missing")
}

func (h *Host) DeploymentInject(name, kind, text string, mentioned bool) error {
	driver, release, err := h.fixtureLease(name)
	if err != nil {
		return err
	}
	defer release()
	return driver.Inject(kind, text, mentioned)
}

func (h *Host) DeploymentInspect(name string) (service.DeploymentObservation, error) {
	driver, release, err := h.fixtureLease(name)
	if err != nil {
		return service.DeploymentObservation{}, err
	}
	defer release()
	return driver.Inspect(), nil
}

func (h *Host) DeploymentInjectMedia(name, kind, mediaKind string) error {
	driver, release, err := h.fixtureLease(name)
	if err != nil {
		return err
	}
	defer release()
	return driver.InjectMedia(kind, mediaKind)
}
