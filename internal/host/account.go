package host

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"time"

	"github.com/JFSAS/wechat-runtime/internal/instances"
	"github.com/JFSAS/wechat-runtime/internal/onebot"
	"github.com/JFSAS/wechat-runtime/internal/service"
)

// Account ownership depends on a start/join interface. Production always uses
// the native Service; a build-tagged deployment fixture supplies the second
// adapter without starting a production session loop.
type accountWorker interface {
	Start(context.Context, string)
	Wait()
}
type accountWorkerFactory struct {
	checkRoot func(Config) error
	check     func(Account, string) error
	open      func(Account, *service.Service) (accountWorker, error)
}

func openAccount(top Config, config Account, profile instances.Profile, state *onebot.State, factory *accountWorkerFactory) (_ *accountRuntime, err error) {
	config.Instance = profile.App
	if factory != nil {
		if err := factory.check(config, profile.Directory); err != nil {
			return nil, err
		}
	}
	archive, err := service.OpenDeliveryArchive(profile.Directory)
	if err != nil {
		return nil, err
	}
	a := &accountRuntime{config: config, accountLifecycle: accountLifecycle{done: make(chan struct{})}}
	a.bot.Archive = archive
	defer func() {
		if err != nil {
			_ = a.closeResources()
		}
	}()
	if err = archive.Recover(profile.Directory); err != nil {
		return nil, err
	}
	if err = service.RecoverBotIntents(profile.Directory, archive); err != nil {
		return nil, err
	}
	a.channel = service.New(profile.Directory, time.Second, profile.App)
	a.bot.Service = a.channel
	history, err := service.OpenEventHistory(profile.Directory)
	if err != nil {
		return nil, err
	}
	if err = a.bot.EnableHistory(history); err != nil {
		_ = history.Close()
		return nil, err
	}
	if err = a.bot.RecoverOutgoingMedia(); err != nil {
		return nil, err
	}
	a.worker = a.channel
	if factory != nil {
		a.worker, err = factory.open(config, a.channel)
		if err != nil {
			return nil, err
		}
	}
	secret, err := token(config.TokenEnv)
	if err != nil {
		return nil, err
	}
	a.adapter = onebot.NewServer(onebot.New(a.bot, state), secret, "")
	streams, err := onebot.OpenStreams(filepath.Join(profile.Directory, "stream-temp"))
	if err != nil {
		return nil, err
	}
	a.adapter.Runtime.Streams = streams
	a.connections, err = openConnectionSettings(profile.Directory, top, config)
	if err != nil {
		return nil, err
	}
	a.inbound = newInboundSettings(profile.Directory, top, config)
	return a, nil
}

func (a *accountRuntime) start(parent context.Context, networks []onebot.NetworkSpec) error {
	ctx, cancel := context.WithCancel(parent)
	a.cancel = cancel
	a.bot.EnableNativeMedia()
	a.worker.Start(ctx, a.config.Mode)
	a.workers.Add(2)
	go func() { defer a.workers.Done(); a.bot.RunArchive(ctx) }()
	go func() { defer a.workers.Done(); a.worker.Wait() }()
	a.inbound.mu.Lock()
	a.inbound.manager = newInboundServer(a.adapter, ctx, a.listener, a.config.Listen, a.adapter.Token)
	a.inbound.active = a.inbound.revision
	a.inbound.mu.Unlock()
	a.connections.manager = onebot.NewNetworkManager(a.adapter, ctx)
	if err := a.connections.manager.Apply(ctx, networks); err != nil {
		cancel()
		return err
	}
	a.connections.mu.Lock()
	a.connections.active = a.connections.revision
	a.connections.mu.Unlock()
	return nil
}

// retire stops admission before joining requests, workers and storage owners.
// Callers may time out; this operation itself keeps running until ownership is
// actually released. Management leases are registered under Host.mu before an
// account is removed from the active slice.
func (a *accountRuntime) retire() <-chan struct{} {
	a.stopOnce.Do(func() {
		if a.cancel != nil {
			a.cancel()
		}
		go func() {
			var failures []error
			ctx := context.Background()
			if a.inbound != nil && a.inbound.manager != nil {
				if err := a.inbound.manager.close(ctx); err != nil {
					failures = append(failures, err)
				}
			}
			if a.adapter != nil {
				if err := a.adapter.Close(ctx); err != nil {
					failures = append(failures, err)
				}
			}
			if a.connections != nil && a.connections.manager != nil {
				if err := a.connections.manager.Close(ctx); err != nil {
					failures = append(failures, err)
				}
			}
			if a.listener != nil {
				_ = a.listener.Close()
			}
			a.workers.Wait()
			a.requests.Wait()
			failures = append(failures, a.closeResources())
			a.closeError = errors.Join(failures...)
			close(a.done)
		}()
	})
	return a.done
}

func (a *accountRuntime) closeResources() error {
	var failures []error
	if a.adapter != nil && a.adapter.Runtime.Streams != nil {
		if err := a.adapter.Runtime.Streams.Close(); err != nil {
			failures = append(failures, err)
		}
	}
	if a.bot.Service != nil {
		if err := a.bot.CloseHistory(); err != nil {
			failures = append(failures, err)
		}
	}
	if a.bot.Archive != nil {
		if err := a.bot.Archive.Close(); err != nil {
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// Keep lifecycle synchronization next to the account's data ownership.
type accountLifecycle struct {
	cancel     context.CancelFunc
	workers    sync.WaitGroup
	requests   sync.WaitGroup
	stopOnce   sync.Once
	done       chan struct{}
	closeError error
}
