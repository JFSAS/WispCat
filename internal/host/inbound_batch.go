package host

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
)

type batchListener struct {
	manager *inboundServer
	listen  string
	secret  string
}

func canonicalListen(address string) (string, bool, error) {
	if !loopbackListen(address) {
		return "", false, errors.New("invalid_listener_configuration")
	}
	host, port, _ := net.SplitHostPort(address)
	number, _ := strconv.ParseUint(port, 10, 16)
	return net.JoinHostPort(net.ParseIP(host).String(), strconv.Itoa(int(number))), number == 0, nil
}

// applyListenerBatch reserves new sockets before cancelling any old request.
// Existing sockets can move between owners only after their old epochs join.
// A failed join restores old admission with a fresh epoch, retaining the old
// retirement channel so shutdown/retry cannot release storage too early.
func applyListenerBatch(parent context.Context, targets []batchListener) (err error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	seen := map[*inboundServer]bool{}
	for _, target := range targets {
		if target.manager == nil || target.manager.parent == nil || seen[target.manager] {
			return errors.New("invalid_listener_batch")
		}
		seen[target.manager] = true
	}
	for _, target := range targets {
		target.manager.operations.Lock()
		defer target.manager.operations.Unlock()
	}
	for _, target := range targets {
		target.manager.mu.Lock()
		defer target.manager.mu.Unlock()
		stop := context.AfterFunc(target.manager.parent, cancel)
		defer stop()
	}
	owned := map[string]*inboundBinding{}
	old := map[*inboundServer]*inboundBinding{}
	for _, target := range targets {
		s := target.manager
		if s.closed || s.parent.Err() != nil || s.prepared != nil {
			return errors.New("listener_batch_unavailable")
		}
		if err = waitInbound(ctx, s.retiring); err != nil {
			return err
		}
		s.retiring = nil
		select {
		case <-s.active.done:
			return errors.New("listener_not_listening")
		default:
		}
		address, _, e := canonicalListen(s.active.listener.Addr().String())
		if e != nil || owned[address] != nil {
			return errors.New("listener_batch_unavailable")
		}
		owned[address], old[s] = s.active, s.active
	}
	allocated := []net.Listener{}
	committed := false
	defer func() {
		if !committed {
			for _, listener := range allocated {
				_ = listener.Close()
			}
		}
	}()
	chosen := map[*inboundServer]*inboundBinding{}
	fresh := map[*inboundServer]net.Listener{}
	assigned := map[*inboundBinding]bool{}
	addresses := map[string]bool{}
	// Fixed destinations get first claim. A zero-port destination can keep its
	// old ephemeral socket only if no fixed destination is taking that socket.
	for _, target := range targets {
		address, zero, e := canonicalListen(target.listen)
		if e != nil || len(target.secret) < 16 || len(target.secret) > 4096 || strings.ContainsAny(target.secret, "\r\n") {
			return errors.New("invalid_listener_configuration")
		}
		if zero {
			continue
		}
		if addresses[address] {
			return errors.New("duplicate_listener")
		}
		addresses[address] = true
		if binding := owned[address]; binding != nil {
			chosen[target.manager], assigned[binding] = binding, true
			continue
		}
		listener, e := net.Listen("tcp", target.listen)
		if e != nil {
			return errors.New("listener_bind_failed")
		}
		allocated = append(allocated, listener)
		fresh[target.manager] = listener
	}
	for _, target := range targets {
		_, zero, _ := canonicalListen(target.listen)
		if !zero {
			continue
		}
		s := target.manager
		previous, previousZero, _ := canonicalListen(s.listen)
		desired, _, _ := canonicalListen(target.listen)
		if previousZero && previous == desired && !assigned[old[s]] {
			chosen[s], assigned[old[s]] = old[s], true
			continue
		}
		listener, e := net.Listen("tcp", target.listen)
		if e != nil {
			return errors.New("listener_bind_failed")
		}
		allocated = append(allocated, listener)
		fresh[s] = listener
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	retired := map[*inboundServer]*inboundEpoch{}
	for _, target := range targets {
		s := target.manager
		if chosen[s] != old[s] || old[s].epoch.secret != target.secret {
			retired[s] = old[s].pause()
		}
	}
	for s, epoch := range retired {
		done := make(chan struct{})
		s.retiring = done
		go func() { epoch.requests.Wait(); close(done) }()
	}
	rollback := func() {
		for s, epoch := range retired {
			binding := old[s]
			binding.mu.Lock()
			binding.epoch = newInboundEpoch(s.parent, epoch.secret)
			binding.paused = false
			binding.mu.Unlock()
			s.lastError = "listener_batch_retirement_pending"
		}
	}
	for s := range retired {
		if err = waitInbound(ctx, s.retiring); err != nil {
			rollback()
			return err
		}
	}
	if err = ctx.Err(); err != nil {
		rollback()
		return err
	}
	for _, target := range targets {
		s := target.manager
		binding := chosen[s]
		if binding == nil {
			binding = newInboundBinding(s.target, fresh[s], newInboundEpoch(s.parent, target.secret))
			binding.paused = true
			chosen[s] = binding
			binding.start()
		} else if binding != old[s] || retired[s] != nil {
			binding.mu.Lock()
			binding.target = s.target
			binding.epoch = newInboundEpoch(s.parent, target.secret)
			binding.mu.Unlock()
		}
		s.active, s.listen, s.retiring, s.lastError = binding, target.listen, nil, ""
		s.target.publish(binding.listener.Addr().String(), target.secret)
	}
	for _, target := range targets {
		binding := chosen[target.manager]
		binding.mu.Lock()
		binding.paused = false
		binding.mu.Unlock()
	}
	for _, binding := range old {
		if !assigned[binding] {
			binding.stop()
			<-binding.done
		}
	}
	committed = true
	return nil
}
