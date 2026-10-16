package host

import (
	"net"
	"sync"
	"sync/atomic"
)

const maxInboundConnections = 256

// Keep ownership of idle and hijacked connections as well as admitted HTTP
// requests. A port handoff closes every old connection while retaining the
// listening socket, so a keep-alive cannot silently cross account ownership.
type inboundConnection struct {
	net.Conn
	binding *inboundBinding
	epoch   *inboundEpoch
	closed  atomic.Bool
	once    sync.Once
}

func (c *inboundConnection) Close() error {
	c.closed.Store(true)
	err := c.Conn.Close()
	c.once.Do(func() {
		c.binding.mu.Lock()
		delete(c.binding.connections, c)
		c.binding.mu.Unlock()
	})
	return err
}

type inboundListener struct {
	net.Listener
	binding *inboundBinding
}

func (l inboundListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		wrapped := &inboundConnection{Conn: conn, binding: l.binding}
		l.binding.mu.Lock()
		refused := l.binding.closed || l.binding.paused || len(l.binding.connections) >= maxInboundConnections
		if !refused {
			wrapped.epoch = l.binding.epoch
			l.binding.connections[wrapped] = struct{}{}
		}
		l.binding.mu.Unlock()
		if refused {
			_ = conn.Close()
			continue
		}
		return wrapped, nil
	}
}

func (b *inboundBinding) pause() *inboundEpoch {
	b.mu.Lock()
	b.paused = true
	epoch := b.epoch
	connections := make([]*inboundConnection, 0, len(b.connections))
	for conn := range b.connections {
		connections = append(connections, conn)
	}
	b.mu.Unlock()
	epoch.cancel()
	for _, conn := range connections {
		_ = conn.Close()
	}
	return epoch
}
