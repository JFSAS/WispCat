package cdn

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
	"time"
)

// Client performs a bounded stream exchange. It does not reauthenticate, retry
// another IP, infer TLS from a port number or read anything from disk. A caller
// can replace DialContext with a TLS-aware dialer after selecting that transport.
// A zero Client uses a 20 second limit, 16 MiB plaintext limit and net.Dialer.
type Client struct {
	Timeout     time.Duration
	MaxBytes    int64
	DialContext func(context.Context, string, string) (net.Conn, error)
}

func (c *Client) Download(ctx context.Context, p Profile, r DownloadRequest) (*Result, error) {
	if ctx == nil {
		return nil, fail("invalid_context")
	}
	timeout, maxBytes := c.Timeout, c.MaxBytes
	if timeout == 0 {
		timeout = 20 * time.Second
	}
	if maxBytes == 0 {
		maxBytes = DefaultMaxBytes
	}
	if timeout < 0 || timeout > 5*time.Minute || maxBytes < 1 || maxBytes > 1<<30 {
		return nil, fail("invalid_config")
	}
	if err := validateProfile(p); err != nil {
		return nil, err
	}
	if err := validateRequest(r, maxBytes); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	packet, err := encodeRequest(p, r)
	if err != nil {
		return nil, err
	}
	frame, err := c.exchange(ctx, p, packet)
	if err != nil {
		return nil, err
	}
	result, err := DecodeResponse(frame, r, packet.sequence, maxBytes)
	if cause := ctx.Err(); cause != nil {
		return nil, networkError(ctx, cause)
	}
	return result, err
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n <= 0 || n > len(data) {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func networkError(ctx context.Context, err error) error {
	if cause := ctx.Err(); cause != nil {
		category := "cancelled"
		if errors.Is(cause, context.DeadlineExceeded) {
			category = "deadline_exceeded"
		}
		return &Error{Category: category, cause: cause}
	}
	var network net.Error
	if errors.As(err, &network) && network.Timeout() {
		return &Error{Category: "deadline_exceeded", cause: context.DeadlineExceeded}
	}
	return fail("network_error")
}

func (c *Client) exchange(ctx context.Context, p Profile, packet Packet) ([]byte, error) {
	var response []byte
	err := c.exchangeFrames(ctx, p, packet, 1, func(frame []byte) (bool, error) {
		response = frame
		return true, nil
	})
	return response, err
}

// exchangeFrames keeps one connection for all acknowledgements. Native30000
// may send progress before the final fileid. An incomplete callback never
// becomes success, and a bounded frame count supplements the overall deadline.
func (c *Client) exchangeFrames(ctx context.Context, p Profile, packet Packet, frameLimit int, accept func([]byte) (bool, error)) error {
	if ctx == nil {
		return fail("invalid_context")
	}
	if frameLimit < 1 || frameLimit > 4096 || accept == nil {
		return fail("invalid_config")
	}
	timeout, maxBytes := c.Timeout, c.MaxBytes
	if timeout == 0 {
		timeout = 20 * time.Second
	}
	if maxBytes == 0 {
		maxBytes = DefaultMaxBytes
	}
	if timeout < 0 || timeout > 5*time.Minute || maxBytes < 1 || maxBytes > 1<<30 {
		return fail("invalid_config")
	}
	if len(p.Addresses) == 0 || len(p.Ports) == 0 || p.Ports[0] == 0 || net.ParseIP(p.Addresses[0]) == nil {
		return invalid("invalid_profile", "endpoint")
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	dial := c.DialContext
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	address := net.JoinHostPort(p.Addresses[0], strconv.Itoa(int(p.Ports[0])))
	conn, err := dial(ctx, "tcp", address)
	if err != nil {
		return networkError(ctx, err)
	}
	if conn == nil {
		return fail("network_error")
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline() // WithTimeout above always installs a deadline.
	if err := conn.SetDeadline(deadline); err != nil {
		return networkError(ctx, err)
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	if err := writeAll(conn, packet.data); err != nil {
		return networkError(ctx, err)
	}
	for count := 0; count < frameLimit; count++ {
		frame, err := readFrame(conn, maxBytes)
		if err != nil {
			var protocol *Error
			if errors.As(err, &protocol) {
				return err
			}
			return networkError(ctx, err)
		}
		if err := ctx.Err(); err != nil {
			return networkError(ctx, err)
		}
		complete, err := accept(frame)
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return networkError(ctx, err)
		}
		if complete {
			return nil
		}
	}
	return fail("response_frame_limit")
}

func readFrame(reader io.Reader, maxBytes int64) ([]byte, error) {
	header := make([]byte, HeaderBytes)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, err
	}
	if header[0] != 0xab {
		return nil, fail("invalid_frame")
	}
	total := uint64(binary.BigEndian.Uint32(header[1:5]))
	bodyLen := uint64(binary.BigEndian.Uint32(header[21:25]))
	if total != bodyLen+HeaderBytes || bodyLen > uint64(maxBytes)+MaxMetadataBytes+16 {
		return nil, fail("invalid_frame")
	}
	frame := make([]byte, int(total))
	copy(frame, header)
	if _, err := io.ReadFull(reader, frame[HeaderBytes:]); err != nil {
		return nil, err
	}
	return frame, nil
}
