package main

import (
	"context"
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// A duplicated nonblocking descriptor lets Go's poller interrupt a full stdout
// pipe or an idle stdin pipe. The original standard descriptor is never closed,
// and its original nonblocking flag is restored after the owned copy closes.
func cancellableStdio(ctx context.Context, original *os.File) (*os.File, func() error, error) {
	errIO := errors.New("mcp_stdio_unavailable")
	raw, err := original.SyscallConn()
	if err != nil {
		return nil, nil, errIO
	}
	fd := -1
	flags := 0
	var operation error
	err = raw.Control(func(value uintptr) {
		flags, operation = unix.FcntlInt(value, unix.F_GETFL, 0)
		if operation == nil {
			fd, operation = unix.Dup(int(value))
		}
	})
	if err != nil || operation != nil {
		if fd >= 0 {
			_ = unix.Close(fd)
		}
		return nil, nil, errIO
	}
	unix.CloseOnExec(fd)
	if unix.SetNonblock(fd, true) != nil {
		_ = unix.Close(fd)
		return nil, nil, errIO
	}
	file := os.NewFile(uintptr(fd), "mcp-stdio")
	if file == nil {
		_ = unix.Close(fd)
		return nil, nil, errIO
	}
	stop, done := make(chan struct{}), make(chan struct{})
	var watchError error
	go func() {
		defer close(done)
		select {
		case <-ctx.Done():
			watchError = file.Close()
		case <-stop:
		}
	}()
	closeOwned := func() error {
		close(stop)
		closeError := file.Close()
		<-done
		restore := raw.Control(func(value uintptr) {
			current, e := unix.FcntlInt(value, unix.F_GETFL, 0)
			operation = e
			if e == nil {
				_, operation = unix.FcntlInt(value, unix.F_SETFL, current&^unix.O_NONBLOCK|flags&unix.O_NONBLOCK)
			}
		})
		if closeError != nil && !errors.Is(closeError, os.ErrClosed) || watchError != nil && !errors.Is(watchError, os.ErrClosed) || restore != nil || operation != nil {
			return errIO
		}
		return nil
	}
	return file, closeOwned, nil
}
