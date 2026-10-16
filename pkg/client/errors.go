package client

import (
	"context"
	"errors"
)

// These categories describe the client exchange, not a platform delivery
// outcome. A failed/canceled exchange may follow a committed send; retain the
// original request key and query its outcome rather than automatically replaying.
var (
	ErrTransport        = errors.New("onebot_transport_failed")
	ErrHTTPStatus       = errors.New("onebot_http_failed")
	ErrInvalidResponse  = errors.New("invalid_onebot_response")
	ErrIncompleteStream = errors.New("incomplete_onebot_stream")
)

// Preserve standard cancellation/deadline identity without exposing an
// underlying URL, authorization/header detail or a caller's custom cancel cause.
// Only failed I/O uses this helper; complete valid responses keep their outcome
// even if cancellation races with decoding.
func exchangeError(ctx context.Context, category, cause error) error {
	if stopped := ctx.Err(); stopped != nil {
		return errors.Join(category, stopped)
	}
	for _, stopped := range []error{context.Canceled, context.DeadlineExceeded} {
		if errors.Is(cause, stopped) {
			return errors.Join(category, stopped)
		}
	}
	return category
}
