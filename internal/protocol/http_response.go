package protocol

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net/http"
)

// ResponseError contains classifications only, never response bytes or headers.
type ResponseError struct {
	Stage, Reason string
	Transient     bool
}

func (e *ResponseError) Error() string { return "HTTP response validation failed: " + e.Reason }

// Check framing before the inherited decoder collapses short reads into ErrWire.
// Do not interpret or change the encrypted WeChat payload here.
func validateHTTPResponse(raw []byte) error {
	response, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(raw)), nil)
	if err != nil {
		short := errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
		reason := "malformed_http_header"
		if short {
			reason = "truncated_http_header"
		}
		return &ResponseError{Stage: "http_header", Reason: reason, Transient: short}
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil // Preserve the original typed HTTP status error.
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, (4<<20)+1))
	if len(body) > 4<<20 {
		return &ResponseError{Stage: "http_body", Reason: "http_body_limit"}
	}
	if err != nil {
		short := errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
		reason := "malformed_http_body"
		if short {
			reason = "truncated_http_body"
		}
		return &ResponseError{Stage: "http_body", Reason: reason, Transient: short}
	}
	return nil
}
