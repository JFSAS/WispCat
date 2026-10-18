package protocol

import (
	"errors"
	"strings"
	"testing"
)

func TestHTTPResponseDistinguishesTruncationFromMalformedData(t *testing.T) {
	for _, test := range []struct {
		name, raw, reason string
		transient         bool
	}{
		{"empty", "", "truncated_http_header", true},
		{"short_body", "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nx", "truncated_http_body", true},
		{"short_chunk", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nx", "truncated_http_body", true},
		{"bad_header", "PRIVATE_RESPONSE_SECRET\r\n\r\n", "malformed_http_header", false},
		{"bad_chunk", "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nPRIVATE_RESPONSE_SECRET\r\n", "malformed_http_body", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateHTTPResponse([]byte(test.raw))
			var response *ResponseError
			if !errors.As(err, &response) || response.Transient != test.transient || response.Reason != test.reason {
				t.Fatalf("unexpected classification: %#v", err)
			}
			if strings.Contains(err.Error(), "PRIVATE_RESPONSE_SECRET") {
				t.Fatal("response error leaked untrusted response bytes")
			}
		})
	}
	for _, raw := range []string{"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nx", "HTTP/1.1 503 Unavailable\r\nContent-Length: 100\r\n\r\nx"} {
		if err := validateHTTPResponse([]byte(raw)); err != nil {
			t.Fatal(err)
		}
	}
}
