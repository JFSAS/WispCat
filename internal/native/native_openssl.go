//go:build (darwin || linux) && cgo

// Package native adapts the validated OpenSSL transport and read-only SQLite.
// The Go backend owns all protocol, storage and service logic.
package native

/*
#cgo CFLAGS: -Wno-unused-function
#cgo darwin LDFLAGS: -lssl -lcrypto -lsqlite3
#cgo linux pkg-config: openssl sqlite3
#include <stdlib.h>
#include <stddef.h>
struct exchange_metadata {
 int configured, tcp_connected, handshake_success, ssl_error;
 long verify_result;
 size_t written, received;
 int extension_sent, read_finished, read_error;
 char tls_version[32], cipher[96];
};
int mmtls2_exchange(const char*,const char*,const char*,const char*,const unsigned char*,size_t,unsigned char*,size_t,struct exchange_metadata*);
int rea_keyinfo(const char*,const char*,unsigned char*,size_t,size_t*,char*);
*/
import "C"

import (
	"context"
	"errors"
	"net"
	"sync"
	"unsafe"
)

var exchangeLock sync.Mutex

func KeyInfo(path, ownerMD5 string) ([]byte, string, error) {
	p, o := C.CString(path), C.CString(ownerMD5)
	defer C.free(unsafe.Pointer(p))
	defer C.free(unsafe.Pointer(o))
	out := make([]byte, 1<<20)
	digest := make([]byte, 33)
	var n C.size_t
	if C.rea_keyinfo(p, o, (*C.uchar)(unsafe.Pointer(&out[0])), C.size_t(len(out)), &n, (*C.char)(unsafe.Pointer(&digest[0]))) != 0 {
		return nil, "", errors.New("current account KeyInfo is unavailable")
	}
	return out[:int(n)], string(digest[:32]), nil
}
func Exchange(ctx context.Context, host, roots string, request []byte) ([]byte, error) {
	return exchange(ctx, host, "443", roots, request)
}
func exchange(ctx context.Context, host, port, roots string, request []byte) ([]byte, error) {
	if len(request) == 0 || len(request) > 1<<20 {
		return nil, errors.New("request size is invalid")
	}
	ips, e := net.DefaultResolver.LookupIP(ctx, "ip4", host)
	if e != nil || len(ips) == 0 {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &ExchangeError{Stage: "resolve", Retryable: true}
	}
	exchangeLock.Lock()
	defer exchangeLock.Unlock()
	if e = ctx.Err(); e != nil {
		return nil, e
	}
	h, ip, p, r := C.CString(host), C.CString(ips[0].String()), C.CString(port), C.CString(roots)
	defer C.free(unsafe.Pointer(h))
	defer C.free(unsafe.Pointer(ip))
	defer C.free(unsafe.Pointer(p))
	defer C.free(unsafe.Pointer(r))
	out := make([]byte, 4<<20)
	var m C.struct_exchange_metadata
	status := C.mmtls2_exchange(h, ip, p, r, (*C.uchar)(unsafe.Pointer(&request[0])), C.size_t(len(request)), (*C.uchar)(unsafe.Pointer(&out[0])), C.size_t(len(out)), &m)
	if e = ctx.Err(); e != nil {
		return nil, e
	}
	if status != 0 || m.handshake_success != 1 || m.verify_result != 0 || m.written != C.size_t(len(request)) {
		diagnostic := &ExchangeError{Code: int(status), SSLError: int(m.ssl_error), VerifyResult: int64(m.verify_result)}
		switch {
		case m.configured != 1:
			diagnostic.Stage = "configure"
		case m.tcp_connected != 1:
			diagnostic.Stage, diagnostic.Retryable = "connect", true
		case m.verify_result != 0:
			diagnostic.Stage = "verify_certificate"
		case m.handshake_success != 1:
			diagnostic.Stage, diagnostic.Retryable = "handshake", true
		default:
			diagnostic.Stage, diagnostic.Retryable = "write", true
		}
		return nil, diagnostic
	}
	if m.received == 0 {
		return nil, &ExchangeError{Stage: "read", Code: int(m.read_error), Retryable: true}
	}
	return out[:int(m.received)], nil
}

func Supported() bool { return true }
