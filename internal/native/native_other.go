//go:build (!darwin && !linux) || !cgo

package native

import (
	"context"
	"errors"
)

var errPlatform = errors.New("this backend requires macOS or Linux with CGO and OpenSSL3")

func KeyInfo(string, string) ([]byte, string, error)                   { return nil, "", errPlatform }
func Exchange(context.Context, string, string, []byte) ([]byte, error) { return nil, errPlatform }

func Supported() bool { return false }
