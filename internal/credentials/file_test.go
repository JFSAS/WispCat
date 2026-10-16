package credentials

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/JFSAS/wechat-runtime/internal/protocol"
)

func syntheticBootstrap() protocol.Bootstrap {
	first, second := bytes.Repeat([]byte{7}, 24), []byte("synthetic-second")
	return protocol.Bootstrap{Username: []byte("synthetic-owner"), UIN: 123, DeviceID: []byte("synthetic-device-000"), LocalID: 42, KeyInfo: []byte("synthetic-key-info"), Cookie: []byte("cookie"), First: first, Second: second, AAK: protocol.Join(protocol.Blob(1, protocol.Buffer(first)), protocol.Blob(2, protocol.Buffer(second)))}
}

func TestEncryptedFileRoundTripAndAuthentication(t *testing.T) {
	b := syntheticBootstrap()
	password := "synthetic-export-password"
	file, err := Seal(b, password)
	if err != nil {
		t.Fatal(err)
	}
	other, err := Seal(b, password)
	if err != nil || bytes.Equal(file, other) || bytes.Contains(file, b.Username) || bytes.Contains(file, b.KeyInfo) {
		t.Fatal("file is not randomized encrypted material")
	}
	got, err := Open(file, password)
	if err != nil || !reflect.DeepEqual(got, b) {
		t.Fatal("round trip failed")
	}
	for _, position := range []int{len(magic), len(magic) + saltSize, len(file) - 1} {
		broken := bytes.Clone(file)
		broken[position] ^= 1
		if _, err := Open(broken, password); !errors.Is(err, ErrFile) {
			t.Fatal("accepted tampering", position)
		}
	}
	if _, err := Open(file, "synthetic-wrong-password"); !errors.Is(err, ErrFile) {
		t.Fatal("accepted wrong password")
	}
}

func TestFileBoundsVersionAndBootstrapValidation(t *testing.T) {
	for _, file := range [][]byte{nil, []byte("plaintext credential"), bytes.Repeat([]byte{0}, MaxSize+1), []byte("WECHAT-LOGIN\x02" + string(make([]byte, 100)))} {
		if _, err := Open(file, "synthetic-export-password"); !errors.Is(err, ErrFile) {
			t.Fatal("invalid envelope accepted")
		}
	}
	b := syntheticBootstrap()
	if _, err := Seal(b, "short"); !errors.Is(err, ErrPassphrase) {
		t.Fatal("weak password accepted")
	}
	b.First[0] ^= 1
	if Validate(b) == nil {
		t.Fatal("inconsistent AAK accepted")
	}
	b = syntheticBootstrap()
	b.MLCert = true
	if Validate(b) == nil {
		t.Fatal("unsupported crypto profile accepted")
	}
}
