// Package credentials carries disk bootstrap material between a Mac and a
// runtime. Files contain no local checkpoint keys or authenticated session.
package credentials

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"

	"github.com/JFSAS/wispcat/internal/protocol"
)

const (
	MaxSize    = 1 << 20
	magic      = "WECHAT-LOGIN\x01"
	saltSize   = 16
	nonceSize  = 12
	iterations = 600000
)

var (
	ErrFile       = errors.New("credential_file_invalid_or_password_incorrect")
	ErrPassphrase = errors.New("credential_password_requires_16_to_4096_bytes")
)

// Validate checks the same disk fields consumed by the original authenticator.
// The official installation fingerprints are checked by account.Source.Load
// on the exporting Mac; an imported file is not proof of server acceptance.
func Validate(b protocol.Bootstrap) error {
	if len(b.Username) == 0 || len(b.Username) > 512 || b.UIN == 0 ||
		len(b.DeviceID) < 16 || len(b.DeviceID) > 512 || b.LocalID == 0 ||
		len(b.Cookie) > 15 || len(b.First) < 24 || len(b.KeyInfo) == 0 || b.MLCert {
		return ErrFile
	}
	if len(b.AAK)+len(b.First)+len(b.Second)+len(b.KeyInfo) > MaxSize/2 {
		return ErrFile
	}
	fields, err := protocol.Parse(b.AAK)
	if err != nil {
		return ErrFile
	}
	first, p := fields.B(1)
	second, q := fields.B(2)
	if !p || !q {
		return ErrFile
	}
	f, err := protocol.Unbuffer(first)
	if err != nil || !bytes.Equal(f, b.First) {
		return ErrFile
	}
	s, err := protocol.Unbuffer(second)
	if err != nil || !bytes.Equal(s, b.Second) {
		return ErrFile
	}
	return nil
}

func aead(password string, salt []byte) (cipher.AEAD, error) {
	if len(password) < 16 || len(password) > 4096 {
		return nil, ErrPassphrase
	}
	key, err := pbkdf2.Key(sha256.New, password, salt, iterations, 32)
	if err != nil {
		return nil, ErrFile
	}
	defer clear(key)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrFile
	}
	return cipher.NewGCM(block)
}

// Seal uses a versioned header, a random salt/nonce, PBKDF2-SHA256 and AES-GCM.
// KDF parameters are fixed by the version rather than controlled by the file.
func Seal(b protocol.Bootstrap, password string) ([]byte, error) {
	if err := Validate(b); err != nil {
		return nil, err
	}
	plain, err := json.Marshal(b)
	if err != nil {
		return nil, ErrFile
	}
	defer clear(plain)
	header := make([]byte, len(magic)+saltSize+nonceSize)
	copy(header, magic)
	if _, err = rand.Read(header[len(magic):]); err != nil {
		return nil, ErrFile
	}
	box, err := aead(password, header[len(magic):len(magic)+saltSize])
	if err != nil {
		return nil, err
	}
	if len(header)+len(plain)+box.Overhead() > MaxSize {
		return nil, ErrFile
	}
	return box.Seal(header, header[len(magic)+saltSize:], plain, header), nil
}

func Open(file []byte, password string) (protocol.Bootstrap, error) {
	var b protocol.Bootstrap
	headerSize := len(magic) + saltSize + nonceSize
	if len(file) < headerSize+16 || len(file) > MaxSize || !bytes.Equal(file[:len(magic)], []byte(magic)) {
		return b, ErrFile
	}
	header := file[:headerSize]
	box, err := aead(password, header[len(magic):len(magic)+saltSize])
	if err != nil {
		return b, err
	}
	plain, err := box.Open(nil, header[len(magic)+saltSize:], file[headerSize:], header)
	if err != nil {
		return b, ErrFile
	}
	defer clear(plain)
	dec := json.NewDecoder(bytes.NewReader(plain))
	dec.DisallowUnknownFields()
	if dec.Decode(&b) != nil || dec.Decode(new(any)) != io.EOF || Validate(b) != nil {
		return protocol.Bootstrap{}, ErrFile
	}
	return b, nil
}
