package protocol

import (
	"bytes"
	"encoding/hex"
)

// GenerateLoginDevice creates an independent installation identifier. It is
// saved before requesting a QR code and is reused after process restarts.
func GenerateLoginDevice() ([]byte, error) {
	b, err := Random(16)
	if err != nil {
		return nil, err
	}
	return []byte("i" + hex.EncodeToString(b)), nil
}

func LocalLoginIdentity(device []byte) (LoginIdentity, error) {
	if len(device) != 33 || device[0] != 'i' {
		return LoginIdentity{}, ErrWire
	}
	if _, err := hex.DecodeString(string(device[1:])); err != nil {
		return LoginIdentity{}, ErrWire
	}
	identity, err := NewClientIdentity(device)
	if err != nil {
		return LoginIdentity{}, err
	}
	return LoginIdentity{DeviceID: bytes.Clone(device), Profile: MacCompatibilityProfile(), Computer: identity.Computer, Description: identity.Description(), Runtime: &identity}, nil
}
