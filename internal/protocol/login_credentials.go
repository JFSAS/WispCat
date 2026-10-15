package protocol

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/elliptic"
	"crypto/md5"
	"time"
)

// decodeAuthentication builds a candidate without changing the live client.
// First login may supply UIN zero; the validated response assigns its account.
func decodeAuthentication(response []byte, h Header, b Bootstrap, profile Profile, private []byte) (*Session, error) {
	curve := elliptic.P224()
	f, e := Parse(response)
	if e != nil {
		return nil, e
	}
	flags, ok := f.N(2)
	section, e := f.Message(3)
	if e != nil || !ok || flags&1 == 0 {
		return nil, &AuthenticationError{Reason: "missing_session_material"}
	}
	uin, present := section.N(1)
	if !present || uin == 0 || uin > 0xffffffff || (b.UIN != 0 && uin != uint64(b.UIN)) {
		return nil, &AuthenticationError{Reason: "response_account_mismatch"}
	}
	b.UIN = uint32(uin)
	if h.UIN != 0 && h.UIN != b.UIN {
		return nil, &AuthenticationError{Reason: "response_account_mismatch"}
	}
	if accountRaw, present := f.B(4); present {
		account, err := Parse(accountRaw)
		if err != nil {
			return nil, err
		}
		if username, present := account.B(1); present && len(username) != 0 {
			if len(username) > 512 || (len(b.Username) != 0 && !bytes.Equal(username, b.Username)) {
				return nil, &AuthenticationError{Reason: "response_account_mismatch"}
			}
			b.Username = bytes.Clone(username)
		}
	}
	if len(b.Username) == 0 {
		return nil, ErrWire
	}
	buffer := func(tag int) ([]byte, error) {
		parts := section.All(tag)
		if len(parts) == 0 {
			return nil, ErrWire
		}
		return Unbuffer(Join(parts...))
	}
	original, e := buffer(3)
	if e != nil {
		return nil, e
	}
	aak, e := buffer(4)
	if e != nil {
		return nil, e
	}
	clientKey, e := buffer(23)
	if e != nil {
		return nil, e
	}
	serverKey, e := buffer(24)
	if e != nil {
		return nil, e
	}
	processing, _ := section.N(19)
	if processing&4 != 0 {
		server, e := section.Message(2)
		if e != nil {
			return nil, e
		}
		nid, _ := server.N(1)
		wrapped, ok := server.B(2)
		if nid != 713 || !ok {
			return nil, ErrWire
		}
		pub, e := Unbuffer(wrapped)
		if e != nil {
			return nil, e
		}
		sx, sy := elliptic.Unmarshal(curve, pub)
		if sx == nil || sy == nil {
			return nil, ErrWire
		}
		shared, _ := curve.ScalarMult(sx, sy, private)
		key := md5.Sum(shared.FillBytes(make([]byte, 28)))
		if len(original) == 0 || len(original)%16 != 0 {
			return nil, ErrWire
		}
		block, e := aes.NewCipher(key[:])
		if e != nil {
			return nil, e
		}
		unwrapped := make([]byte, len(original))
		cipher.NewCBCDecrypter(block, key[:]).CryptBlocks(unwrapped, original)
		pad := int(unwrapped[len(unwrapped)-1])
		if pad < 1 || pad > 16 || pad > len(unwrapped) {
			return nil, ErrWire
		}
		for _, v := range unwrapped[len(unwrapped)-pad:] {
			if int(v) != pad {
				return nil, ErrWire
			}
		}
		original = unwrapped[:len(unwrapped)-pad]
	}
	if len(original) != 16 || len(clientKey) != 24 || len(serverKey) != 24 {
		return nil, ErrWire
	}
	parts, e := Parse(aak)
	if e != nil {
		return nil, e
	}
	first, ok := parts.B(1)
	second, sok := parts.B(2)
	if !ok || !sok {
		return nil, ErrWire
	}
	b.First, e = Unbuffer(first)
	if e != nil {
		return nil, e
	}
	b.Second, e = Unbuffer(second)
	if e != nil {
		return nil, e
	}
	if len(b.First) < 24 || len(b.Second) == 0 {
		return nil, ErrWire
	}
	b.AAK = aak
	b.Cookie = h.Cookie
	return &Session{Bootstrap: b, Profile: profile, Cookie: bytes.Clone(h.Cookie), Original: bytes.Clone(original), ClientKey: bytes.Clone(clientKey), ServerKey: bytes.Clone(serverKey), Sequence: 1, AuthGeneration: 1, AuthenticatedAt: time.Now().Unix()}, nil
}
