package protocol

import (
	"bytes"
	"compress/zlib"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"strconv"
)

func Random(n int) ([]byte, error) { b := make([]byte, n); _, e := rand.Read(b); return b, e }
func Seal(key, aad, plain []byte) ([]byte, error) {
	b, e := aes.NewCipher(key)
	if e != nil {
		return nil, e
	}
	g, e := cipher.NewGCM(b)
	if e != nil {
		return nil, e
	}
	var z bytes.Buffer
	w := zlib.NewWriter(&z)
	if _, e = w.Write(plain); e != nil {
		return nil, e
	}
	if e = w.Close(); e != nil {
		return nil, e
	}
	nonce, e := Random(12)
	if e != nil {
		return nil, e
	}
	c := g.Seal(nil, nonce, z.Bytes(), aad)
	return Join(c[:len(c)-16], nonce, c[len(c)-16:]), nil
}
func Unseal(key, aad, container []byte) ([]byte, error) {
	if len(container) < 28 {
		return nil, ErrWire
	}
	b, e := aes.NewCipher(key)
	if e != nil {
		return nil, e
	}
	g, e := cipher.NewGCM(b)
	if e != nil {
		return nil, e
	}
	at := len(container) - 28
	c := Join(container[:at], container[at+12:])
	plain, e := g.Open(nil, container[at:at+12], c, aad)
	if e != nil {
		return nil, errors.New("authenticated payload rejected")
	}
	r, e := zlib.NewReader(bytes.NewReader(plain))
	if e != nil {
		return nil, ErrWire
	}
	defer r.Close()
	out, e := io.ReadAll(io.LimitReader(r, 16<<20+1))
	if e != nil || len(out) > 16<<20 {
		return nil, ErrWire
	}
	return out, nil
}

type Hybrid struct {
	private                          *ecdh.PrivateKey
	aak, aad, requestCipher, signing []byte
}

func NewHybrid(serverPoint, signing, aak, plain []byte) (*Hybrid, []byte, error) {
	k, e := ecdh.P256().GenerateKey(rand.Reader)
	if e != nil {
		return nil, nil, e
	}
	p, e := ecdh.P256().NewPublicKey(serverPoint)
	if e != nil {
		return nil, nil, ErrWire
	}
	secret, e := k.ECDH(p)
	if e != nil {
		return nil, nil, e
	}
	dh := sha256.Sum256(secret)
	meta := Join([]byte("1415"), k.PublicKey().Bytes())
	mh := sha256.Sum256(meta)
	random, e := Random(32)
	if e != nil {
		return nil, nil, e
	}
	wrap, e := Seal(dh[:24], mh[:], random)
	if e != nil {
		return nil, nil, e
	}
	if len(aak) > 24 {
		aak = aak[:24]
	}
	if len(aak) != 0 && len(aak) != 16 && len(aak) != 24 {
		return nil, nil, ErrWire
	}
	aak = bytes.Clone(aak)
	var aw []byte
	if len(aak) != 0 {
		aw, e = Seal(aak, mh[:], random)
		if e != nil {
			return nil, nil, e
		}
	}
	extract := hmac.New(sha256.New, []byte("security hdkf expand"))
	extract.Write(random)
	prk := extract.Sum(nil)
	var previous, derived []byte
	for i := byte(1); i <= 2; i++ {
		m := hmac.New(sha256.New, prk)
		m.Write(previous)
		m.Write(mh[:])
		m.Write([]byte{i})
		previous = m.Sum(nil)
		derived = append(derived, previous...)
	}
	ra := sha256.Sum256(Join(meta, wrap, aw))
	encrypted, e := Seal(derived[:24], ra[:], plain)
	if e != nil {
		return nil, nil, e
	}
	h := &Hybrid{k, aak, Join(derived[24:56], plain), encrypted, signing}
	wire := Join(Uint(1, 1), Blob(2, Join(Uint(1, 415), Blob(2, k.PublicKey().Bytes()))), Blob(3, wrap), Blob(4, aw), Blob(5, encrypted))
	return h, wire, nil
}
func (h *Hybrid) Decrypt(wire []byte) ([]byte, error) {
	f, e := Parse(wire)
	if e != nil {
		return nil, e
	}
	p, e := f.Message(1)
	if e != nil {
		return nil, e
	}
	nid, ok := p.N(1)
	point, pok := p.B(2)
	kind, kok := f.N(2)
	c, cok := f.B(3)
	if !ok || nid != 415 || !pok || !kok || !cok || (kind != 1 && kind != 2) {
		return nil, ErrWire
	}
	// A first login has no credential wrap. Its response must carry the
	// production signing-key proof; type 2 requires existing key material.
	if len(h.aak) == 0 && kind != 1 {
		return nil, ErrWire
	}
	if kind == 1 {
		sig, _ := f.B(4)
		block, _ := pem.Decode(h.signing)
		if block == nil {
			return nil, ErrWire
		}
		pub, e := x509.ParsePKIXPublicKey(block.Bytes)
		if e != nil {
			return nil, ErrWire
		}
		key, ok := pub.(*ecdsa.PublicKey)
		if !ok {
			return nil, ErrWire
		}
		first := sha256.Sum256(Join(h.requestCipher, c))
		second := sha256.Sum256(first[:])
		if !ecdsa.VerifyASN1(key, second[:], sig) {
			return nil, errors.New("server signature rejected")
		}
	}
	peer, e := ecdh.P256().NewPublicKey(point)
	if e != nil {
		return nil, ErrWire
	}
	secret, e := h.private.ECDH(peer)
	if e != nil {
		return nil, e
	}
	key := sha256.Sum256(secret)
	aad := sha256.Sum256(Join(h.aad, []byte("415"), point, []byte(strconv.FormatUint(kind, 10))))
	return Unseal(key[:24], aad[:], c)
}
