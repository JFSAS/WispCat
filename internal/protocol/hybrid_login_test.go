package protocol

import (
	"bytes"
	"crypto/ecdh"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"testing"
)

// This peer derives the request key independently with the standard HKDF
// implementation, then signs a synthetic first-login response.
func TestHybridFirstLoginAndTemporaryKey(t *testing.T) {
	for _, size := range []int{0, 16, 24, 32} {
		t.Run(string(rune('A'+size)), func(t *testing.T) {
			peer, err := ecdh.P256().GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			signer, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			der, err := x509.MarshalPKIXPublicKey(&signer.PublicKey)
			if err != nil {
				t.Fatal(err)
			}
			key := bytes.Repeat([]byte{0x61}, size)
			plain := []byte("synthetic first-login request")
			h, request, err := NewHybrid(peer.PublicKey().Bytes(), pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}), key, plain)
			if err != nil {
				t.Fatal(err)
			}
			fields, err := Parse(request)
			if err != nil {
				t.Fatal(err)
			}
			pub, err := fields.Message(2)
			if err != nil {
				t.Fatal(err)
			}
			point, _ := pub.B(2)
			clientPoint, err := ecdh.P256().NewPublicKey(point)
			if err != nil {
				t.Fatal(err)
			}
			secret, err := peer.ECDH(clientPoint)
			if err != nil {
				t.Fatal(err)
			}
			shared := sha256.Sum256(secret)
			meta := Join([]byte("1415"), point)
			mh := sha256.Sum256(meta)
			wrap, _ := fields.B(3)
			random, err := Unseal(shared[:24], mh[:], wrap)
			if err != nil || len(random) != 32 {
				t.Fatal("first wrap", err)
			}
			aw, present := fields.B(4)
			if !present {
				t.Fatal("empty credential wrap lost wire presence")
			}
			if size == 0 {
				if len(aw) != 0 {
					t.Fatal("first login invented a credential")
				}
			} else {
				used := key
				if len(used) > 24 {
					used = used[:24]
				}
				v, err := Unseal(used, mh[:], aw)
				if err != nil || !bytes.Equal(v, random) {
					t.Fatal("temporary wrap", err)
				}
			}
			derived, err := hkdf.Key(sha256.New, random, []byte("security hdkf expand"), string(mh[:]), 56)
			if err != nil {
				t.Fatal(err)
			}
			ra := sha256.Sum256(Join(meta, wrap, aw))
			ciphertext, _ := fields.B(5)
			decoded, err := Unseal(derived[:24], ra[:], ciphertext)
			if err != nil || !bytes.Equal(decoded, plain) {
				t.Fatal("independent request", err)
			}
			responsePeer, err := ecdh.P256().GenerateKey(rand.Reader)
			if err != nil {
				t.Fatal(err)
			}
			responseSecret, err := responsePeer.ECDH(clientPoint)
			if err != nil {
				t.Fatal(err)
			}
			responseKey := sha256.Sum256(responseSecret)
			responseAAD := sha256.Sum256(Join(derived[24:56], plain, []byte("415"), responsePeer.PublicKey().Bytes(), []byte("1")))
			expected := []byte("synthetic QR response")
			body, err := Seal(responseKey[:24], responseAAD[:], expected)
			if err != nil {
				t.Fatal(err)
			}
			first := sha256.Sum256(Join(ciphertext, body))
			digest := sha256.Sum256(first[:])
			sig, err := ecdsa.SignASN1(rand.Reader, signer, digest[:])
			if err != nil {
				t.Fatal(err)
			}
			response := Join(Blob(1, Join(Uint(1, 415), Blob(2, responsePeer.PublicKey().Bytes()))), Uint(2, 1), Blob(3, body), Blob(4, sig))
			got, err := h.Decrypt(response)
			if err != nil || !bytes.Equal(got, expected) {
				t.Fatal("signed first login", err)
			}
			if size == 0 {
				unsigned := Join(Blob(1, Join(Uint(1, 415), Blob(2, responsePeer.PublicKey().Bytes()))), Uint(2, 2), Blob(3, body))
				if _, err := h.Decrypt(unsigned); err == nil {
					t.Fatal("uncredentialed unsigned response accepted")
				}
			}
		})
	}
}

func TestHybridRejectsPartialTemporaryKeys(t *testing.T) {
	peer, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	for _, size := range []int{1, 15, 17, 23} {
		if _, _, err := NewHybrid(peer.PublicKey().Bytes(), nil, make([]byte, size), nil); err == nil {
			t.Fatal("unsupported temporary key length accepted")
		}
	}
}
