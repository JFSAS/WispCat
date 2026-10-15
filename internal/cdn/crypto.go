package cdn

import (
	"crypto/aes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	_ "embed"
)

// Static PUBLIC key from MM CDN initialization 4243010, setter56dea10:
// version1, 1024-bit N, E=010001. No account material is embedded.
//
//go:embed legacy-public-key.der
var legacyPublicKeyDER []byte

const LegacyRSAVersion = "1"

func wrapAESKey(key [16]byte) ([]byte, error) {
	public, err := x509.ParsePKCS1PublicKey(legacyPublicKeyDER)
	if err != nil {
		return nil, fail("public_key_unavailable")
	}
	// 565d254 uses RSA_public_encrypt(...,1), not OAEP and not base64.
	wrapped, err := rsa.EncryptPKCS1v15(rand.Reader, public, key[:])
	if err != nil {
		return nil, fail("key_wrap_failed")
	}
	return wrapped, nil
}

func decryptContent(ciphertext []byte, key [16]byte) ([]byte, error) {
	if len(ciphertext) == 0 || len(ciphertext)%aes.BlockSize != 0 {
		return nil, fail("invalid_ciphertext")
	}
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, fail("invalid_key")
	}
	plain := make([]byte, len(ciphertext))
	for offset := 0; offset < len(ciphertext); offset += aes.BlockSize {
		block.Decrypt(plain[offset:offset+aes.BlockSize], ciphertext[offset:offset+aes.BlockSize])
	}
	padding := int(plain[len(plain)-1])
	if padding < 1 || padding > aes.BlockSize || padding > len(plain) {
		return nil, fail("invalid_padding")
	}
	for _, b := range plain[len(plain)-padding:] {
		if int(b) != padding {
			return nil, fail("invalid_padding")
		}
	}
	return plain[:len(plain)-padding], nil
}
