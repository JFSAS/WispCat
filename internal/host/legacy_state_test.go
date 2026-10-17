package host

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/JFSAS/wispcat/internal/store"
)

func writeLegacyPublicID(t *testing.T, directory, account string) int64 {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	if err := store.Atomic(filepath.Join(directory, "onebot.key"), key); err != nil {
		t.Fatal(err)
	}
	data := map[string]any{"version": 1, "next": 10001,
		"ids":     map[string]int64{account + "\x00user\x00owner": 10000},
		"refs":    map[string]any{"10000": map[string]string{"account": account, "kind": "user", "key": "owner"}},
		"cursors": map[string]uint64{}}
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	block, _ := aes.NewCipher(key)
	aead, _ := cipher.NewGCM(block)
	nonce := make([]byte, aead.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		t.Fatal(err)
	}
	sealed := append(nonce, aead.Seal(nil, nonce, raw, []byte("wechat-runtime/onebot/v1"))...)
	if err = store.Atomic(filepath.Join(directory, "onebot.state"), sealed); err != nil {
		t.Fatal(err)
	}
	return 10000
}
