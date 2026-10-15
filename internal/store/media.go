package store

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"unicode"
	"unicode/utf8"
)

const MaxMediaBytes = 32 << 20
const mediaMagic = "REA-WX-MEDIA\x01"

var ErrMediaCache = errors.New("media cache is unavailable or unauthenticated")

// MediaJob is persisted only inside the existing encrypted checkpoint. It does
// not retain download keys or media XML. Public APIs select safe fields instead
// of serializing this storage record.
type MediaJob struct {
	Binding             string `json:"account_binding"`
	SessionID           string `json:"session_id"`
	MessageID           string `json:"message_id"`
	Variant             string `json:"variant"`
	Kind                string `json:"kind"`
	Name                string `json:"name"`
	MIME                string `json:"mime,omitempty"`
	Status              string `json:"status"`
	Size                int64  `json:"size_bytes,omitempty"`
	Digest              string `json:"content_digest,omitempty"`
	ChecksumVerified    bool   `json:"checksum_verified,omitempty"`
	CreatedAt           int64  `json:"created_at"`
	Attempts            uint32 `json:"attempts"`
	Error               string `json:"error_category,omitempty"`
	ErrorCode           int64  `json:"error_code,omitempty"`
	ExpectedCipherBytes uint64 `json:"expected_cipher_bytes,omitempty"`
	ReceivedCipherBytes uint64 `json:"received_cipher_bytes,omitempty"`
}

func (j MediaJob) String() string   { return "store.MediaJob(redacted)" }
func (j MediaJob) GoString() string { return j.String() }

func restoreMediaJobs(data *Data) error {
	for id, job := range data.MediaJobs {
		if job == nil || !mediaHex(id, 32) || job.Binding != data.Binding || !mediaHex(job.SessionID, 32) || !mediaHex(job.MessageID, 64) || job.Size < 0 || job.Size > MaxMediaBytes || len(job.Name) > 1024 || !utf8.ValidString(job.Name) || strings.ContainsAny(job.Name, "/\\") {
			return errors.New("media checkpoint task is invalid")
		}
		for _, c := range job.Name {
			if unicode.IsControl(c) || unicode.In(c, unicode.Cf) {
				return errors.New("media checkpoint task is invalid")
			}
		}
		switch job.Variant {
		case "regular", "thumbnail", "original", "attachment":
		default:
			return errors.New("media checkpoint task is invalid")
		}
		if job.Kind != "image" && job.Kind != "file" {
			return errors.New("media checkpoint task is invalid")
		}
		if job.Kind == "file" && job.Variant != "attachment" || job.Kind == "image" && job.Variant == "attachment" {
			return errors.New("media checkpoint task is invalid")
		}
		switch job.Status {
		case "queued", "failed":
		case "downloading":
			job.Status = "failed"
			job.Error = "media_interrupted"
			job.ErrorCode = 0
		case "downloaded":
			if !mediaHex(job.Digest, 64) || job.MIME == "" {
				return errors.New("media checkpoint task is invalid")
			}
		default:
			return errors.New("media checkpoint task is invalid")
		}
	}
	return nil
}

func mediaHex(value string, n int) bool {
	return len(value) == n && strings.Trim(value, "0123456789abcdef") == ""
}

// MediaCache seals validated downloaded bytes using a private per-binding key.
// The binding and opaque job ID are authenticated, so renaming/copying an
// encrypted file cannot move it to another job or account.
type MediaCache struct {
	Directory string `json:"-"`
	Binding   string `json:"-"`
}

func (c MediaCache) String() string   { return "store.MediaCache(redacted)" }
func (c MediaCache) GoString() string { return c.String() }

func (c MediaCache) paths(id string) (string, string, error) {
	if !mediaHex(c.Binding, 64) || !mediaHex(id, 32) {
		return "", "", ErrMediaCache
	}
	return filepath.Join(c.Directory, id+".enc"), filepath.Join(c.Directory, c.Binding+".key"), nil
}

func (c MediaCache) key(path string, create bool) ([]byte, error) {
	if err := Directory(c.Directory); err != nil {
		return nil, ErrMediaCache
	}
	key, err := ReadPrivate(path, 32)
	if err == nil {
		if len(key) != 32 {
			return nil, ErrMediaCache
		}
		return key, nil
	}
	if !create || !errors.Is(err, os.ErrNotExist) {
		return nil, ErrMediaCache
	}
	key = make([]byte, 32)
	if _, err = rand.Read(key); err != nil {
		return nil, ErrMediaCache
	}
	fd, err := syscall.Open(path, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if errors.Is(err, os.ErrExist) {
		return c.key(path, false)
	}
	if err != nil {
		return nil, ErrMediaCache
	}
	file := os.NewFile(uintptr(fd), path)
	if _, err = file.Write(key); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil || closeErr != nil {
		return nil, ErrMediaCache
	}
	return key, nil
}

func (c MediaCache) Save(id string, data []byte) error {
	if len(data) > MaxMediaBytes {
		return ErrMediaCache
	}
	path, keyPath, err := c.paths(id)
	if err != nil {
		return err
	}
	key, err := c.key(keyPath, true)
	if err != nil {
		return err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return ErrMediaCache
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return ErrMediaCache
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return ErrMediaCache
	}
	encrypted := append([]byte(mediaMagic), nonce...)
	encrypted = aead.Seal(encrypted, nonce, data, []byte(mediaMagic+c.Binding+id))
	if err = Atomic(path, encrypted); err != nil {
		return ErrMediaCache
	}
	return nil
}

func (c MediaCache) Load(id string) ([]byte, error) {
	path, keyPath, err := c.paths(id)
	if err != nil {
		return nil, err
	}
	key, err := c.key(keyPath, false)
	if err != nil {
		return nil, err
	}
	encrypted, err := ReadPrivate(path, int64(MaxMediaBytes+len(mediaMagic)+28))
	if err != nil || len(encrypted) < len(mediaMagic)+28 || string(encrypted[:len(mediaMagic)]) != mediaMagic {
		return nil, ErrMediaCache
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrMediaCache
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, ErrMediaCache
	}
	data, err := aead.Open(nil, encrypted[len(mediaMagic):len(mediaMagic)+aead.NonceSize()], encrypted[len(mediaMagic)+aead.NonceSize():], []byte(mediaMagic+c.Binding+id))
	if err != nil || len(data) > MaxMediaBytes {
		return nil, ErrMediaCache
	}
	return data, nil
}

func (c MediaCache) Remove(id string) error {
	path, _, err := c.paths(id)
	if err != nil {
		return err
	}
	if err = os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return ErrMediaCache
	}
	return nil
}
