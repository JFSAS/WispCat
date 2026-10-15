// Package account reads a hash-guarded, current-user disk bootstrap. It never
// reads process memory and never returns account values in error messages.
package account

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/JFSAS/wechat-runtime/internal/assets"
	"github.com/JFSAS/wechat-runtime/internal/native"
	"github.com/JFSAS/wechat-runtime/internal/protocol"
)

const DefaultApp = "/Applications/WeChat.app"

type Source struct{ App string }

const MainSHA = "a9df8843c8ee9a7e018f4a3580ef7ce39f2e1227e9174b18cdd1f087b9969ae8"
const LibrarySHA = "6e82322680d7747020c305f67e932b235fe68b22578c24d9c524dabb31a4694a"

var ErrDiskPlatform = errors.New("official_disk_import_requires_macos")

var ErrLogin = errors.New("please log in to the supported official WeChat account")
var ErrVersion = errors.New("installed WeChat version is not supported")
var ErrDiskNotFound = errors.New("official WeChat disk configuration not found")
var ErrDiskAmbiguous = errors.New("multiple official WeChat disk configurations found")

// ImportError records the failed disk-import step without including account
// names, paths, credentials, or the underlying error text in diagnostics.
type ImportError struct {
	Stage string
	Err   error
}

func (e *ImportError) Error() string { return "official_disk_import_failed" }
func (e *ImportError) Unwrap() error { return e.Err }

func (source Source) Verify() error {
	if runtime.GOOS != "darwin" {
		return ErrDiskPlatform
	}
	profile, err := source.profile()
	if err != nil {
		return ErrVersion
	}
	app, err := ResolveApp(source.App)
	if err != nil || app.BundleID != profile.BundleID {
		return ErrVersion
	}
	mainHash, libraryHash := profile.MainHash, profile.LibraryHash
	for path, want := range map[string]string{"Contents/MacOS/WeChat": mainHash, "Contents/Resources/wechat.dylib": libraryHash} {
		f, e := os.Open(filepath.Join(source.App, path))
		if e != nil {
			return ErrVersion
		}
		h := sha256.New()
		_, readErr := io.Copy(h, f)
		closeErr := f.Close()
		if readErr != nil || closeErr != nil || hex.EncodeToString(h.Sum(nil)) != want {
			return ErrVersion
		}
	}
	return nil
}

type Scope struct {
	PID          string
	Config, Root string
}

func (source Source) CurrentScope() (Scope, error) {
	return resolveScope(source.diskScope, source.processScope)
}

// A running process is only a locator for nonstandard disk locations. Missing
// login fields or ambiguous disk files do not justify selecting another account.
func resolveScope(disk, process func() (Scope, error)) (Scope, error) {
	scope, err := disk()
	if errors.Is(err, ErrDiskNotFound) {
		return process()
	}
	return scope, err
}

func (source Source) diskScope() (Scope, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Scope{}, errors.New("current user home directory is unavailable")
	}
	return source.diskScopeAt(home)
}

func (source Source) diskScopeAt(home string) (Scope, error) {
	profile, err := source.profile()
	if err != nil {
		return Scope{}, err
	}
	bundle := profile.BundleID
	container := filepath.Join(home, "Library", "Containers", bundle, "Data")
	var found []string
	for _, root := range []string{
		filepath.Join(container, "Documents", "xwechat_files"),
		filepath.Join(container, "Library", "Application Support", bundle, "xwechat_files"),
		filepath.Join(home, "Library", "Application Support", bundle, "xwechat_files"),
	} {
		path := filepath.Join(root, "all_users", "config", "global_config")
		info, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil || !info.Mode().IsRegular() {
			return Scope{}, errors.New("official WeChat disk configuration is unavailable")
		}
		found = append(found, path)
	}
	if len(found) == 0 {
		return Scope{}, ErrDiskNotFound
	}
	if len(found) != 1 {
		return Scope{}, ErrDiskAmbiguous
	}
	return Scope{Config: found[0]}, nil
}

func (source Source) processScope() (Scope, error) {
	var s Scope
	p, e := exec.Command("/usr/bin/pgrep", "-x", "WeChat").Output()
	if e != nil {
		return s, ErrLogin
	}
	var matching []string
	for _, pid := range strings.Fields(string(p)) {
		out, e := exec.Command("/bin/ps", "-p", pid, "-o", "comm=").Output()
		if e == nil && strings.TrimSpace(string(out)) == filepath.Join(source.App, "Contents/MacOS/WeChat") {
			matching = append(matching, pid)
		}
	}
	if len(matching) != 1 {
		return s, ErrLogin
	}
	s.PID = matching[0]
	out, e := exec.Command("/usr/sbin/lsof", "-a", "-p", s.PID, "-Fn").Output()
	if e != nil {
		return s, ErrLogin
	}
	roots, configs := map[string]bool{}, map[string]bool{}
	for _, line := range strings.Split(string(out), "\n") {
		if !strings.HasPrefix(line, "n") {
			continue
		}
		path := line[1:]
		if strings.HasSuffix(path, "/xwechat_files/all_users/config/global_config") {
			configs[path] = true
		}
		at := strings.Index(path, "/xwechat_files/")
		if at >= 0 {
			rest := strings.Split(path[at+len("/xwechat_files/"):], "/")
			if len(rest) > 2 && rest[0] != "all_users" && (rest[1] == "config" || rest[1] == "db_storage") {
				roots[rest[0]] = true
			}
		}
	}
	if len(roots) != 1 || len(configs) != 1 {
		return s, ErrLogin
	}
	for r := range roots {
		s.Root = r
	}
	for c := range configs {
		s.Config = c
	}
	return s, nil
}
func Correlates(root string, username []byte) bool {
	u := string(username)
	return u != "" && (root == u || strings.HasPrefix(root, u+"_"))
}

func protectionKey() ([]byte, error) {
	b, e := assets.Files.ReadFile("protection.json")
	if e != nil {
		return nil, e
	}
	var p struct {
		Key string `json:"key_hex"`
		SHA string `json:"key_sha256"`
	}
	if e = json.Unmarshal(b, &p); e != nil {
		return nil, e
	}
	key, e := hex.DecodeString(p.Key)
	if e != nil || len(key) != 16 {
		return nil, protocol.ErrWire
	}
	h := sha256.Sum256(key)
	if hex.EncodeToString(h[:]) != p.SHA {
		return nil, protocol.ErrWire
	}
	return key, nil
}
func readNumber(b []byte, at *int) (uint64, error) {
	if *at > len(b) {
		return 0, protocol.ErrWire
	}
	v, n := binary.Uvarint(b[*at:])
	if n <= 0 {
		return 0, protocol.ErrWire
	}
	*at += n
	return v, nil
}
func readBlob(b []byte, at *int) ([]byte, error) {
	n, e := readNumber(b, at)
	if e != nil || n > uint64(len(b)-*at) {
		return nil, protocol.ErrWire
	}
	v := b[*at : *at+int(n)]
	*at += int(n)
	return v, nil
}
func diskFields(path string) (map[string][]byte, error) {
	key, e := protectionKey()
	if e != nil {
		return nil, e
	}
	var plain []byte
	for i := 0; i < 3; i++ {
		before, e := os.ReadFile(path + ".crc")
		if e != nil {
			return nil, ErrLogin
		}
		data, e := os.ReadFile(path)
		if e != nil {
			return nil, ErrLogin
		}
		after, e := os.ReadFile(path + ".crc")
		if e != nil || len(before) < 112 || len(after) < 112 || len(data) < 4 || len(data) > 4<<20 {
			return nil, ErrLogin
		}
		if string(before[:112]) != string(after[:112]) {
			continue
		}
		version := binary.LittleEndian.Uint32(before[4:])
		if version != 3 && version != 4 {
			return nil, ErrLogin
		}
		if version == 4 && binary.LittleEndian.Uint64(before[104:])&1 != 0 {
			return nil, ErrLogin
		}
		n := binary.LittleEndian.Uint32(before[28:])
		if n > uint32(len(data)-4) {
			return nil, ErrLogin
		}
		encrypted := data[4 : 4+int(n)]
		if crc32.ChecksumIEEE(encrypted) != binary.LittleEndian.Uint32(before) {
			continue
		}
		block, e := aes.NewCipher(key)
		if e != nil {
			return nil, e
		}
		plain = make([]byte, len(encrypted))
		cipher.NewCFBDecrypter(block, before[12:28]).XORKeyStream(plain, encrypted)
		break
	}
	if len(plain) == 0 {
		return nil, ErrLogin
	}
	at := 0
	if _, e = readNumber(plain, &at); e != nil {
		return nil, e
	}
	fields := map[string][]byte{}
	allowed := map[string]bool{"mmkv_key_user_name": true, "mmkv_key_latest_login_username": true, "mmkv_key_latest_login_uin": true, "mmkv_key_auto_auth_key": true, "mmkv_key_server_id": true, "key_wechat_device_id": true, "key_wechat_local_id_64": true, "use_ml_cert": true}
	for count := 0; at < len(plain); count++ {
		if count > 100000 {
			return nil, protocol.ErrWire
		}
		k, e := readBlob(plain, &at)
		if e != nil || len(k) > 4096 {
			return nil, protocol.ErrWire
		}
		v, e := readBlob(plain, &at)
		if e != nil {
			return nil, e
		}
		if !allowed[string(k)] {
			continue
		}
		if len(v) == 0 {
			delete(fields, string(k))
		} else {
			fields[string(k)] = v
		}
	}
	return fields, nil
}
func (source Source) Load() (b protocol.Bootstrap, err error) {
	stage := "verify_installation"
	defer func() {
		if err != nil {
			err = &ImportError{Stage: stage, Err: err}
		}
	}()
	if e := source.Verify(); e != nil {
		return b, e
	}
	stage = "locate_files"
	scope, e := source.CurrentScope()
	if e != nil {
		return b, e
	}
	stage = "read_config"
	fields, e := diskFields(scope.Config)
	if e != nil {
		return b, e
	}
	stage = "decode_credentials"
	stringsMap := map[string]*[]byte{"mmkv_key_latest_login_username": &b.Username, "mmkv_key_auto_auth_key": &b.AAK, "mmkv_key_server_id": &b.Cookie, "key_wechat_device_id": &b.DeviceID}
	for name, target := range stringsMap {
		raw, ok := fields[name]
		if !ok {
			return b, ErrLogin
		}
		at := 0
		*target, e = readBlob(raw, &at)
		if e != nil || at != len(raw) {
			return b, ErrLogin
		}
	}
	raw := fields["mmkv_key_user_name"]
	at := 0
	auto, e := readBlob(raw, &at)
	if e != nil || string(auto) != string(b.Username) || (scope.Root != "" && !Correlates(scope.Root, b.Username)) {
		return b, ErrLogin
	}
	at = 0
	uin, e := readNumber(fields["mmkv_key_latest_login_uin"], &at)
	if e != nil || uin == 0 || uin > 0xffffffff || at != len(fields["mmkv_key_latest_login_uin"]) {
		return b, ErrLogin
	}
	b.UIN = uint32(uin)
	at = 0
	b.LocalID, e = readNumber(fields["key_wechat_local_id_64"], &at)
	if e != nil || b.LocalID == 0 || at != len(fields["key_wechat_local_id_64"]) {
		return b, ErrLogin
	}
	if len(b.DeviceID) < 16 || len(b.Cookie) > 15 {
		return b, ErrLogin
	}
	a, e := protocol.Parse(b.AAK)
	if e != nil {
		return b, e
	}
	first, p := a.B(1)
	second, q := a.B(2)
	if !p || !q {
		return b, ErrLogin
	}
	b.First, e = protocol.Unbuffer(first)
	if e != nil {
		return b, e
	}
	b.Second, e = protocol.Unbuffer(second)
	if e != nil {
		return b, e
	}
	if len(b.First) < 24 {
		return b, ErrLogin
	}
	if ml, ok := fields["use_ml_cert"]; ok {
		at = 0
		v, e := readNumber(ml, &at)
		if e != nil || v > 1 || at != len(ml) {
			return b, ErrLogin
		}
		b.MLCert = v == 1
	}
	if b.MLCert {
		stage = "crypto_profile"
		return b, errors.New("alternate account crypto profile is not supported")
	}
	stage = "read_key_info"
	digest := md5.Sum(b.Username)
	owner := hex.EncodeToString(digest[:])
	ids := []string{string(b.Username), strconv.FormatUint(uin, 10), strconv.FormatUint(uin, 16), fmt.Sprintf("%08x", uin), owner}
	var candidates []string
	seen := map[string]bool{}
	for _, id := range ids {
		if id == "" || strings.ContainsAny(id, "/\\") || seen[id] {
			continue
		}
		seen[id] = true
		path := filepath.Join(filepath.Dir(filepath.Dir(scope.Config)), "login", id, "key_info.db")
		if info, e := os.Stat(path); e == nil && !info.IsDir() {
			candidates = append(candidates, path)
		}
	}
	if len(candidates) != 1 {
		return b, ErrLogin
	}
	value, expected, e := native.KeyInfo(candidates[0], owner)
	if e != nil {
		return b, e
	}
	stage = "validate_key_info"
	h := md5.Sum(value)
	if hex.EncodeToString(h[:]) != strings.ToLower(expected) {
		return b, ErrLogin
	}
	b.KeyInfo = value
	// Recheck the disk snapshot, not the official process. An application can
	// exit or show its login page without invalidating our independent session.
	stage = "validate_snapshot"
	final, e := diskFields(scope.Config)
	if e != nil || len(final) != len(fields) {
		return b, ErrLogin
	}
	for name, value := range fields {
		if !bytes.Equal(value, final[name]) {
			return b, ErrLogin
		}
	}
	return b, nil
}
