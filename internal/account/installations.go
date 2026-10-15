package account

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

var ErrAppPath = errors.New("invalid_wechat_app_path")
var ErrAppMissing = errors.New("wechat_app_not_found")
var ErrAppMetadata = errors.New("wechat_app_metadata_unavailable")

type installationProfile struct{ Path, BundleID, MainHash, LibraryHash string }

var installationProfiles = []installationProfile{
	{DefaultApp, "com.tencent.xinWeChat", MainSHA, LibrarySHA},
	{"/Applications/微信2.app", "com.tencent.xinWeChat2", "a6fc346d1a687b55206c03f8fc79bfe9f5b5b7355b04ec36ef1c59c38a54388f", "f088f36dd5fde89216d3dfc3f6acc095ccfaa7bb25f070f2be5b438408341c11"},
}

type Installation struct {
	Path      string `json:"path"`
	Name      string `json:"name"`
	BundleID  string `json:"bundle_id,omitempty"`
	Version   string `json:"version,omitempty"`
	Available bool   `json:"available"`
	Stored    bool   `json:"stored,omitempty"`
}

func NormalizeAppPath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if len(path) > 4096 || !utf8.ValidString(path) || !filepath.IsAbs(path) || strings.ContainsAny(path, "\x00\r\n") {
		return "", ErrAppPath
	}
	path = filepath.Clean(path)
	if !strings.EqualFold(filepath.Ext(path), ".app") {
		return "", ErrAppPath
	}
	return path, nil
}

// Read only bundle metadata. Listing/choosing an App never reads login material.
func inspectApp(path string) (Installation, error) {
	path, err := NormalizeAppPath(path)
	if err != nil {
		return Installation{}, err
	}
	result := Installation{Path: path, Name: strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))}
	info, err := os.Stat(path)
	if err != nil || !info.IsDir() {
		return result, ErrAppMissing
	}
	f, err := os.Open(filepath.Join(path, "Contents", "Info.plist"))
	if err != nil {
		return result, ErrAppMetadata
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil || !stat.Mode().IsRegular() || stat.Size() > 1<<20 {
		return result, ErrAppMetadata
	}
	raw, err := io.ReadAll(io.LimitReader(f, 1<<20+1))
	if err != nil || len(raw) > 1<<20 {
		return result, ErrAppMetadata
	}
	values, err := bundleMetadata(raw)
	if err != nil {
		return result, err
	}
	result.BundleID, result.Version = values["CFBundleIdentifier"], values["CFBundleShortVersionString"]
	if result.BundleID == "" || len(result.BundleID) > 255 || len(result.Version) > 80 || strings.ContainsAny(result.Version, "\x00\r\n") {
		return result, ErrAppMetadata
	}
	result.Available = true
	return result, nil
}

// Bound conversion output as it is produced, including adversarial plist references.
type metadataOutput struct{ bytes.Buffer }

func (output *metadataOutput) Write(p []byte) (int, error) {
	if len(p) > 4<<20-output.Len() {
		return 0, ErrAppMetadata
	}
	return output.Buffer.Write(p)
}

func bundleMetadata(raw []byte) (map[string]string, error) {
	values := map[string]string{}
	if bytes.HasPrefix(raw, []byte("bplist")) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		command := exec.CommandContext(ctx, "/usr/bin/plutil", "-convert", "json", "-o", "-", "-")
		command.Stdin = bytes.NewReader(raw)
		output := &metadataOutput{}
		command.Stdout = output
		err := command.Run()
		if err != nil {
			return nil, ErrAppMetadata
		}
		var all map[string]json.RawMessage
		if json.Unmarshal(output.Bytes(), &all) != nil {
			return nil, ErrAppMetadata
		}
		// Decode only public string metadata, never arbitrary plist contents.
		for _, key := range []string{"CFBundleIdentifier", "CFBundleShortVersionString"} {
			var value string
			if json.Unmarshal(all[key], &value) == nil {
				values[key] = value
			}
		}
		return values, nil
	}
	var plist struct {
		Dict struct {
			Nodes []struct {
				XMLName xml.Name
				Value   string `xml:",chardata"`
			} `xml:",any"`
		} `xml:"dict"`
	}
	if xml.Unmarshal(raw, &plist) != nil {
		return nil, ErrAppMetadata
	}
	nodes := plist.Dict.Nodes
	for i := 0; i+1 < len(nodes); i++ {
		if nodes[i].XMLName.Local == "key" && nodes[i+1].XMLName.Local == "string" {
			key := nodes[i].Value
			if key == "CFBundleIdentifier" || key == "CFBundleShortVersionString" {
				values[key] = nodes[i+1].Value
			}
		}
	}
	return values, nil
}
func profileByBundle(bundle string) (installationProfile, bool) {
	for _, profile := range installationProfiles {
		if profile.BundleID == bundle {
			return profile, true
		}
	}
	return installationProfile{}, false
}
func ResolveApp(path string) (Installation, error) {
	app, err := inspectApp(path)
	if err != nil {
		return app, err
	}
	if _, ok := profileByBundle(app.BundleID); !ok {
		return app, ErrVersion
	}
	// Retain the original source guards for the two canonical installations.
	for _, p := range installationProfiles {
		if app.Path == p.Path && app.BundleID != p.BundleID {
			return app, ErrVersion
		}
	}
	return app, nil
}
func (source Source) profile() (installationProfile, error) {
	for _, profile := range installationProfiles {
		if filepath.Clean(source.App) == profile.Path {
			return profile, nil
		}
	}
	app, err := ResolveApp(source.App)
	if err != nil {
		return installationProfile{}, ErrVersion
	}
	profile, _ := profileByBundle(app.BundleID)
	return profile, nil
}
func DiscoverApps(selected string) []Installation {
	roots := []string{"/Applications"}
	if home, err := os.UserHomeDir(); err == nil {
		roots = append(roots, filepath.Join(home, "Applications"))
	}
	return discoverAppsAt(roots, selected)
}
func discoverAppsAt(roots []string, selected string) []Installation {
	candidates := map[string]bool{}
	for _, root := range roots {
		entries, err := os.ReadDir(root)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			name := entry.Name()
			lower := strings.ToLower(name)
			if strings.HasSuffix(lower, ".app") && (strings.Contains(lower, "wechat") || strings.Contains(name, "微信")) {
				candidates[filepath.Join(root, name)] = true
			}
		}
	}
	if path, err := NormalizeAppPath(selected); err == nil {
		candidates[path] = true
	}
	result := []Installation{}
	for path := range candidates {
		app, err := ResolveApp(path)
		if err == nil {
			result = append(result, app)
		} else if path == selected {
			result = append(result, Installation{Path: path, Name: strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Path < result[j].Path })
	return result
}
