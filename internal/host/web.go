package host

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

//go:embed web/dist
var managementWeb embed.FS

// Only built UI resources are public. Host/Origin checks precede this handler;
// all account data, configuration and mutations still require the admin token.
func serveManagementWeb(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}
	file, kind := "", ""
	if r.URL.Path == "/" {
		file, kind = "index.html", "text/html; charset=utf-8"
	} else if r.URL.Path == "/third-party-notices" {
		file, kind = "THIRD_PARTY_NOTICES.txt", "text/plain; charset=utf-8"
	} else if strings.HasPrefix(r.URL.Path, "/assets/") {
		file = strings.TrimPrefix(r.URL.Path, "/")
		if !fs.ValidPath(file) || strings.Count(file, "/") != 1 {
			return false
		}
		switch path.Ext(file) {
		case ".js":
			kind = "text/javascript; charset=utf-8"
		case ".css":
			kind = "text/css; charset=utf-8"
		default:
			return false
		}
	} else {
		return false
	}
	data, err := managementWeb.ReadFile("web/dist/" + file)
	if err != nil {
		http.NotFound(w, r)
		return true
	}
	w.Header().Set("Content-Type", kind)
	w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' blob: data:; connect-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("X-Frame-Options", "DENY")
	if r.Method == http.MethodGet {
		_, _ = w.Write(data)
	}
	return true
}
