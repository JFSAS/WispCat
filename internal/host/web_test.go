package host

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
)

func TestManagementWebAssetsKeepAPIsAuthenticatedAndOriginBound(t *testing.T) {
	h, err := New(testConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	if err = h.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer h.Close(context.Background())
	request := func(method, path, host, origin string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "http://"+h.Address()+path, nil)
		if host != "" {
			r.Host = host
		}
		r.Header.Set("Origin", origin)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	index := request(http.MethodGet, "/", "", "")
	if index.Code != 200 || !strings.Contains(index.Body.String(), "WispCat") {
		t.Fatal("embedded application missing", index.Code)
	}
	assets := regexp.MustCompile(`(?:src|href)="(/assets/[^" ]+\.(?:js|css))"`).FindAllStringSubmatch(index.Body.String(), -1)
	if len(assets) != 2 {
		t.Fatal("application must load the built script and stylesheet")
	}
	for _, asset := range assets {
		response := request(http.MethodGet, asset[1], "", "")
		if response.Code != 200 || response.Body.Len() == 0 || response.Header().Get("X-Content-Type-Options") != "nosniff" || !strings.Contains(response.Header().Get("Content-Security-Policy"), "script-src 'self'") {
			t.Fatal("static resource is unavailable or lacks browser protections", asset[1])
		}
		if request(http.MethodGet, asset[1], "foreign.invalid", "").Code != 403 || request(http.MethodGet, asset[1], "", "http://foreign.invalid").Code != 403 {
			t.Fatal("asset bypasses Host/Origin restrictions")
		}
		if request(http.MethodHead, asset[1], "", "").Body.Len() != 0 {
			t.Fatal("HEAD returned an asset body")
		}
	}
	for _, path := range []string{"/accounts", "/configuration", "/assets/../configuration", "/assets/nested/../../configuration"} {
		response := request(http.MethodGet, path, "", "")
		if response.Code != 401 {
			t.Fatal("public application handler exposes protected API", path, response.Code)
		}
		if body, _ := io.ReadAll(response.Result().Body); !strings.Contains(string(body), "authentication_required") {
			t.Fatal("unexpected unauthenticated response", path)
		}
	}
	if request(http.MethodPost, "/", "", "").Code != 401 || request(http.MethodGet, "/assets/missing.js", "", "").Code != 404 {
		t.Fatal("static routes accept mutation or unknown resources")
	}
}
