package protocol

import (
	"bufio"
	"bytes"
	"context"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/JFSAS/wispcat/internal/assets"
	"github.com/JFSAS/wispcat/internal/native"
)

type Client struct {
	Bootstrap     Bootstrap
	Profile       *Profile
	Identity      *ClientIdentity
	State         *Session
	Roots         string
	SavedCursor   []byte
	authAttemptAt time.Time
}

func (c *Client) exchange(ctx context.Context, command uint16, path string, packet []byte) (Header, []byte, error) {
	var zero Header
	req := Join([]byte(fmt.Sprintf("POST %s HTTP/1.1\r\nAccept: */*\r\nCache-Control: no-cache\r\nConnection: close\r\nContent-Length: %d\r\nContent-Type: application/octet-stream\r\nHost: short.weixin.qq.com\r\nUser-Agent: MicroMessenger Client\r\n\r\n", path, len(packet))), packet)
	raw, e := native.Exchange(ctx, "short.weixin.qq.com", c.Roots, req)
	if e != nil {
		return zero, nil, e
	}
	if e = validateHTTPResponse(raw); e != nil {
		return zero, nil, e
	}
	resp, e := http.ReadResponse(bufio.NewReader(bytes.NewReader(raw)), nil)
	if e != nil {
		return zero, nil, ErrWire
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return zero, nil, &Error{"http_response_failed", int32(resp.StatusCode)}
	}
	body, e := io.ReadAll(io.LimitReader(resp.Body, 4<<20+1))
	if e != nil || len(body) > 4<<20 {
		return zero, nil, ErrWire
	}
	h, n, e := DecodeHeader(body)
	if e != nil {
		return zero, nil, e
	}
	if h.Command != command {
		return zero, nil, &Error{"response_command_mismatch", 0}
	}
	return h, body[n:], nil
}
func (c *Client) sessionExchange(ctx context.Context, cmd uint16, path string, plain []byte) ([]byte, error) {
	s := c.State
	if s == nil {
		return nil, &Error{"session_missing", 0}
	}
	s.Sequence = uint16(uint32(s.Sequence)%65535 + 1)
	encrypted, e := Seal(s.ClientKey, nil, plain)
	if e != nil {
		return nil, e
	}
	h := Header{Version: s.Profile.Version, UIN: s.Bootstrap.UIN, Command: cmd, Original: uint32(len(plain)), Transformed: uint32(len(plain)), Cookie: s.Cookie, Compression: 2, Encryption: 13, Config: uint16(s.Profile.Config), Checksum: Checksum(s.Bootstrap.UIN, plain), Flag: 255, Notice: 0xffffffff, Sequence: s.Sequence}
	head, e := h.Encode()
	if e != nil {
		return nil, e
	}
	r, p, e := c.exchange(ctx, cmd, path, Join(head, encrypted))
	if e != nil {
		return nil, e
	}
	ret := int32(r.Version)
	if ret != 0 {
		category := "gateway_rejected"
		if ret == -13 {
			category = "session_expired"
		}
		return nil, &Error{category, ret}
	}
	if r.Encryption != 13 || (r.UIN != 0 && r.UIN != s.Bootstrap.UIN) {
		return nil, &Error{"session_header_mismatch", 0}
	}
	return Unseal(s.ServerKey, nil, p)
}
func (c *Client) Sync(ctx context.Context, scene uint32, cursor []byte) (SyncResult, error) {
	if c.State == nil {
		return SyncResult{}, &Error{"session_missing", 0}
	}
	if cursor == nil {
		cursor = c.State.Cursor
	}
	if len(cursor) == 0 {
		cursor = []byte{0}
	}
	request := Join(Blob(1, Uint(1, 0)), Uint(2, 7), Blob(3, Buffer(cursor)), Uint(4, uint64(scene)), Blob(5, []byte(c.State.Profile.DeviceType)), Uint(9, 0))
	plain, e := c.sessionExchange(ctx, 138, "/cgi-bin/micromsg-bin/newsync", request)
	if e != nil {
		return SyncResult{}, e
	}
	r, e := DecodeSync(plain)
	if e != nil {
		return r, e
	}
	c.State.Cursor, e = MergeCursor(c.State.Cursor, r.Cursor)
	if e == nil {
		c.State.Confirmed = true
	}
	return r, e
}
func (c *Client) Send(ctx context.Context, recipient, content string, when, id uint32) (Receipt, error) {
	return c.SendWithSource(ctx, recipient, content, when, id, "")
}
func (c *Client) SendWithSource(ctx context.Context, recipient, content string, when, id uint32, source string) (Receipt, error) {
	item := Join(Blob(1, String([]byte(recipient))), Blob(2, []byte(content)), Uint(3, 1), Uint(4, uint64(when)), Uint(5, uint64(id)))
	if source != "" {
		item = append(item, Blob(6, []byte(source))...)
	}
	plain, e := c.sessionExchange(ctx, 522, "/cgi-bin/micromsg-bin/newsendmsg", Join(Uint(1, 1), Blob(2, item)))
	if e != nil {
		return Receipt{}, e
	}
	return DecodeReceipt(plain, []byte(recipient))
}
func (c *Client) List(ctx context.Context, a, b uint32) (ContactPage, error) {
	var r ContactPage
	request := Join(Blob(1, c.Bootstrap.Username), Uint(2, uint64(a)), Uint(3, uint64(b)))
	plain, e := c.sessionExchange(ctx, 851, "/cgi-bin/micromsg-bin/initcontact", request)
	if e != nil {
		return r, e
	}
	f, e := Parse(plain)
	if e != nil {
		return r, e
	}
	values := []*uint32{&r.CursorA, &r.CursorB, &r.Continue}
	for i, target := range values {
		v, ok := f.N(i + 2)
		if !ok || v > 0xffffffff {
			return r, ErrWire
		}
		*target = uint32(v)
	}
	r.Usernames = f.All(5)
	if len(r.Usernames) > 20000 {
		return r, ErrWire
	}
	for _, u := range r.Usernames {
		if len(u) == 0 || len(u) > 512 {
			return r, ErrWire
		}
	}
	return r, nil
}
func (c *Client) Brief(ctx context.Context, names [][]byte) ([]Brief, error) {
	if len(names) == 0 || len(names) > 80 {
		return nil, ErrWire
	}
	var request []byte
	expected := map[string]bool{}
	for _, u := range names {
		if len(u) == 0 || expected[string(u)] {
			return nil, ErrWire
		}
		expected[string(u)] = true
		request = append(request, Blob(1, u)...)
	}
	plain, e := c.sessionExchange(ctx, 945, "/cgi-bin/micromsg-bin/batchgetcontactbriefinfo", request)
	if e != nil {
		return nil, e
	}
	f, e := Parse(plain)
	if e != nil {
		return nil, e
	}
	var result []Brief
	seen := map[string]bool{}
	for _, raw := range f.All(2) {
		v, e := Parse(raw)
		if e != nil {
			return nil, e
		}
		name, ok := v.B(1)
		status, p := v.N(2)
		if !ok || !p || !expected[string(name)] || seen[string(name)] {
			return nil, ErrWire
		}
		seen[string(name)] = true
		r := Brief{Username: name, Status: int32(status)}
		if r.Status == 0 {
			b, ok := v.B(3)
			if !ok {
				return nil, ErrWire
			}
			contact, e := ParseContact(b, false)
			if e != nil || !bytes.Equal(contact.Username, name) {
				return nil, ErrWire
			}
			r.Contact = &contact
		}
		result = append(result, r)
	}
	if len(result) == 0 || len(result) > 80 {
		return nil, ErrWire
	}
	return result, nil
}
func (c *Client) Authenticate(ctx context.Context) error {
	if !c.authAttemptAt.IsZero() && time.Since(c.authAttemptAt) < 10*time.Second {
		return &Error{Category: "authentication_backoff"}
	}
	c.authAttemptAt = time.Now()
	b := c.Bootstrap
	if b.MLCert || len(b.DeviceID) < 16 {
		return ErrWire
	}
	profile, identity, e := c.compatibilityIdentity()
	if e != nil {
		return e
	}
	curve := elliptic.P224()
	private, x, y, e := elliptic.GenerateKey(curve, rand.Reader)
	if e != nil {
		return e
	}
	point := elliptic.Marshal(curve, x, y)
	computer, description := identity.Computer, identity.Description()
	zone := identity.TimezoneHours * 3600
	base := Join(Blob(1, []byte{0}), Uint(2, uint64(b.UIN)), Blob(3, b.DeviceID[:16]), Uint(4, uint64(int64(int32(profile.Version)))), Blob(5, []byte(profile.DeviceType)), Uint(6, 2))
	auth := Join(Blob(2, Blob(4, Buffer([]byte{0}))), Blob(5, Buffer(b.KeyInfo)))
	rsa := Join(Blob(2, Buffer(b.First)), Blob(3, Join(Uint(1, 713), Blob(2, Buffer(point)))))
	aesPart := Join(Blob(1, base), Blob(2, auth), Blob(3, Buffer(b.AAK)), Uint(6, 0), Blob(8, []byte(description)), Blob(9, []byte(computer)), Blob(10, []byte("Apple")), Blob(11, []byte(profile.Language)), Blob(12, []byte(strconv.Itoa(zone/3600))), Blob(19, []byte("Apple")))
	plain := Join(Blob(1, rsa), Blob(2, aesPart))
	resource, e := assets.Files.ReadFile("production-profile.json")
	if e != nil {
		return e
	}
	var public struct {
		ECDH    string `json:"ecdh_public_key_hex"`
		Signing string `json:"signing_public_pem"`
		ID      uint16 `json:"profile_id"`
	}
	if e = json.Unmarshal(resource, &public); e != nil {
		return e
	}
	serverPoint, e := hex.DecodeString(public.ECDH)
	if e != nil {
		return e
	}
	hybrid, encrypted, e := NewHybrid(serverPoint, []byte(public.Signing), b.AAK, plain)
	if e != nil {
		return e
	}
	head, e := (Header{Version: profile.Version, UIN: b.UIN, Command: 763, Original: uint32(len(plain)), Transformed: uint32(len(plain)), Cookie: b.Cookie, Compression: 2, Encryption: 12, Unknown37: public.ID, Config: uint16(profile.Config), Flag: 255, Notice: 0xffffffff, Sequence: 1}).Encode()
	if e != nil {
		return e
	}
	h, response, e := c.exchange(ctx, 763, "/cgi-bin/micromsg-bin/secautoauth", Join(head, encrypted))
	if e != nil {
		return e
	}
	if h.Encryption != 12 {
		return &AuthenticationError{Reason: "response_format_mismatch"}
	}
	response, e = hybrid.Decrypt(response)
	if e != nil {
		return e
	}
	candidate, e := decodeAuthentication(response, h, b, profile, private)
	if e != nil {
		return e
	}
	candidate.Identity = &identity
	candidate.Cursor = bytes.Clone(c.SavedCursor)
	candidate.AuthGeneration = 1
	if c.State != nil {
		candidate.Cursor = bytes.Clone(c.State.Cursor)
		candidate.AuthGeneration = c.State.AuthGeneration + 1
	}
	c.Bootstrap, c.State = candidate.Bootstrap, candidate
	return nil
}
