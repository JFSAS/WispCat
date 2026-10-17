package protocol

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"strconv"
	"time"

	"github.com/JFSAS/wispcat/internal/assets"
	"github.com/JFSAS/wispcat/internal/native"
)

const (
	getQRCodeCommand   uint16 = 502
	checkQRCodeCommand uint16 = 503
	manualAuthCommand  uint16 = 252
	getQRCodePath             = "/cgi-bin/micromsg-bin/getloginqrcode"
	checkQRCodePath           = "/cgi-bin/micromsg-bin/checkloginqrcode"
	manualAuthPath            = "/cgi-bin/micromsg-bin/secmanualauth"
)

// LoginIdentity contains a locally created device identity and public runtime
// metadata. No credential imported from an official installation is needed.
type LoginIdentity struct {
	Runtime                         *ClientIdentity
	DeviceID                        []byte
	Profile                         Profile
	Computer, Description, Software string
}

type LoginExchange func(context.Context, uint16, string, []byte, []byte) (Header, []byte, error)

// QRLogin owns one challenge. It must be used by one serial worker. Credentials
// and the server's QR identifier never appear in JSON or diagnostic formatting.
type QRLogin struct {
	identity                                                               LoginIdentity
	exchange                                                               LoginExchange
	uuid, notificationKey, cookie, username, authorization, authorization2 []byte
	image                                                                  []byte
	state                                                                  string
	created, deadline                                                      time.Time
	serverStatus                                                           uint32
	attempted                                                              bool
	expectedUsername                                                       []byte
	expectedUIN                                                            uint32
	diagnostics                                                            LoginDiagnostics
	pollInterval                                                           time.Duration
}

type LoginDiagnostics struct {
	Command            uint16      `json:"command"`
	Stage              string      `json:"stage"`
	Encryption         byte        `json:"response_encryption"`
	ResponseBytes      int         `json:"response_bytes"`
	BaseRetPresent     bool        `json:"base_ret_present"`
	BaseRet            int32       `json:"base_ret"`
	NotifyStatus       uint32      `json:"notify_status"`
	UsernameBytes      int         `json:"username_bytes"`
	AuthorizationBytes int         `json:"authorization_bytes"`
	AuthFlags          uint32      `json:"auth_flags"`
	KeyLengths         map[int]int `json:"key_lengths,omitempty"`
	NotifyFieldLengths map[int]int `json:"notify_field_lengths,omitempty"`
	ChallengeMatches   bool        `json:"challenge_matches"`
	ObservedAt         int64       `json:"observed_at"`
	TransportStage     string      `json:"transport_stage,omitempty"`
	TransportCode      int         `json:"transport_code,omitempty"`
	TLSError           int         `json:"tls_error,omitempty"`
	CertificateResult  int64       `json:"certificate_result,omitempty"`
}

func (d LoginDiagnostics) Clone() LoginDiagnostics {
	if d.KeyLengths != nil {
		m := make(map[int]int, len(d.KeyLengths))
		for k, v := range d.KeyLengths {
			m[k] = v
		}
		d.KeyLengths = m
	}
	if d.NotifyFieldLengths != nil {
		m := make(map[int]int, len(d.NotifyFieldLengths))
		for k, v := range d.NotifyFieldLengths {
			m[k] = v
		}
		d.NotifyFieldLengths = m
	}
	return d
}

func (q *QRLogin) Diagnostics() LoginDiagnostics { return q.diagnostics.Clone() }

type LoginStatus struct {
	State        string `json:"state"`
	CreatedAt    int64  `json:"created_at,omitempty"`
	Deadline     int64  `json:"deadline,omitempty"`
	ImageReady   bool   `json:"image_ready"`
	ServerStatus uint32 `json:"server_status,omitempty"`
}

func (q *QRLogin) Status() LoginStatus {
	s := LoginStatus{State: q.state, ImageReady: len(q.image) > 0, ServerStatus: q.serverStatus}
	if !q.created.IsZero() {
		s.CreatedAt = q.created.Unix()
	}
	if !q.deadline.IsZero() {
		s.Deadline = q.deadline.Unix()
	}
	return s
}
func (q *QRLogin) String() string               { return "protocol.QRLogin(redacted)" }
func (q *QRLogin) GoString() string             { return q.String() }
func (q *QRLogin) MarshalJSON() ([]byte, error) { return json.Marshal(q.Status()) }

func NewQRLogin(identity LoginIdentity, exchange LoginExchange) (*QRLogin, error) {
	if len(identity.DeviceID) < 16 || len(identity.DeviceID) > 64 || identity.Profile.Version == 0 || identity.Profile.DeviceType == "" || exchange == nil {
		return nil, ErrWire
	}
	if identity.Runtime != nil {
		if err := identity.Runtime.Validate(); err != nil {
			return nil, err
		}
		saved := *identity.Runtime
		identity.Runtime = &saved
	}
	identity.DeviceID = bytes.Clone(identity.DeviceID)
	q := &QRLogin{identity: identity, state: "idle"}
	q.exchange = func(ctx context.Context, command uint16, path string, plain, key []byte) (Header, []byte, error) {
		h, response, err := exchange(ctx, command, path, plain, key)
		q.diagnostics.ObservedAt = time.Now().Unix()
		q.diagnostics.Command, q.diagnostics.Encryption, q.diagnostics.ResponseBytes = command, h.Encryption, len(response)
		q.diagnostics.BaseRetPresent, q.diagnostics.BaseRet = false, 0
		q.diagnostics.TransportStage, q.diagnostics.TransportCode, q.diagnostics.TLSError, q.diagnostics.CertificateResult = "", 0, 0, 0
		var transport *native.ExchangeError
		if errors.As(err, &transport) {
			q.diagnostics.TransportStage, q.diagnostics.TransportCode = transport.Stage, transport.Code
			q.diagnostics.TLSError, q.diagnostics.CertificateResult = transport.SSLError, transport.VerifyResult
		}
		if f, e := Parse(response); e == nil {
			if base, e := f.Message(1); e == nil {
				r, p := base.N(1)
				q.diagnostics.BaseRetPresent, q.diagnostics.BaseRet = p, int32(r)
			}
			if command == manualAuthCommand {
				flags, _ := f.N(2)
				q.diagnostics.AuthFlags = uint32(flags)
				q.diagnostics.KeyLengths = map[int]int{}
				if section, e := f.Message(3); e == nil {
					for _, tag := range []int{3, 4, 23, 24} {
						raw, p := section.B(tag)
						if !p {
							q.diagnostics.KeyLengths[tag] = -1
							continue
						}
						v, e := Unbuffer(raw)
						if e != nil {
							q.diagnostics.KeyLengths[tag] = -2
						} else {
							q.diagnostics.KeyLengths[tag] = len(v)
						}
					}
				}
			}
		}
		return h, response, err
	}
	return q, nil
}

func loginBase(identity LoginIdentity, scene uint32) []byte {
	return Join(Blob(1, []byte{0}), Uint(2, 0), Blob(3, identity.DeviceID[:16]), Uint(4, uint64(int64(int32(identity.Profile.Version)))), Blob(5, []byte(identity.Profile.DeviceType)), Uint(6, uint64(scene)))
}

func (q *QRLogin) Start(ctx context.Context, now time.Time) error {
	q.diagnostics.Stage = "request_qr"
	if q.state != "idle" {
		return &Error{Category: "login_already_started"}
	}
	random, err := Random(16)
	if err != nil {
		return err
	}
	request := Join(Blob(1, loginBase(q.identity, 0)), Blob(2, Buffer(random)), Uint(3, 0), Blob(4, []byte(q.identity.Computer)))
	h, plain, err := q.exchange(ctx, getQRCodeCommand, getQRCodePath, request, random)
	if err != nil {
		return err
	}
	f, err := loginResponse(plain)
	if err != nil {
		return err
	}
	uuid, ok := f.B(3)
	imageBuffer, imageOK := f.B(2)
	keyBuffer, keyOK := f.B(5)
	if !ok || len(uuid) == 0 || len(uuid) > 512 || !imageOK || !keyOK {
		return ErrWire
	}
	pixels, err := Unbuffer(imageBuffer)
	if err != nil || len(pixels) == 0 || len(pixels) > 1<<20 {
		return ErrWire
	}
	config, format, err := image.DecodeConfig(bytes.NewReader(pixels))
	if err != nil || (format != "png" && format != "jpeg") || config.Width < 1 || config.Height < 1 || config.Width > 2048 || config.Height > 2048 {
		return ErrWire
	}
	key, err := Unbuffer(keyBuffer)
	if err != nil || len(key) != 16 {
		return ErrWire
	}
	q.uuid, q.notificationKey, q.cookie, q.image = bytes.Clone(uuid), bytes.Clone(key), bytes.Clone(h.Cookie), bytes.Clone(pixels)
	validity := 10 * time.Minute
	if seconds, present := f.N(6); present && seconds > 0 && seconds <= 600 {
		validity = time.Duration(seconds) * time.Second
	}
	q.pollInterval = 2 * time.Second
	if seconds, present := f.N(4); present && seconds >= 1 && seconds <= 30 {
		q.pollInterval = time.Duration(seconds) * time.Second
	}
	q.created, q.deadline, q.state = now, now.Add(validity), "waiting_scan"
	return nil
}

func (q *QRLogin) PollInterval() time.Duration { return q.pollInterval }

func (q *QRLogin) Image() ([]byte, error) {
	if len(q.image) == 0 {
		return nil, &Error{Category: "login_image_missing"}
	}
	return bytes.Clone(q.image), nil
}

func (q *QRLogin) ExpectAccount(expected Bootstrap) {
	q.expectedUsername = bytes.Clone(expected.Username)
	q.expectedUIN = expected.UIN
}

func (q *QRLogin) Poll(ctx context.Context, now time.Time) error {
	q.diagnostics.Stage = "poll_notification"
	if q.state != "waiting_scan" && q.state != "waiting_confirmation" {
		return &Error{Category: "login_not_polling"}
	}
	if !now.Before(q.deadline) {
		q.Cancel("timed_out")
		return &Error{Category: "login_wait_timeout"}
	}
	random, err := Random(16)
	if err != nil {
		return err
	}
	// Native 0x2c711b4 narrows the millisecond result of 0x43811a8 to
	// uint32 before dividing by 1000. Dividing first changes the wire value.
	request := Join(Blob(1, loginBase(q.identity, 0)), Blob(2, Buffer(random)), Blob(3, q.uuid), Uint(4, uint64(uint32(now.UnixMilli())/1000)), Uint(5, 0))
	_, plain, err := q.exchange(ctx, checkQRCodeCommand, checkQRCodePath, request, random)
	if err != nil {
		return err
	}
	q.diagnostics.Stage = "poll_base_response"
	f, err := loginResponse(plain)
	if err != nil {
		// Native CheckLoginQRCode at 0x2c70f08 explicitly maps -2007 to
		// notification state 3. This challenge cannot be used for login.
		if e, ok := err.(*Error); ok && e.Code == -2007 {
			q.serverStatus = 3
			q.Cancel("expired")
			return nil
		}
		return err
	}
	q.diagnostics.Stage = "poll_notify_package"
	container, ok := f.B(3)
	if !ok {
		return ErrWire
	}
	pkg, err := Parse(container)
	if err != nil {
		return err
	}
	wrapped, present := pkg.B(1)
	if !present {
		return ErrWire
	}
	ciphertext, err := Unbuffer(wrapped)
	if err != nil {
		return err
	}
	q.diagnostics.Stage = "poll_notify_decrypt"
	notify, err := decryptLoginNotification(q.notificationKey, ciphertext)
	if err != nil {
		return err
	}
	q.diagnostics.Stage = "poll_notify_parse"
	n, err := Parse(notify)
	if err != nil {
		return err
	}
	uuid, ok := n.B(1)
	status, present := n.N(2)
	q.diagnostics.ChallengeMatches = ok && bytes.Equal(uuid, q.uuid)
	q.diagnostics.NotifyFieldLengths = map[int]int{}
	for tag := range n {
		if v, p := n.B(tag); p {
			q.diagnostics.NotifyFieldLengths[tag] = len(v)
		}
	}
	q.diagnostics.NotifyStatus = uint32(status)
	q.diagnostics.Stage = "poll_challenge_check"
	if !ok || !bytes.Equal(uuid, q.uuid) || !present || status > 0xffffffff {
		return ErrWire
	}
	q.serverStatus = uint32(status)
	q.diagnostics.NotifyStatus = uint32(status)
	user, _ := n.B(3)
	authorizationProbe, _ := n.B(4)
	q.diagnostics.UsernameBytes, q.diagnostics.AuthorizationBytes = len(user), len(authorizationProbe)
	q.diagnostics.Stage = "poll_notify_state"
	switch status {
	case 0:
		q.state = "waiting_scan"
	case 1:
		q.state = "waiting_confirmation"
	case 2:
		q.diagnostics.Stage = "poll_confirmation_fields"
		username, uok := n.B(3)
		authorization, aok := n.B(4)
		if !uok || !aok || len(username) == 0 || len(username) > 512 || len(authorization) == 0 || len(authorization) > 4096 {
			return ErrWire
		}
		q.username, q.authorization = bytes.Clone(username), bytes.Clone(authorization)
		q.state = "confirmed"
	case 3:
		q.Cancel("expired")
	case 4:
		q.Cancel("cancelled")
	default:
		q.Cancel("additional_verification")
		return &Error{Category: "login_additional_verification"}
	}
	return nil
}

func decryptLoginNotification(key, ciphertext []byte) ([]byte, error) {
	if len(key) != 16 || len(ciphertext) < 16 || len(ciphertext)%16 != 0 || len(ciphertext) > 128<<10 {
		return nil, ErrWire
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, ErrWire
	}
	plain := make([]byte, len(ciphertext))
	cipher.NewCBCDecrypter(block, key).CryptBlocks(plain, ciphertext)
	padding := int(plain[len(plain)-1])
	if padding < 1 || padding > 16 || padding > len(plain) {
		return nil, ErrWire
	}
	for _, v := range plain[len(plain)-padding:] {
		if int(v) != padding {
			return nil, ErrWire
		}
	}
	return plain[:len(plain)-padding], nil
}

func loginResponse(plain []byte) (Fields, error) {
	f, err := Parse(plain)
	if err != nil {
		return nil, err
	}
	b, ok := f.B(1)
	if !ok {
		return nil, ErrWire
	}
	base, err := Parse(b)
	if err != nil {
		return nil, err
	}
	ret, present := base.N(1)
	if !present {
		return nil, ErrWire
	}
	if int32(ret) != 0 {
		return nil, &Error{Category: "login_server_rejected", Code: int32(ret)}
	}
	return f, nil
}

func (q *QRLogin) Cancel(state string) {
	q.state = state
	clear(q.notificationKey)
	clear(q.authorization)
	clear(q.authorization2)
	q.notificationKey, q.authorization, q.authorization2, q.uuid, q.username, q.image = nil, nil, nil, nil, nil, nil
}

// Authenticate consumes the confirmation exactly once. A lost response is
// uncertain and must not cause a second use of the same authorization token.
func (q *QRLogin) Authenticate(ctx context.Context) (*Session, error) {
	q.diagnostics.Stage = "manual_authentication"
	if q.state != "confirmed" || q.attempted {
		return nil, &Error{Category: "login_not_confirmed"}
	}
	if len(q.expectedUsername) > 0 && !bytes.Equal(q.expectedUsername, q.username) {
		q.Cancel("failed")
		return nil, &Error{Category: "account_scope_changed"}
	}
	q.attempted = true
	q.state = "authenticating"
	private, x, y, err := elliptic.GenerateKey(elliptic.P224(), rand.Reader)
	if err != nil {
		return nil, err
	}
	random, err := Random(16)
	if err != nil {
		return nil, err
	}
	rsa := Join(Blob(1, Buffer(random)), Blob(2, Join(Uint(1, 713), Blob(2, Buffer(elliptic.Marshal(elliptic.P224(), x, y))))), Blob(3, q.username), Blob(4, q.authorization), Blob(5, q.authorization2))
	baseAuth := Blob(2, Join(Blob(1, nil), Blob(4, Buffer([]byte{0}))))
	_, zone := time.Now().Zone()
	if q.identity.Runtime != nil {
		zone = q.identity.Runtime.TimezoneHours * 3600
	}
	aesPart := Join(Blob(1, loginBase(q.identity, 1)), Blob(2, baseAuth), Blob(4, []byte(q.identity.Software)), Uint(5, 0), Blob(7, []byte(q.identity.Description)), Blob(8, []byte(q.identity.Computer)), Blob(9, []byte(q.identity.Profile.DeviceType)), Blob(10, []byte(q.identity.Profile.Language)), Blob(11, []byte(formatLoginZone(zone))), Uint(13, 1000), Blob(15, []byte(q.identity.Profile.Brand)), Blob(16, q.identity.DeviceID), Blob(17, []byte(q.identity.Computer)), Blob(19, []byte("0")), Uint(22, 2))
	h, plain, err := q.exchange(ctx, manualAuthCommand, manualAuthPath, Join(Blob(1, rsa), Blob(2, aesPart)), random)
	if err != nil {
		q.Cancel("unknown")
		return nil, err
	}
	q.diagnostics.Stage = "manual_base_response"
	if _, err = loginResponse(plain); err != nil {
		q.Cancel("failed")
		return nil, err
	}
	b := Bootstrap{Username: bytes.Clone(q.username), UIN: q.expectedUIN, DeviceID: bytes.Clone(q.identity.DeviceID)}
	q.diagnostics.Stage = "decode_session"
	candidate, err := decodeAuthentication(plain, h, b, q.identity.Profile, private)
	if err != nil {
		q.Cancel("failed")
		return nil, err
	}
	if q.identity.Runtime != nil {
		value := *q.identity.Runtime
		candidate.Identity = &value
	}
	q.Cancel("authenticated")
	return candidate, nil
}

func formatLoginZone(seconds int) string {
	// Native 43895d0 formats the integral UTC offset in hours.
	return strconv.Itoa(seconds / 3600)
}

func (c *Client) QRExchange(ctx context.Context, command uint16, path string, plain, temporary []byte) (Header, []byte, error) {
	if command != getQRCodeCommand && command != checkQRCodeCommand && command != manualAuthCommand {
		return Header{}, nil, ErrWire
	}
	profile, _, err := c.compatibilityIdentity()
	if err != nil {
		return Header{}, nil, err
	}
	resource, err := assetsLoginProfile()
	if err != nil {
		return Header{}, nil, err
	}
	// The account profile has no AAK before first login. The request's random
	// 16-byte key is retained separately as the request/response context.
	hybrid, encrypted, err := NewHybrid(resource.point, resource.signing, nil, plain)
	if err != nil {
		return Header{}, nil, err
	}
	header, err := (Header{Version: profile.Version, Command: command, Original: uint32(len(plain)), Transformed: uint32(len(plain)), Compression: 2, Encryption: 12, Unknown37: resource.id, Config: uint16(profile.Config), Flag: 255, Notice: 0xffffffff, Sequence: 1}).Encode()
	if err != nil {
		return Header{}, nil, err
	}
	h, body, err := c.exchange(ctx, command, path, Join(header, encrypted))
	if err != nil {
		return h, nil, err
	}
	if h.Encryption != 12 {
		return h, nil, &Error{Category: "login_response_format"}
	}
	body, err = hybrid.Decrypt(body)
	return h, body, err
}

type loginCryptoProfile struct {
	point, signing []byte
	id             uint16
}

func assetsLoginProfile() (loginCryptoProfile, error) {
	resource, err := assets.Files.ReadFile("production-profile.json")
	if err != nil {
		return loginCryptoProfile{}, err
	}
	var public struct {
		ECDH    string `json:"ecdh_public_key_hex"`
		Signing string `json:"signing_public_pem"`
		ID      uint16 `json:"profile_id"`
	}
	if json.Unmarshal(resource, &public) != nil {
		return loginCryptoProfile{}, ErrWire
	}
	point, err := hex.DecodeString(public.ECDH)
	if err != nil {
		return loginCryptoProfile{}, ErrWire
	}
	return loginCryptoProfile{point, []byte(public.Signing), public.ID}, nil
}
