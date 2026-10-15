package protocol

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/json"
	"fmt"
	"image"
	"image/png"
	"testing"
	"time"
)

func qrTestImage(t *testing.T) []byte {
	t.Helper()
	var b bytes.Buffer
	if err := png.Encode(&b, image.NewRGBA(image.Rect(0, 0, 1, 1))); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}
func qrTestIdentity() LoginIdentity {
	runtime, _ := NewClientIdentity([]byte("synthetic-new-device-id-0000000000"))
	return LoginIdentity{Runtime: &runtime, DeviceID: []byte("synthetic-new-device-id-0000000000"), Profile: Profile{Version: 0xf2641d22, Config: 38, DeviceType: "synthetic", Language: "zh_CN", Brand: "Apple"}, Computer: "synthetic-host"}
}
func qrTestNotify(key []byte, status uint32, uuid string) []byte {
	p := Join(Blob(1, []byte(uuid)), Uint(2, uint64(status)), Blob(3, []byte("synthetic-owner")), Blob(4, []byte("synthetic-one-time-authorization")))
	padding := 16 - len(p)%16
	p = append(p, bytes.Repeat([]byte{byte(padding)}, padding)...)
	b, _ := aes.NewCipher(key)
	c := make([]byte, len(p))
	cipher.NewCBCEncrypter(b, key).CryptBlocks(c, p)
	return Join(Blob(1, Uint(1, 0)), Blob(3, Join(Blob(1, Buffer(c)), Uint(2, 0))))
}
func qrTestSession() []byte {
	aak := Join(Blob(1, Buffer(bytes.Repeat([]byte{0x11}, 32))), Blob(2, Buffer([]byte("synthetic-ticket"))))
	auth := Join(Uint(1, 123), Blob(3, Buffer(bytes.Repeat([]byte{2}, 16))), Blob(4, Buffer(aak)), Blob(23, Buffer(bytes.Repeat([]byte{3}, 24))), Blob(24, Buffer(bytes.Repeat([]byte{4}, 24))))
	return Join(Blob(1, Uint(1, 0)), Uint(2, 1), Blob(3, auth), Blob(4, Blob(1, []byte("synthetic-owner"))))
}
func TestIndependentQRFlowAndOneTimeConfirmation(t *testing.T) {
	ctx := context.Background()
	key := bytes.Repeat([]byte{0x31}, 16)
	now := time.Unix(1700000000, 0)
	calls := []uint16{}
	q, err := NewQRLogin(qrTestIdentity(), func(ctx context.Context, cmd uint16, path string, p, random []byte) (Header, []byte, error) {
		calls = append(calls, cmd)
		f, err := Parse(p)
		if err != nil || len(random) != 16 {
			t.Fatal("invalid request", err)
		}
		switch cmd {
		case 502:
			if path != getQRCodePath {
				t.Fatal("QR route changed")
			}
			r, _ := f.B(2)
			v, e := Unbuffer(r)
			if e != nil || !bytes.Equal(v, random) {
				t.Fatal("random key mismatch")
			}
			return Header{}, Join(Blob(1, Uint(1, 0)), Blob(2, Buffer(qrTestImage(t))), Blob(3, []byte("synthetic-challenge")), Blob(5, Buffer(key))), nil
		case 503:
			if path != checkQRCodePath {
				t.Fatal("poll route changed")
			}
			return Header{}, qrTestNotify(key, 2, "synthetic-challenge"), nil
		case 252:
			if path != manualAuthPath {
				t.Fatal("manual route changed")
			}
			rsa, e := f.Message(1)
			if e != nil {
				t.Fatal(e)
			}
			owner, _ := rsa.B(3)
			proof, _ := rsa.B(4)
			if string(owner) != "synthetic-owner" || string(proof) != "synthetic-one-time-authorization" {
				t.Fatal("confirmation did not reach manual auth")
			}
			return Header{UIN: 123, Cookie: []byte("synthetic")}, qrTestSession(), nil
		default:
			t.Fatal("unexpected network operation")
			return Header{}, nil, ErrWire
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = q.Start(ctx, now); err != nil {
		t.Fatal(err)
	}
	if err = q.Poll(ctx, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(q)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{string(key), "synthetic-owner", "synthetic-challenge", "synthetic-one-time-authorization"} {
		if bytes.Contains(raw, []byte(v)) || bytes.Contains([]byte(fmt.Sprintf("%#v", q)), []byte(v)) {
			t.Fatal("credential leaked")
		}
	}
	session, err := q.Authenticate(ctx)
	if err != nil || session.Bootstrap.UIN != 123 || len(session.ClientKey) != 24 {
		t.Fatal("first login", err)
	}
	if session.Identity == nil || *session.Identity != *qrTestIdentity().Runtime {
		t.Fatal("QR login lost persistent client identity")
	}
	if session.Confirmed {
		t.Fatal("auth reply was presented as a successful sync")
	}
	if _, err = q.Authenticate(ctx); err == nil || len(calls) != 3 {
		t.Fatal("authorization repeated")
	}
	if q.notificationKey != nil || q.authorization != nil || q.image != nil {
		t.Fatal("completed challenge retained private materials")
	}
}
func TestQRRejectsChangedChallengeAndExpiredApproval(t *testing.T) {
	key := bytes.Repeat([]byte{5}, 16)
	now := time.Unix(1700000000, 0)
	calls := 0
	q, _ := NewQRLogin(qrTestIdentity(), func(context.Context, uint16, string, []byte, []byte) (Header, []byte, error) {
		calls++
		return Header{}, qrTestNotify(key, 2, "other-challenge"), nil
	})
	q.uuid = []byte("original-challenge")
	q.notificationKey = key
	q.state = "waiting_scan"
	q.deadline = now.Add(time.Minute)
	if err := q.Poll(context.Background(), now); err == nil || q.state == "confirmed" {
		t.Fatal("challenge substitution accepted")
	}
	if err := q.Poll(context.Background(), now.Add(time.Minute)); err == nil || calls != 1 || q.state != "timed_out" {
		t.Fatal("expired challenge contacted server")
	}
}
func TestAuthenticationCandidateRejectsAccountSubstitution(t *testing.T) {
	b := Bootstrap{Username: []byte("other-owner"), DeviceID: qrTestIdentity().DeviceID}
	if _, err := decodeAuthentication(qrTestSession(), Header{}, b, qrTestIdentity().Profile, nil); err == nil {
		t.Fatal("substituted account accepted")
	}
	if _, err := decodeAuthentication(qrTestSession(), Header{UIN: 124}, Bootstrap{Username: []byte("synthetic-owner")}, qrTestIdentity().Profile, nil); err == nil {
		t.Fatal("header account mismatch accepted")
	}
}

func TestLoginDiagnosticsSnapshotsDoNotShareMutableMaps(t *testing.T) {
	q := &QRLogin{diagnostics: LoginDiagnostics{KeyLengths: map[int]int{23: 24}, NotifyFieldLengths: map[int]int{4: 128}}}
	first := q.Diagnostics()
	first.KeyLengths[23] = 0
	first.NotifyFieldLengths[4] = 0
	if q.Diagnostics().KeyLengths[23] != 24 || q.Diagnostics().NotifyFieldLengths[4] != 128 {
		t.Fatal("public snapshot changed private diagnostics")
	}
	q.diagnostics.KeyLengths[23] = 48
	q.diagnostics.NotifyFieldLengths[4] = 256
	if first.KeyLengths[23] != 0 || first.NotifyFieldLengths[4] != 0 {
		t.Fatal("network worker changed an already returned snapshot")
	}
}

func TestQRServerExpiryNeverTriggersManualAuthentication(t *testing.T) {
	key := bytes.Repeat([]byte{5}, 16)
	now := time.Unix(1700000000, 0)
	q, _ := NewQRLogin(qrTestIdentity(), func(context.Context, uint16, string, []byte, []byte) (Header, []byte, error) {
		ret := int32(-2007)
		return Header{}, Blob(1, Uint(1, uint64(int64(ret)))), nil
	})
	q.uuid, q.notificationKey, q.image = []byte("synthetic-challenge"), key, []byte("synthetic-image")
	q.state, q.deadline = "waiting_scan", now.Add(time.Minute)
	if err := q.Poll(context.Background(), now); err != nil || q.Status().State != "expired" || q.Status().ImageReady {
		t.Fatal("server-expired QR remained usable", err)
	}
	if _, err := q.Authenticate(context.Background()); err == nil {
		t.Fatal("expired challenge reached authentication")
	}
}

func TestQRQueryTimeMatchesNativeNarrowingBeforeDivision(t *testing.T) {
	key := bytes.Repeat([]byte{5}, 16)
	now := time.UnixMilli(1<<32 + 1234567)
	q, _ := NewQRLogin(qrTestIdentity(), func(_ context.Context, command uint16, _ string, request, _ []byte) (Header, []byte, error) {
		f, err := Parse(request)
		if err != nil {
			t.Fatal(err)
		}
		timestamp, present := f.N(4)
		if command != 503 || !present || timestamp != 1234 {
			t.Fatal("query time divided before narrowing", timestamp)
		}
		return Header{}, qrTestNotify(key, 1, "synthetic-challenge"), nil
	})
	q.uuid, q.notificationKey = []byte("synthetic-challenge"), key
	q.state, q.deadline = "waiting_scan", now.Add(time.Minute)
	if err := q.Poll(context.Background(), now); err != nil {
		t.Fatal(err)
	}
}
