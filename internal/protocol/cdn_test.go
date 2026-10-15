package protocol

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestCDNDiscoveryCurrentWireAndPrivateMaterial(t *testing.T) {
	secret := []byte("SYNTHETIC_CDN_AUTH_MATERIAL")
	packed := binary.AppendUvarint(nil, 443)
	packed = binary.AppendUvarint(packed, 80)
	endpoint := Join(Uint(1, 1), Uint(2, 123), Uint(3, 2), Uint(4, 3), Uint(5, 4), Blob(6, String([]byte("203.0.113.2"))), Blob(11, String([]byte("2001:db8::2"))), Blob(8, Buffer(secret)), Blob(12, Blob(2, packed)))
	d, err := DecodeCDNDiscovery(Join(Blob(2, endpoint), Blob(5, Buffer([]byte("normal"))), Blob(6, Buffer([]byte("safe")))))
	if err != nil || len(d.DNS) != 1 {
		t.Fatal("discovery decode failed", err)
	}
	got := d.DNS[2]
	if got.UIN != 123 || got.IPv4[0] != "203.0.113.2" || got.IPv6[0] != "2001:db8::2" || len(got.Ports) != 2 || got.Ports[0] != 443 || got.Ports[1] != 80 || got.Province != 2 || got.City != 3 || got.ISP != 4 || !bytes.Equal(got.Material(), secret) {
		t.Fatal("native profile fields were lost")
	}
	for _, value := range []any{d, got} {
		raw, err := json.Marshal(value)
		if err != nil || strings.Contains(string(raw), string(secret)) || strings.Contains(fmt.Sprintf("%+v", value), string(secret)) || strings.Contains(fmt.Sprintf("%#v", value), string(secret)) {
			t.Fatal("discovery diagnostics exposed private material")
		}
	}
}

func TestCDNDiscoveryRequestUsesExistingIdentity(t *testing.T) {
	b := Bootstrap{UIN: 123, DeviceID: []byte("0123456789abcdefextra")}
	p := Profile{Version: 0x8137ce63, DeviceType: "synthetic-platform"}
	f, err := Parse(cdnDiscoveryRequest(b, p, 7))
	if err != nil || len(f) != 1 {
		t.Fatal("request adds unsupported fields")
	}
	base, err := f.Message(1)
	zero, _ := base.B(1)
	device, _ := base.B(3)
	version, _ := base.N(4)
	scene, _ := base.N(6)
	if err != nil || len(base) != 6 || !bytes.Equal(zero, []byte{0}) || !bytes.Equal(device, b.DeviceID[:16]) || uint32(version) != p.Version || scene != 7 {
		t.Fatal("BaseRequest does not follow the current injected builder")
	}
}

func TestCDNDiscoveryRejectsInvalidAddressesAndPorts(t *testing.T) {
	for _, fields := range [][]byte{
		Blob(6, String([]byte("host\r\nInjected: true"))),
		Blob(12, Blob(2, []byte{0x80})),
		Blob(12, Uint(2, 65536)),
	} {
		if _, err := DecodeCDNDiscovery(Blob(2, Join(Uint(1, 1), fields))); err == nil {
			t.Fatal("invalid endpoint was accepted")
		}
	}
}
