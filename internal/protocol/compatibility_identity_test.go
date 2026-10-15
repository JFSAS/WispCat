package protocol

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestMacCompatibilityIdentityIgnoresHostEnvironment(t *testing.T) {
	device := []byte("synthetic-device")
	t.Setenv("PATH", t.TempDir())
	t.Setenv("HOSTNAME", "linux-test-host")
	t.Setenv("LANG", "en_US.UTF-8")
	t.Setenv("TZ", "UTC")
	c := Client{Bootstrap: Bootstrap{DeviceID: device}}
	profile, first, err := c.compatibilityIdentity()
	if err != nil {
		t.Fatal(err)
	}
	if profile.DeviceType != "UnifiedPCMac 15 arm64" || profile.Brand != "Apple" || profile.Version != 0xf2641d22 || profile.Config != 38 || first.TimezoneHours != 8 {
		t.Fatal("host or architecture changed wire profile")
	}
	if strings.Contains(first.Computer, "linux") || !strings.Contains(first.Description(), "ProcessName:WeChat;") {
		t.Fatal("host leaked into identity")
	}
	t.Setenv("HOSTNAME", "moved-host")
	t.Setenv("TZ", "Europe/London")
	again, err := NewClientIdentity(device)
	if err != nil || again != first {
		t.Fatal("moving hosts changed identity", err)
	}
	other, _ := NewClientIdentity([]byte("different-device"))
	if other.MachineID == first.MachineID || other.Computer == first.Computer {
		t.Fatal("new devices share identity")
	}
}
func TestPersistedClientIdentityAndProfileTakePrecedence(t *testing.T) {
	identity, _ := NewClientIdentity([]byte("synthetic-device"))
	identity.Computer = "Existing-MacBook"
	identity.TimezoneHours = 0
	profile := MacCompatibilityProfile()
	profile.DeviceType = "UnifiedPCMac 14 arm64"
	profile.Language = "en"
	s := Session{Profile: profile, Identity: &identity, Bootstrap: Bootstrap{DeviceID: []byte("synthetic-device")}}
	raw, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var restored Session
	if err = json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	c := Client{Bootstrap: restored.Bootstrap, State: &restored}
	gotProfile, gotIdentity, err := c.compatibilityIdentity()
	if err != nil || gotProfile != profile || gotIdentity != identity {
		t.Fatal("saved macOS identity lost", err)
	}
	restored.Identity.MachineID = "bad-address"
	if _, _, err = c.compatibilityIdentity(); err == nil {
		t.Fatal("invalid saved identity silently replaced")
	}
}
func TestQRIdentityIsIndependentOfMacOSUtilities(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	device, err := GenerateLoginDevice()
	if err != nil {
		t.Fatal(err)
	}
	first, err := LocalLoginIdentity(device)
	if err != nil {
		t.Fatal(err)
	}
	second, err := LocalLoginIdentity(device)
	if err != nil {
		t.Fatal(err)
	}
	if first.Profile != MacCompatibilityProfile() || first.Runtime == nil || *first.Runtime != *second.Runtime {
		t.Fatal("QR identity is not stable")
	}
}
