package protocol

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"strings"
	"unicode"
)

// ClientIdentity is protocol metadata, not a description of the host OS. It
// travels with the encrypted session so moving the cache does not change the
// simulated computer, adapter address, language or timezone.
type ClientIdentity struct {
	Schema        int    `json:"schema"`
	Computer      string `json:"computer"`
	MachineID     string `json:"machine_id"`
	TimezoneHours int    `json:"timezone_hours"`
}

// Keep the already implemented macOS 4.1.13 protocol on every supported host.
// A Linux host must not accidentally advertise an unimplemented Linux client.
func MacCompatibilityProfile() Profile {
	return Profile{Version: 0xf2641d22, Config: 38, DeviceType: "UnifiedPCMac 15 arm64", Language: "zh_CN", Brand: "Apple"}
}
func NewClientIdentity(device []byte) (ClientIdentity, error) {
	if len(device) < 16 || len(device) > 128 {
		return ClientIdentity{}, ErrWire
	}
	digest := sha256.Sum256(Join([]byte("rea-macos-client-v1\x00"), device))
	mac := net.HardwareAddr(append([]byte(nil), digest[:6]...))
	mac[0] = (mac[0] & 0xfe) | 0x02 // stable, locally administered unicast address
	return ClientIdentity{Schema: 1, Computer: "MacBook-Pro-" + hex.EncodeToString(digest[6:10]), MachineID: strings.ToUpper(mac.String()), TimezoneHours: 8}, nil
}
func (i ClientIdentity) Validate() error {
	if i.Schema != 1 || i.Computer == "" || len(i.Computer) > 128 || strings.IndexFunc(i.Computer, unicode.IsControl) >= 0 || i.TimezoneHours < -12 || i.TimezoneHours > 14 {
		return ErrWire
	}
	mac, err := net.ParseMAC(i.MachineID)
	if err != nil || len(mac) != 6 {
		return ErrWire
	}
	return nil
}
func (i ClientIdentity) Description() string {
	return fmt.Sprintf("ProcessId:%d;ProcessName:WeChat;ClassName:WeChatLoginWndForPC;qbMachineId:;MachineId:%s7;QBVerSion:", os.Getpid(), i.MachineID)
}
func (c *Client) compatibilityIdentity() (Profile, ClientIdentity, error) {
	profile := MacCompatibilityProfile()
	if c.Profile != nil {
		profile = *c.Profile
	}
	identity := c.Identity
	if c.State != nil {
		if c.State.Profile.Version != 0 {
			profile = c.State.Profile
		}
		if c.State.Identity != nil {
			identity = c.State.Identity
		}
	}
	if identity != nil {
		return profile, *identity, identity.Validate()
	}
	generated, err := NewClientIdentity(c.Bootstrap.DeviceID)
	return profile, generated, err
}
