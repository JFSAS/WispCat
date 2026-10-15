package protocol

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"net"
	"strings"
)

// DNS field numbers deliberately remain aliases until their individual CDN
// roles are established. The material is private and is never a display value.
type CDNEndpoint struct {
	ID       uint32
	UIN      uint64
	IPv4     []string
	IPv6     []string
	Ports    []uint16
	Province int32
	City     int32
	ISP      int32
	material []byte
}

func (d CDNEndpoint) Material() []byte { return append([]byte(nil), d.material...) }
func (d CDNEndpoint) String() string   { return "protocol.CDNEndpoint(redacted)" }
func (d CDNEndpoint) GoString() string { return d.String() }
func (d CDNEndpoint) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{"id": d.ID, "address_count": len(d.IPv4) + len(d.IPv6), "port_count": len(d.Ports), "material_bytes": len(d.material)})
}

type CDNDiscovery struct {
	DNS        map[int]CDNEndpoint
	normalRule []byte
	safeRule   []byte
}

func (d CDNDiscovery) String() string   { return "protocol.CDNDiscovery(redacted)" }
func (d CDNDiscovery) GoString() string { return d.String() }
func (d CDNDiscovery) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{"dns": d.DNS, "normal_rule_bytes": len(d.normalRule), "safe_rule_bytes": len(d.safeRule)})
}

// CDNInfo follows the observed 423ce44→423ffec→42401bc builder. It reuses an
// authenticated session and does not import or change any login material.
func (c *Client) CDNInfo(ctx context.Context, taskID uint32) (CDNDiscovery, error) {
	if c.State == nil || taskID == 0 || len(c.Bootstrap.DeviceID) < 16 {
		return CDNDiscovery{}, ErrWire
	}
	plain, err := c.sessionExchange(ctx, 379, "/cgi-bin/micromsg-bin/getcdndns", cdnDiscoveryRequest(c.Bootstrap, c.State.Profile, taskID))
	if err != nil {
		return CDNDiscovery{}, err
	}
	return DecodeCDNDiscovery(plain)
}

func cdnDiscoveryRequest(bootstrap Bootstrap, profile Profile, taskID uint32) []byte {
	base := Join(Blob(1, []byte{0}), Uint(2, uint64(bootstrap.UIN)), Blob(3, bootstrap.DeviceID[:16]), Uint(4, uint64(int64(int32(profile.Version)))), Blob(5, []byte(profile.DeviceType)), Uint(6, uint64(taskID)))
	return Blob(1, base)
}

func DecodeCDNDiscovery(data []byte) (CDNDiscovery, error) {
	d := CDNDiscovery{DNS: map[int]CDNEndpoint{}}
	f, err := Parse(data)
	if err != nil {
		return d, err
	}
	for _, tag := range []int{2, 3, 4, 7, 12} {
		raw, found := f.B(tag)
		if !found {
			continue
		}
		dns, err := decodeCDNEndpoint(raw)
		if err != nil {
			return d, err
		}
		d.DNS[tag] = dns
	}
	for tag, target := range map[int]*[]byte{5: &d.normalRule, 6: &d.safeRule} {
		if raw, found := f.B(tag); found {
			*target, err = Unbuffer(raw)
			if err != nil {
				return d, err
			}
		}
	}
	if len(d.DNS) == 0 {
		return d, &Error{Category: "cdn_discovery_unavailable"}
	}
	return d, nil
}

func decodeCDNEndpoint(data []byte) (CDNEndpoint, error) {
	var d CDNEndpoint
	f, err := Parse(data)
	if err != nil {
		return d, err
	}
	id, ok := f.N(1)
	if !ok || id > 0xffffffff {
		return d, ErrWire
	}
	d.ID = uint32(id)
	d.UIN, _ = f.N(2)
	province, _ := f.N(3)
	city, _ := f.N(4)
	isp, _ := f.N(5)
	d.Province, d.City, d.ISP = int32(province), int32(city), int32(isp)
	for tag, target := range map[int]*[]string{6: &d.IPv4, 11: &d.IPv6} {
		for _, raw := range f.All(tag) {
			if len(*target) >= 64 {
				return d, ErrWire
			}
			address, err := Unstring(raw)
			if err != nil || len(address) > 255 || strings.ContainsAny(string(address), "\x00\r\n") || net.ParseIP(string(address)) == nil {
				return d, ErrWire
			}
			*target = append(*target, string(address))
		}
	}
	if raw, found := f.B(8); found {
		d.material, err = Unbuffer(raw)
		if err != nil || len(d.material) > 64<<10 {
			return d, ErrWire
		}
	}
	// The observed legacy adapter uses the first field12 PortInfo list.
	if values := f.All(12); len(values) > 0 {
		ports, err := Parse(values[0])
		if err != nil {
			return d, err
		}
		for _, value := range ports[2] {
			if len(d.Ports) >= 64 {
				return d, ErrWire
			}
			if value.Wire == 0 {
				if value.Number > 65535 || value.Number == 0 {
					return d, ErrWire
				}
				d.Ports = append(d.Ports, uint16(value.Number))
			} else if value.Wire == 2 {
				for packed := value.Bytes; len(packed) > 0; {
					n, count := binary.Uvarint(packed)
					if count <= 0 || n > 65535 || n == 0 || len(d.Ports) >= 64 {
						return d, ErrWire
					}
					d.Ports = append(d.Ports, uint16(n))
					packed = packed[count:]
				}
			} else {
				return d, ErrWire
			}
		}
	}
	return d, nil
}
