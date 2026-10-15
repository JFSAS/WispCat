package protocol

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
)

type Bootstrap struct {
	Username []byte `json:"username"`
	UIN      uint32 `json:"uin"`
	AAK      []byte `json:"serialized_aak"`
	First    []byte `json:"aak_first_part"`
	Second   []byte `json:"aak_second_part"`
	Cookie   []byte `json:"server_id"`
	DeviceID []byte `json:"stored_device_id"`
	LocalID  uint64 `json:"local_device_id"`
	KeyInfo  []byte `json:"key_info"`
	MLCert   bool   `json:"use_ml_cert"`
}

func (b Bootstrap) Binding() string {
	device := b.DeviceID
	if len(device) > 16 {
		device = device[:16]
	}
	h := sha256.Sum256(Join([]byte("rea-wechat-v1\x00"), b.Username, []byte{0}, binary.BigEndian.AppendUint64(nil, uint64(b.UIN)), device))
	return hex.EncodeToString(h[:])
}

type Profile struct {
	Version    uint32 `json:"client_version_u32"`
	Config     uint32 `json:"config_128_u32"`
	DeviceType string `json:"device_type"`
	Language   string `json:"language"`
	Brand      string `json:"brand"`
}
type Session struct {
	Identity        *ClientIdentity `json:"client_identity,omitempty"`
	Bootstrap       Bootstrap       `json:"bootstrap"`
	Profile         Profile         `json:"profile"`
	Cookie          []byte          `json:"cookie"`
	Original        []byte          `json:"original_key"`
	ClientKey       []byte          `json:"client_key"`
	ServerKey       []byte          `json:"server_key"`
	Cursor          []byte          `json:"cursor"`
	Sequence        uint16          `json:"sequence"`
	Confirmed       bool            `json:"authentication_confirmed"`
	AuthGeneration  uint64          `json:"auth_generation,omitempty"`
	AuthenticatedAt int64           `json:"authenticated_at,omitempty"`
}
type Message struct {
	Sender    []byte `json:"sender"`
	Recipient []byte `json:"recipient"`
	Content   []byte `json:"content"`
	Type      uint32 `json:"type"`
	Timestamp uint32 `json:"timestamp"`
	ServerID  uint64 `json:"server_id"`
	Source    []byte `json:"source,omitempty"`
}

func (m Message) Peer(owner []byte) []byte {
	if IsGroup(m.Sender) {
		return m.Sender
	}
	if IsGroup(m.Recipient) {
		return m.Recipient
	}
	if string(m.Sender) == string(owner) {
		return m.Recipient
	}
	return m.Sender
}

type Contact struct {
	Username []byte  `json:"username"`
	Nickname *string `json:"nickname"`
	Remark   *string `json:"remark"`
	Deleted  bool    `json:"-"`
}

func (c Contact) Label() string {
	if c.Remark != nil && *c.Remark != "" {
		return *c.Remark
	}
	if c.Nickname != nil && *c.Nickname != "" {
		return *c.Nickname
	}
	return string(c.Username)
}

type SyncResult struct {
	Cursor   []byte
	Continue uint32
	Contacts []Contact
	Messages []Message
}
type Receipt struct {
	ServerID uint64
	Time     uint32
}
type ContactPage struct {
	CursorA, CursorB, Continue uint32
	Usernames                  [][]byte
}
type Brief struct {
	Username []byte
	Status   int32
	Contact  *Contact
}
type Error struct {
	Category string
	Code     int32
}

func (e *Error) Error() string { return e.Category }

// AuthenticationError distinguishes the existing response checks without
// exposing the response body or presenting an unspecified code as a server code.
type AuthenticationError struct {
	Reason string
}

func (e *AuthenticationError) Error() string { return "authentication_rejected" }
func (e *AuthenticationError) Unwrap() error { return &Error{Category: "authentication_rejected"} }
