//go:build integrationfixtures

package service

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"path/filepath"
	"strings"
	"sync"

	"github.com/JFSAS/wechat-runtime/internal/cdn"
	"github.com/JFSAS/wechat-runtime/internal/protocol"
)

//go:embed deployment-media.json
var deploymentMediaJSON []byte

type deploymentMediaFixture struct {
	Schema       int    `json:"schema"`
	Image        string `json:"image_base64"`
	FileTemplate string `json:"file_template"`
	FileName     string `json:"file_name"`
	LargeBytes   int    `json:"large_payload_bytes"`
}

var deploymentMediaData = sync.OnceValues(func() (deploymentMediaFixture, error) {
	var value deploymentMediaFixture
	if json.Unmarshal(deploymentMediaJSON, &value) != nil || value.Schema != 1 {
		return value, errors.New("invalid_deployment_media_fixture")
	}
	return value, nil
})

func deploymentMediaPayload(kind, label string) ([]byte, string, error) {
	value, err := deploymentMediaData()
	if err != nil {
		return nil, "", err
	}
	baseKind, large := strings.CutPrefix(kind, "large_")
	if large && (value.LargeBytes < 1<<20 || value.LargeBytes > 16<<20) {
		return nil, "", errors.New("invalid_deployment_large_media_size")
	}
	switch baseKind {
	case "image":
		body, err := base64.StdEncoding.DecodeString(value.Image)
		if err == nil && large {
			if len(body) < 12 || !bytes.Equal(body[len(body)-8:], []byte{'I', 'E', 'N', 'D', 0xae, 0x42, 0x60, 0x82}) || value.LargeBytes < len(body)+12 {
				return nil, "", errors.New("invalid_deployment_png_padding")
			}
			padding := deploymentRepeat([]byte("synthetic-image-padding/"+label), value.LargeBytes-len(body)-12)
			chunk := make([]byte, 8, 8+len(padding)+4)
			binary.BigEndian.PutUint32(chunk[:4], uint32(len(padding)))
			copy(chunk[4:], "npAd")
			chunk = append(chunk, padding...)
			checksum := crc32.ChecksumIEEE(chunk[4:])
			chunk = binary.BigEndian.AppendUint32(chunk, checksum)
			body = append(append(append([]byte(nil), body[:len(body)-12]...), chunk...), body[len(body)-12:]...)
		}
		return body, "fixture.png", err
	case "file":
		body := []byte(strings.ReplaceAll(value.FileTemplate, "{account}", label))
		if large {
			if len(body) == 0 {
				return nil, "", errors.New("invalid_deployment_file_template")
			}
			body = deploymentRepeat(body, value.LargeBytes)
		}
		return body, value.FileName, nil
	default:
		return nil, "", errors.New("invalid_deployment_media_kind")
	}
}

func deploymentRepeat(seed []byte, size int) []byte {
	return bytes.Repeat(seed, (size+len(seed)-1)/len(seed))[:size]
}

func (d *DeploymentDriver) downloadMedia(ctx context.Context, request MediaDownloadRequest) (*cdn.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	kind := string(request.Description.Public().Kind)
	ref := request.Description.DownloadReference()
	registered := kind
	if ref.CDNID(request.Variant) == "fixture-large_"+kind+"-"+d.label {
		registered = "large_" + kind
	} else if ref.CDNID(request.Variant) != "fixture-"+kind+"-"+d.label {
		return nil, errors.New("deployment_media_reference_not_registered")
	}
	body, _, err := deploymentMediaPayload(registered, d.label)
	if err != nil {
		return nil, err
	}
	wanted := "fixture-" + registered + "-" + d.label
	size, known := ref.SizeBytes(request.Variant)
	if ref.CDNID(request.Variant) != wanted || !known || size != uint64(len(body)) || ref.MD5() != fmt.Sprintf("%x", md5.Sum(body)) || request.MaxBytes > 0 && int64(len(body)) > request.MaxBytes {
		return nil, errors.New("deployment_media_reference_not_registered")
	}
	d.mu.Lock()
	d.downloads++
	d.mu.Unlock()
	return &cdn.Result{Data: bytes.Clone(body), RawSize: uint64(len(body)), ChecksumVerified: true}, nil
}

func (d *DeploymentDriver) sendMedia(ctx context.Context, request MediaSendRequest) (MediaSendResult, error) {
	d.mu.Lock()
	d.uploads++
	d.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return MediaSendResult{}, err
	}
	registered := request.Kind
	corpus, corpusErr := deploymentMediaData()
	if corpusErr == nil && len(request.Data) == corpus.LargeBytes {
		registered = "large_" + registered
	}
	expected, _, err := deploymentMediaPayload(registered, d.label)
	if err != nil || !bytes.Equal(request.Data, expected) {
		return MediaSendResult{}, errors.New("deployment_media_payload_not_registered")
	}
	main := request.Data
	var thumb []byte
	var tw, th int
	if request.Kind == "image" {
		main, thumb, _, _, tw, th, err = prepareImageUpload(request.Data)
		if err != nil {
			return MediaSendResult{}, err
		}
	}
	progress := request.Metadata
	progress.Stage, progress.FileID = "uploaded", "synthetic-deployment-upload-"+request.ID
	progress.MD5 = fmt.Sprintf("%x", md5.Sum(main))
	progress.UploadSize = uint32(len(main))
	if request.Kind == "image" {
		progress.ThumbnailID = "synthetic-deployment-thumb-" + request.ID
		progress.ThumbSize = uint32(len(thumb))
		progress.ThumbWidth = uint32(tw)
		progress.ThumbHeight = uint32(th)
	}
	if err := request.SaveProgress(progress); err != nil {
		return MediaSendResult{}, err
	}
	s := d.service
	s.mu.RLock()
	bootstrap := s.client.Bootstrap
	profile := s.client.State.Profile
	s.mu.RUnlock()
	var encoded []byte
	if request.Kind == "image" {
		encoded, err = protocol.EncodeImageSubmission(bootstrap, profile, 1, protocol.ImageSubmission{Recipient: request.Peer, ClientID: progress.NativeID, Metadata: protocol.ImageSubmissionMetadata{MidReference: progress.FileID, ThumbnailReference: progress.ThumbnailID, AESKey: progress.AESKey, ThumbnailAESKey: progress.ThumbnailKey, MD5: progress.MD5, MidLength: progress.UploadSize, ThumbnailLength: progress.ThumbSize, ThumbnailWidth: progress.ThumbWidth, ThumbnailHeight: progress.ThumbHeight}})
	} else {
		xml, e := protocol.BuildFileAppMessage(progress.Name, uint64(progress.UploadSize), progress.MD5, progress.FileID, progress.AESKey, filepath.Ext(progress.Name), string(bootstrap.Username))
		if e != nil {
			return MediaSendResult{}, e
		}
		encoded, err = protocol.EncodeFileSubmission(bootstrap, profile, 1, protocol.FileSubmission{Recipient: request.Peer, ClientID: progress.NativeID, AppMessageXML: xml, CreateTime: request.CreatedAt, MD5: progress.MD5, FileLength: uint64(progress.UploadSize)})
	}
	if err != nil {
		return MediaSendResult{FailureStep: "message_encode"}, err
	}
	progress.Stage = "submitting"
	if err := request.SaveProgress(progress); err != nil {
		return MediaSendResult{}, err
	}
	sum := sha256.Sum256([]byte("synthetic-media/" + d.label + "/" + request.ID))
	id := binary.BigEndian.Uint64(sum[:8]) | 1
	payload := sha256.Sum256(request.Data)
	packet := sha256.Sum256(encoded)
	kind := "private"
	if protocol.IsGroup([]byte(request.Peer)) {
		kind = "group"
	}
	row := DeploymentSend{ID: request.ID, Kind: kind, Media: request.Kind, Mentions: []string{}, ServerID: id, PayloadBytes: len(request.Data), PayloadSHA: hex.EncodeToString(payload[:]), EncodedBytes: len(encoded), EncodedSHA: hex.EncodeToString(packet[:])}
	d.mu.Lock()
	if d.pendingMedia == nil {
		d.pendingMedia = map[string]DeploymentSend{}
	}
	d.pendingMedia[request.ID] = row
	d.mu.Unlock()
	return MediaSendResult{Receipt: protocol.MediaSubmissionReceipt{ServerAccepted: true, ServerID: id}, SubmissionStarted: true}, nil
}

func (d *DeploymentDriver) InjectMedia(kind, mediaKind string) error {
	body, name, err := deploymentMediaPayload(mediaKind, d.label)
	if err != nil {
		return err
	}
	ref := "fixture-" + mediaKind + "-" + d.label
	message := protocol.Message{Type: 3}
	if strings.TrimPrefix(mediaKind, "large_") == "image" {
		message.Content = []byte(fmt.Sprintf(`<msg><img aeskey="00112233445566778899aabbccddeeff" cdnmidimgurl="%s" length="%d" md5="%x"/></msg>`, ref, len(body), md5.Sum(body)))
	} else {
		message.Type = 49
		message.Content = []byte(fmt.Sprintf(`<msg><appmsg><title>%s</title><type>6</type><appattach><totallen>%d</totallen><aeskey>00112233445566778899aabbccddeeff</aeskey><cdnattachurl>%s</cdnattachurl><md5>%x</md5></appattach></appmsg></msg>`, name, len(body), ref, md5.Sum(body)))
	}
	return d.injectMessage(kind, message, kind == "group")
}
