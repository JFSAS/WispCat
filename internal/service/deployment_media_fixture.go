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
	switch kind {
	case "image":
		body, err := base64.StdEncoding.DecodeString(value.Image)
		return body, "fixture.png", err
	case "file":
		return []byte(strings.ReplaceAll(value.FileTemplate, "{account}", label)), value.FileName, nil
	default:
		return nil, "", errors.New("invalid_deployment_media_kind")
	}
}

func (d *DeploymentDriver) downloadMedia(ctx context.Context, request MediaDownloadRequest) (*cdn.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	kind := string(request.Description.Public().Kind)
	body, _, err := deploymentMediaPayload(kind, d.label)
	if err != nil {
		return nil, err
	}
	ref := request.Description.DownloadReference()
	wanted := "fixture-" + kind + "-" + d.label
	size, known := ref.SizeBytes(request.Variant)
	if ref.CDNID(request.Variant) != wanted || !known || size != uint64(len(body)) || ref.MD5() != fmt.Sprintf("%x", md5.Sum(body)) {
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
	expected, _, err := deploymentMediaPayload(request.Kind, d.label)
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
	if mediaKind == "image" {
		message.Content = []byte(fmt.Sprintf(`<msg><img aeskey="00112233445566778899aabbccddeeff" cdnmidimgurl="%s" length="%d" md5="%x"/></msg>`, ref, len(body), md5.Sum(body)))
	} else {
		message.Type = 49
		message.Content = []byte(fmt.Sprintf(`<msg><appmsg><title>%s</title><type>6</type><appattach><totallen>%d</totallen><aeskey>00112233445566778899aabbccddeeff</aeskey><cdnattachurl>%s</cdnattachurl><md5>%x</md5></appattach></appmsg></msg>`, name, len(body), ref, md5.Sum(body)))
	}
	return d.injectMessage(kind, message, kind == "group")
}
