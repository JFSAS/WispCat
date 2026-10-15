package service

import (
	"bytes"
	"context"
	"hash/crc32"
	"image"
	"image/color"
	"image/jpeg"
	"path/filepath"
	"strings"

	"github.com/JFSAS/wechat-runtime/internal/cdn"
	"github.com/JFSAS/wechat-runtime/internal/media"
	"github.com/JFSAS/wechat-runtime/internal/protocol"
)

func (s *Service) nativeMediaSend(ctx context.Context, r MediaSendRequest) (MediaSendResult, error) {
	var outcome MediaSendResult
	if s.client == nil || s.client.State == nil {
		return outcome, ErrMediaUnavailable
	}
	if r.Kind != "image" && r.Kind != "file" {
		return outcome, ErrMediaInvalidRequest
	}
	m := r.Metadata
	data := r.Data
	var thumb []byte
	var width, height, thumbWidth, thumbHeight int
	if r.Kind == "image" {
		var err error
		data, thumb, width, height, thumbWidth, thumbHeight, err = prepareImageUpload(data)
		if err != nil {
			return outcome, err
		}
	}
	discovery, err := s.client.CDNInfo(ctx, uint32(s.client.State.Sequence)+1)
	if err != nil {
		outcome.FailureStep = "cdn_discovery"
		return outcome, err
	}
	dns := discovery.DNS[2]
	if uint32(dns.UIN) != s.client.Bootstrap.UIN {
		return outcome, ErrMediaUnavailable
	}
	profile := cdn.Profile{Version: dns.ID, UIN: uint32(dns.UIN), Province: dns.Province, City: dns.City, ISP: dns.ISP, ClientVersion: s.client.State.Profile.Version, OSType: s.client.State.Profile.DeviceType, AuthKey: dns.Material(), Addresses: dns.IPv4, Ports: dns.Ports, NetType: 0}
	fileType := int32(2)
	if r.Kind == "file" {
		fileType = 5
	}
	chatType := int32(0)
	if protocol.IsGroup([]byte(r.Peer)) {
		chatType = 1
	}
	crc := crc32.ChecksumIEEE(data)
	args := cdn.UploadRequest{Data: data, FileType: fileType, FileKey: r.ID, ToUser: r.Peer, RawCRC: &crc, Common: &cdn.UploadOptions{AppType: 1, ChatType: chatType, ImageWidth: int32(width), ImageHeight: int32(height)}}
	copy(args.AESKey[:], m.AESKey)
	transport := &cdn.Client{MaxBytes: MaxMediaSendBytes}
	upload, err := transport.Upload(ctx, profile, args)
	if err != nil {
		outcome.FailureStep = "cdn_main_upload"
		return outcome, err
	}
	m.FileID, m.MD5, m.UploadSize = upload.FileID, upload.MD5, uint32(upload.Size)
	m.Stage = "main_uploaded"
	if err = r.SaveProgress(m); err != nil {
		return outcome, err
	}
	if r.Kind == "image" {
		thumbCRC := crc32.ChecksumIEEE(thumb)
		thumbArgs := cdn.UploadRequest{Data: thumb, FileType: 3, FileKey: r.ID + "_thumb", ToUser: r.Peer, RawCRC: &thumbCRC, Common: &cdn.UploadOptions{AppType: 1, ChatType: chatType, ImageWidth: int32(thumbWidth), ImageHeight: int32(thumbHeight)}}
		copy(thumbArgs.AESKey[:], m.ThumbnailKey)
		thumbnail, err := transport.Upload(ctx, profile, thumbArgs)
		if err != nil {
			outcome.FailureStep = "cdn_thumbnail_upload"
			return outcome, err
		}
		m.ThumbnailID, m.ThumbSize, m.ThumbWidth, m.ThumbHeight = thumbnail.FileID, uint32(thumbnail.Size), uint32(thumbWidth), uint32(thumbHeight)
	}
	m.Stage = "uploaded"
	if err = r.SaveProgress(m); err != nil {
		return outcome, err
	}
	// Validate the exact message before marking the non-replayable boundary.
	if r.Kind == "image" {
		input := protocol.ImageSubmission{Recipient: r.Peer, ClientID: m.NativeID, Metadata: protocol.ImageSubmissionMetadata{MidReference: m.FileID, ThumbnailReference: m.ThumbnailID, AESKey: m.AESKey, ThumbnailAESKey: m.ThumbnailKey, MD5: m.MD5, MidLength: m.UploadSize, ThumbnailLength: m.ThumbSize, ThumbnailWidth: m.ThumbWidth, ThumbnailHeight: m.ThumbHeight}}
		task := uint32(s.client.State.Sequence) + 1
		if _, err = protocol.EncodeImageSubmission(s.client.Bootstrap, s.client.State.Profile, task, input); err != nil {
			outcome.FailureStep = "message_encode"
			return outcome, err
		}
		m.Stage = "submitting"
		if err = r.SaveProgress(m); err != nil {
			return outcome, err
		}
		outcome.SubmissionStarted = true
		outcome.Receipt, err = s.client.SubmitImage(ctx, task, input)
	} else {
		// The file XML builder is supplied by the command222 native codec.
		xml, buildErr := protocol.BuildFileAppMessage(m.Name, uint64(m.UploadSize), m.MD5, m.FileID, m.AESKey, filepath.Ext(m.Name), string(s.client.Bootstrap.Username))
		if buildErr != nil {
			outcome.FailureStep = "message_encode"
			return outcome, buildErr
		}
		input := protocol.FileSubmission{Recipient: r.Peer, ClientID: m.NativeID, AppMessageXML: xml, CreateTime: r.CreatedAt, MD5: m.MD5, FileLength: uint64(m.UploadSize)}
		task := uint32(s.client.State.Sequence) + 1
		if _, err = protocol.EncodeFileSubmission(s.client.Bootstrap, s.client.State.Profile, task, input); err != nil {
			outcome.FailureStep = "message_encode"
			return outcome, err
		}
		m.Stage = "submitting"
		if err = r.SaveProgress(m); err != nil {
			return outcome, err
		}
		outcome.SubmissionStarted = true
		outcome.Receipt, err = s.client.SubmitFile(ctx, task, input)
	}
	if err != nil && outcome.SubmissionStarted {
		outcome.FailureStep = "message_submit"
		if strings.Contains(err.Error(), "exchange:") {
			outcome.FailureStep = "message_exchange"
		}
		if strings.Contains(err.Error(), "receipt:") {
			outcome.FailureStep = "message_receipt"
		}
	}
	return outcome, err
}

// Ordinary image sending creates a JPEG medium image and a JPEG thumbnail.
// GIF sends its first frame. This path does not claim original/HD preservation.
func prepareImageUpload(data []byte) (main, thumb []byte, width, height, thumbWidth, thumbHeight int, err error) {
	if _, err = media.ValidateImage(data); err != nil {
		return
	}
	source, _, decodeErr := image.Decode(bytes.NewReader(data))
	if decodeErr != nil {
		err = ErrMediaInvalidRequest
		return
	}
	encode := func(limit, quality int) ([]byte, int, int, error) {
		b := source.Bounds()
		w, h := b.Dx(), b.Dy()
		if w > limit || h > limit {
			if w >= h {
				h = max(1, h*limit/w)
				w = limit
			} else {
				w = max(1, w*limit/h)
				h = limit
			}
		}
		rgb := image.NewRGBA(image.Rect(0, 0, w, h))
		for y := 0; y < h; y++ {
			for x := 0; x < w; x++ {
				red, green, blue, alpha := source.At(b.Min.X+x*b.Dx()/w, b.Min.Y+y*b.Dy()/h).RGBA()
				rgb.SetRGBA(x, y, color.RGBA{R: uint8((red + 65535 - alpha) >> 8), G: uint8((green + 65535 - alpha) >> 8), B: uint8((blue + 65535 - alpha) >> 8), A: 255})
			}
		}
		var output bytes.Buffer
		if err := jpeg.Encode(&output, rgb, &jpeg.Options{Quality: quality}); err != nil {
			return nil, 0, 0, ErrMediaInvalidRequest
		}
		if output.Len() > MaxMediaSendBytes {
			return nil, 0, 0, ErrMediaInvalidRequest
		}
		return output.Bytes(), w, h, nil
	}
	main, width, height, err = encode(2048, 85)
	if err != nil {
		return
	}
	thumb, thumbWidth, thumbHeight, err = encode(320, 75)
	return
}
