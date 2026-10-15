package service

import (
	"context"
	"errors"
	"github.com/JFSAS/wechat-runtime/internal/cdn"
	"github.com/JFSAS/wechat-runtime/internal/media"
	"github.com/JFSAS/wechat-runtime/internal/protocol"
	"strings"
)

// EnableNativeMedia binds the serial network worker to the verified ordinary
// CDN image and file paths. It never creates another WeChat login or runs in HTTP goroutines.
func (s *Service) EnableNativeMedia() {
	s.SetMediaDownloader(s.nativeMediaDownload)
	s.SetMediaSender(s.nativeMediaSend)
}

func (s *Service) nativeMediaDownload(ctx context.Context, request MediaDownloadRequest) (*cdn.Result, error) {
	if s.client == nil || s.client.State == nil {
		return nil, ErrMediaUnavailable
	}
	reference := request.Description.DownloadReference()
	key := reference.AESKey(request.Variant)
	if len(key) != 16 || !reference.Has(request.Variant) {
		return nil, ErrMediaInvalidRequest
	}
	fileType := int32(0)
	switch request.Variant {
	case media.Regular:
		fileType = 2
	case media.Original:
		fileType = 1
	case media.Thumbnail:
		fileType = 3
	case media.Attachment:
		if request.Description.Public().Kind != media.File {
			return nil, ErrMediaInvalidRequest
		}
		size, known := reference.SizeBytes(media.Attachment)
		if !known || size == 0 || size > MaxMediaSendBytes {
			return nil, ErrMediaInvalidRequest
		}
		// Ordinary opaque references select native type5. HTTP(S) resources
		// take the distinct type19 path; do not coerce them into this transport.
		ref := strings.ToLower(reference.CDNID(media.Attachment))
		if strings.HasPrefix(ref, "http://") || strings.HasPrefix(ref, "https://") {
			return nil, ErrMediaTransportUnconfigured
		}
		fileType = 5
	default:
		return nil, ErrMediaInvalidRequest
	}
	discovery, err := s.client.CDNInfo(ctx, uint32(s.client.State.Sequence)+1)
	if err != nil {
		return nil, err
	}
	dns := discovery.DNS[2]
	if uint32(dns.UIN) != s.client.Bootstrap.UIN {
		return nil, errors.New("media_account_binding_mismatch")
	}
	profile := cdn.Profile{Version: dns.ID, UIN: uint32(dns.UIN), Province: dns.Province, City: dns.City, ISP: dns.ISP, ClientVersion: s.client.State.Profile.Version, OSType: s.client.State.Profile.DeviceType, AuthKey: dns.Material(), Addresses: dns.IPv4, Ports: dns.Ports, NetType: 0}
	chatType := int32(0)
	if protocol.IsGroup(request.Peer) {
		chatType = 1
	}
	size, _ := reference.SizeBytes(request.Variant)
	args := cdn.DownloadRequest{FileID: reference.CDNID(request.Variant), FileType: fileType, RawSize: size, FileKey: request.ID, Common: &cdn.CommonOptions{ChatType: chatType}}
	copy(args.AESKey[:], key)
	if request.Variant != media.Thumbnail {
		args.ExpectedMD5 = reference.MD5()
		if request.Variant == media.Original {
			args.ExpectedMD5 = reference.OriginMD5()
		}
	}
	return (&cdn.Client{MaxBytes: request.MaxBytes}).Download(ctx, profile, args)
}
