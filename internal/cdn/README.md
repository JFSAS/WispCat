# CDN media transfer package

This package implements ordinary image/file CDN download and command30000
upload from WeChat4.1.13 /269602, using the observed plaintext metadata branch. It does
not read accounts, refresh login, reuse a WeChat RPC client, or start a model.

The parent adapter supplies `Profile` from GetCDNDns and a `DownloadRequest` with
the chosen file ID, hex-decoded AES key, native file type and initial raw size.
`Common` must be non-nil; `ChatType`, `DownloadBehavior` and similar
values are caller choices until their app-side initialization is confirmed.
`FileKey` may identify the local task but is not sent. `ToUser` and upload-only
common options must be empty/zero. Native `CreateC2CImageDownloadTask` sets operation
4 and ordinary filetype5 uses `CreateC2CStreamDownloadTask` operation2;
`PackCommonHeader` then writes `offset=0` and returns before the upload-only
`touser/apptype/compresstype/filekey` block. `PackVideoHeader` still adds the
unconditional `largesvideo=0/sourceflag=0`. Image types1..3 and ordinary filetype5
are admitted. Appmsg49/type6 Resource103 with an opaque CDN ref maps to5 via
`3037d0c`; HTTP(S) URLs map to19 and are outside this implementation. No meta GCM key is
supplied for the observed legacy profile.

```go
client := cdn.Client{Timeout: 20 * time.Second, MaxBytes: 16 << 20}
request.Common = &cdn.CommonOptions{ /* explicit caller choices */ }
result, err := client.Download(ctx, profile, request)
```

Only the first address and port are used. There is no automatic endpoint retry
or TLS guess. The caller selects another endpoint by restricting those slices;
it can provide `DialContext` for a known TLS transport.

For a parent-side transport probe, `EncodeRequest` returns `Packet.Bytes()` (the
25-byte AB stream frame), `Packet.Body()` (raw TLV for the separately confirmed
HTTP POST branch), and `Packet.Sequence()`. `DecodeResponse` accepts one complete
AB stream response frame. Its `filedata` parser follows native `577b258`: consume
the full remaining frame payload, treating declared value length as a lower
bound rather than truncating to it.

A returned result has passed response status, sequence, AES-128-ECB/PKCS7,
server raw length and plaintext MD5 checks. A missing MD5 is an error unless the
caller supplies `ExpectedMD5`. Server `rawfilesize` may replace the XML size hint;
the replacement is bounded and checked against decrypted length. Meta-encrypted
responses are rejected because the legacy profile has no corresponding key.

`Profile`, `DownloadRequest`, `CommonOptions`, `Packet` and `Result` redact fmt and
JSON diagnostics. Byte accessors intentionally return sensitive data for the
transport; neither they nor request/response bytes should enter logs or Git.
Errors only contain categories, safe field names and numeric server codes.

The embedded DER is a public RSA key, recovered from native `4243010` and
`56dea10`: version `1`, 1024-bit modulus, exponent `010001`. The request wraps the
raw 16-byte file AES key using PKCS#1 v1.5 and emits raw RSA ciphertext.

Static evidence: `reports/media-file-stage22.md`,
`outputs/wechat-media-2026-10-11/file/download-wire-spec.json`,
`outputs/wechat-media-2026-10-11/image/cdn-static-rsa-public.json`.

Tests only create synthetic plaintext/credentials and loopback TCP listeners.
They validate split reads, frame bounds, cancellation/deadline, variable payload
semantics, integrity failures and redacted output. These tests do not establish
real WeChat CDN acceptance or delivery.

## Original-resource upload

`Client.Upload(ctx, profile, UploadRequest)` uses ordinary MM command30000.
SafeC2CUploadTask's vtable9972578 invokes `5856548 PackStreamUpload` and streams
the content afterwards. Factory5846ba4 overrides the intermediate command10000
when the normal request byte611 is zero. Its plaintext metadata and legacy
content-header branch is implemented. `Data` is uploaded byte for byte after
AES-128-ECB/PKCS7, without image
re-encoding. Main2 and thumbnail3 may be uploaded as independent resources;
parent110/222 message construction is outside this package. FileType, recipient,
app/chat/compression/format choices and `RawCRC` must be explicit. Ordinary
appmsg6's selector31a918c selects type5 at up to25MiB, and7 above; the project's
16MiB limit stays in type5. Its common request31c1550 supplies Biz1/AppType1.
Rule-selected safeproto and large-file behavior remain outside this branch.

The encoder includes `PackThumbHeader`'s zero-size fields even for hasthumb0,
`PackVideoHeader`'s unconditional fields and the upload-only common section. It
includes the shared common tail `dataid=0` and `offset=0` for the first block.
`meta_length` is the first TLV key, and its value is the first header-only length including
AB25 and the variable filedata header, excluding ciphertext and the second-pass
meta_length field. One complete frame is sent; there is no cache-hit query,
packet concurrency, retransmission or hit-induced AES key reuse.

The ACK must contain retcode0, the matching sequence and a nonempty fileid.
`ServerAccepted` is separate from `ChecksumVerified`; absent server digest never
becomes hash verification. Supplied server MD5/size and AES key are checked.
The digest in `UploadResult.MD5` is the local plaintext digest for the parent
message XML. This acknowledgement does not claim phone delivery.
