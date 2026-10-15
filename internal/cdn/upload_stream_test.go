package cdn

import (
	"context"
	"errors"
	"io"
	"strconv"
	"testing"
	"time"
)

func TestUploadTransportKeepsConnectionAcrossAcknowledgements(t *testing.T) {
	listener := syntheticListener(t)
	p, r := syntheticUpload()
	setEndpoint(&p, listener)
	packet, err := EncodeUpload(p, r)
	if err != nil {
		t.Fatal(err)
	}
	serverErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		if _, err := readFrame(conn, 1024); err != nil {
			serverErr <- err
			return
		}
		progress := uploadAck(map[string]string{"retcode": "0", "seq": "29"})
		final := uploadAck(map[string]string{"retcode": "0", "seq": "29", "fileid": "synthetic_final_secret"})
		// Consecutive replies can share a TCP read; framing must retain both.
		serverErr <- writeAll(conn, append(progress, final...))
	}()
	frames := 0
	err = (&Client{Timeout: time.Second, MaxBytes: 1024}).exchangeFrames(context.Background(), p, packet, 16, func(frame []byte) (bool, error) {
		frames++
		body, err := frameBody(frame, 1024)
		if err != nil {
			return false, err
		}
		meta, _, err := parseBody(body)
		if err != nil {
			return false, err
		}
		return len(meta["fileid"]) != 0, nil
	})
	if err != nil || frames != 2 {
		t.Fatalf("progress did not retain the upload connection: frames=%d err=%v", frames, err)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestUploadTransportCancellationAfterProgressClosesSocket(t *testing.T) {
	listener := syntheticListener(t)
	p, r := syntheticUpload()
	setEndpoint(&p, listener)
	packet, err := EncodeUpload(p, r)
	if err != nil {
		t.Fatal(err)
	}
	serverClosed := make(chan struct{})
	go func() {
		defer close(serverClosed)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		if _, err := readFrame(conn, 1024); err != nil {
			return
		}
		_ = writeAll(conn, uploadAck(map[string]string{"retcode": "0", "seq": "29"}))
		_, _ = io.Copy(io.Discard, conn)
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	progress := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- (&Client{Timeout: time.Second, MaxBytes: 1024}).exchangeFrames(ctx, p, packet, 16, func([]byte) (bool, error) {
			close(progress)
			return false, nil
		})
	}()
	select {
	case <-progress:
	case <-time.After(time.Second):
		t.Fatal("no upload progress received")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("progress cancellation lost: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("upload read stayed blocked after cancellation")
	}
	<-serverClosed
}

func TestUploadTransportRejectsUnboundedProgress(t *testing.T) {
	listener := syntheticListener(t)
	p, r := syntheticUpload()
	setEndpoint(&p, listener)
	packet, err := EncodeUpload(p, r)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		if _, err := readFrame(conn, 1024); err != nil {
			return
		}
		for i := 0; i < 3; i++ {
			_ = writeAll(conn, uploadAck(map[string]string{"retcode": "0", "seq": strconv.Itoa(int(r.Sequence))}))
		}
	}()
	frames := 0
	err = (&Client{Timeout: time.Second, MaxBytes: 1024}).exchangeFrames(context.Background(), p, packet, 3, func([]byte) (bool, error) {
		frames++
		return false, nil
	})
	var protocol *Error
	if !errors.As(err, &protocol) || protocol.Category != "response_frame_limit" || frames != 3 {
		t.Fatalf("endless progress became success: frames=%d err=%v", frames, err)
	}
}

func TestUploadNativeProgressThenFinalAcknowledgement(t *testing.T) {
	for _, finalError := range []bool{false, true} {
		t.Run(strconv.FormatBool(finalError), func(t *testing.T) {
			listener := syntheticListener(t)
			p, r := syntheticUpload()
			setEndpoint(&p, listener)
			serverErr := make(chan error, 1)
			go func() {
				conn, err := listener.Accept()
				if err != nil {
					serverErr <- err
					return
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(time.Second))
				if _, err := readFrame(conn, 1024); err != nil {
					serverErr <- err
					return
				}
				progress := uploadAck(map[string]string{"retcode": "0", "seq": "29", "recvlen": "16"})
				final := map[string]string{"retcode": "0", "seq": "29", "fileid": "synthetic_completed_secret"}
				if finalError {
					final["retcode"] = "-5103009"
				}
				serverErr <- writeAll(conn, append(progress, uploadAck(final)...))
			}()
			result, err := (&Client{Timeout: time.Second, MaxBytes: 1024}).Upload(context.Background(), p, r)
			if finalError {
				var protocol *Error
				if result != nil || !errors.As(err, &protocol) || protocol.Category != "server_error" || protocol.Code != -5103009 {
					t.Fatalf("progress hid final rejection: %v %v", result, err)
				}
			} else if err != nil || result == nil || !result.ServerAccepted || result.FileID != "synthetic_completed_secret" || result.ChecksumVerified {
				t.Fatalf("same-socket progress/final state was lost: %v %v", result, err)
			}
			if err := <-serverErr; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestUploadNativeProgressCannotOutliveDeadline(t *testing.T) {
	listener := syntheticListener(t)
	p, r := syntheticUpload()
	setEndpoint(&p, listener)
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(time.Second))
		if _, err := readFrame(conn, 1024); err != nil {
			return
		}
		_ = writeAll(conn, uploadAck(map[string]string{"retcode": "0", "seq": "29", "recvlen": "16"}))
		_, _ = io.Copy(io.Discard, conn)
	}()
	result, err := (&Client{Timeout: 80 * time.Millisecond, MaxBytes: 1024}).Upload(context.Background(), p, r)
	if result != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("incomplete progress became accepted or lost deadline: %v %v", result, err)
	}
	<-closed
}
