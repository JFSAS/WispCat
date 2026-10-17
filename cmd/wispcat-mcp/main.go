package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/JFSAS/wispcat/internal/mcp"
	"github.com/JFSAS/wispcat/pkg/client"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() (result error) {
	mode := os.Getenv("WISPCAT_MCP_MODE")
	endpoint := os.Getenv("WISPCAT_MCP_ENDPOINT")
	if mode == "" {
		mode = "docs"
		if endpoint != "" {
			mode = "read"
		}
	}
	server := mcp.Server{Mode: mode}
	if mode != "docs" {
		c, err := client.New(endpoint, os.Getenv("WISPCAT_MCP_TOKEN"))
		if err != nil {
			return errors.New("MCP endpoint or token is invalid")
		}
		server.Client = c
	}
	if mode == "write" {
		maximum, err := positiveEnv("WISPCAT_MCP_MAX_STREAM_BYTES", client.MaxStreamFileBytes, client.MaxStreamFileBytes)
		if err != nil {
			return err
		}
		timeout, err := positiveEnv("WISPCAT_MCP_TIMEOUT_MS", 30000, 600000)
		if err != nil {
			return err
		}
		config := mcp.TransferConfig{DownloadDir: os.Getenv("WISPCAT_MCP_STREAM_DIR"), UploadRoot: os.Getenv("WISPCAT_MCP_UPLOAD_ROOT"), MaxFileBytes: maximum, Timeout: time.Duration(timeout) * time.Millisecond}
		transfers, err := mcp.OpenTransfers(config)
		if err != nil {
			return err
		}
		defer func() { result = errors.Join(result, transfers.Close()) }()
		server.Transfers = transfers
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	input, closeInput, err := cancellableStdio(ctx, os.Stdin)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, closeInput()) }()
	output, closeOutput, err := cancellableStdio(ctx, os.Stdout)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, closeOutput()) }()
	err = server.Run(ctx, input, output)
	if ctx.Err() != nil || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func positiveEnv(name string, fallback, maximum int64) (int64, error) {
	raw, exists := os.LookupEnv(name)
	if !exists {
		return fallback, nil
	}
	if raw == "" || strings.IndexFunc(raw, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
		return 0, errors.New("invalid_mcp_transfer_config")
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 1 || value > maximum {
		return 0, errors.New("invalid_mcp_transfer_config")
	}
	return value, nil
}
