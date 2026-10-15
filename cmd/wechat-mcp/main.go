package main

import (
	"context"
	"fmt"
	"os"

	"github.com/JFSAS/wechat-runtime/internal/mcp"
	"github.com/JFSAS/wechat-runtime/pkg/client"
)

func main() {
	mode := os.Getenv("WECHAT_MCP_MODE")
	endpoint := os.Getenv("WECHAT_MCP_ENDPOINT")
	if mode == "" {
		mode = "docs"
		if endpoint != "" {
			mode = "read"
		}
	}
	server := mcp.Server{Mode: mode}
	if mode != "docs" {
		c, err := client.New(endpoint, os.Getenv("WECHAT_MCP_TOKEN"))
		if err != nil {
			fmt.Fprintln(os.Stderr, "MCP endpoint or token is invalid")
			os.Exit(1)
		}
		server.Client = c
	}
	if err := server.Run(context.Background(), os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
