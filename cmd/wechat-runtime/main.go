package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/JFSAS/wechat-runtime/internal/host"
	"github.com/JFSAS/wechat-runtime/internal/onebot"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "wechat-runtime:", err)
		os.Exit(1)
	}
}
func run(args []string) error {
	command := "serve"
	if len(args) > 0 && args[0] != "" && args[0][0] != '-' {
		command, args = args[0], args[1:]
	}
	if command == "catalog" {
		return json.NewEncoder(os.Stdout).Encode((&onebot.Runtime{}).Actions())
	}
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	path := flags.String("config", "runtime.local.json", "runtime configuration file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	cfg, err := host.ReadConfig(*path)
	if err != nil {
		return err
	}
	if command == "check" {
		fmt.Println("configuration valid; no runtime started")
		return nil
	}
	if command != "serve" {
		return fmt.Errorf("unknown command %q", command)
	}
	runtime, err := host.New(cfg)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err = runtime.Start(ctx); err != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = runtime.Close(closeCtx)
		return err
	}
	fmt.Println("management:", "http://"+runtime.Address())
	fmt.Printf("accounts: %d; modes are explicit in the configuration\n", len(cfg.Accounts))
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	return runtime.Close(shutdown)
}
