// wispcat-credentials exports the original main's verified Mac disk bootstrap.
// It never connects to WeChat or reads another runtime's private checkpoint.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"

	"github.com/JFSAS/wispcat/internal/account"
	"github.com/JFSAS/wispcat/internal/credentials"
	"github.com/JFSAS/wispcat/internal/protocol"
)

func main() {
	err := run(os.Args[1:], os.Stdout, func(app string) (protocol.Bootstrap, error) {
		if runtime.GOOS != "darwin" {
			return protocol.Bootstrap{}, account.ErrDiskPlatform
		}
		return (account.Source{App: app}).Load()
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer, load func(string) (protocol.Bootstrap, error)) error {
	flags := flag.NewFlagSet("wispcat-credentials", flag.ContinueOnError)
	flags.SetOutput(out)
	app := flags.String("app", account.DefaultApp, "official WeChat .app installation")
	path := flags.String("out", "wispcat-login.wxlogin", "encrypted output file (must not exist)")
	env := flags.String("passphrase-env", "WISPCAT_CREDENTIAL_PASSPHRASE", "environment variable containing the export password (16+ bytes)")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return errors.New("invalid_export_arguments")
	}
	if flags.NArg() != 0 || *app == "" || *path == "" || *env == "" {
		return errors.New("invalid_export_arguments")
	}
	password := os.Getenv(*env)
	if len(password) < 16 || len(password) > 4096 {
		return credentials.ErrPassphrase
	}
	b, err := load(*app)
	if err != nil {
		var disk *account.ImportError
		if errors.As(err, &disk) {
			return fmt.Errorf("official_disk_import_failed: %s", disk.Stage)
		}
		return errors.New("official_disk_import_failed")
	}
	file, err := credentials.Seal(b, password)
	if err != nil {
		return err
	}
	if err := writeExclusive(*path, file); err != nil {
		return errors.New("credential_output_failed_check_path_or_existing_file")
	}
	_, err = fmt.Fprintln(out, "已生成加密凭证文件。请在目标运行时管理页选择对应账号，上传文件并输入导出密码。")
	return err
}

func writeExclusive(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".wxlogin-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, err = f.Write(data); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	// Link publishes only the completed private file and refuses replacement,
	// including an existing symlink. Both paths are on the same filesystem.
	return os.Link(f.Name(), path)
}
