package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"syscall"

	"p2pchat/internal/config"
	"p2pchat/internal/hub"
)

func main() {
	cfg, err := config.Load("./config.yaml")
	if err != nil {
		slog.Error("load config", "err", err)
		os.Exit(1)
	}
	setupLogging(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, err := hub.New(ctx)
	if err != nil {
		slog.Error("init failed", "err", err)
		os.Exit(1)
	}

	port, err := h.Start()
	if err != nil {
		slog.Error("start failed", "err", err)
		os.Exit(1)
	}

	openURL := fmt.Sprintf("http://127.0.0.1:%d/ui", port)
	slog.Info("opening browser", "url", openURL)
	if err := openBrowser(openURL); err != nil {
		slog.Warn("failed to open browser", "err", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	slog.Info("shutting down")
	cancel()
}

func openBrowser(url string) error {
	var err error
	switch runtime.GOOS {
	case "linux":
		err = exec.Command("xdg-open", url).Start()
	case "windows":
		err = exec.Command("cmd", "/c", "start", url).Start()
	case "darwin":
		err = exec.Command("open", url).Start()
	default:
		err = fmt.Errorf("不支持的平台: %s", runtime.GOOS)
	}
	return err
}

func setupLogging(cfg *config.Config) {
	var l slog.Level
	switch cfg.LogLevel {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	slog.SetLogLoggerLevel(l)
}
