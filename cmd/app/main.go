package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"syscall"

	"p2pchat/internal/hub"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h, err := hub.New(ctx)
	if err != nil {
		log.Printf("init failed: %v\n", err)
		os.Exit(1)
	}

	port, err := h.Start()
	if err != nil {
		log.Fatalln(err)
	}

	url := fmt.Sprintf("http://127.0.0.1:%d/ui", port)
	fmt.Printf("open browser: %s\n", url)
	if err := openBrowser(url); err != nil {
		log.Println("failed to open browser")
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	log.Println("\nshutdown...")
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
