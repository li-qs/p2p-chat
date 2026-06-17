package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"p2pchat/internal/config"
	"p2pchat/internal/event"
	"p2pchat/internal/transport"
	"p2pchat/internal/transport/protocol"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

var (
	bus  *event.EventBus
	node *transport.Node

	selectedPeer peer.ID
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Println("\nshutdown...")
		cancel()
		node.Close()
		os.Exit(0)
	}()

	config.Init("./config.yaml")
	bus = event.NewEventBus()
	initNode(ctx)

	handleEvents()

	fmt.Println("--- p2p-chat started ---")
	fmt.Println("id:", node.PeerID())
	fmt.Println("------------------------")

	r := bufio.NewReader(os.Stdin)
	for {
		printPrompt()

		line, err := r.ReadString('\n')
		if err != nil {
			println("read error:", err)
			return
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// 命令行模式
		if strings.HasPrefix(line, "/") {
			handleCommand(line[1:])
			continue
		}

		// 默认模式：发送文本消息
		sendText(line)
	}
}

func initNode(ctx context.Context) {
	var maddrs []string
	for _, ip := range config.Get().Bind {
		maddrs = append(maddrs, fmt.Sprintf("/ip4/%s/tcp/%d", ip, config.Get().Port))
	}
	var err error
	node, err = transport.NewNode(ctx, maddrs, bus, config.Get().FileDir)
	if err != nil {
		panic(err)
	}
}

func handleEvents() {
	ch1, _ := bus.Subscribe(event.SessionCreatedEvent{})
	go func() {
		for e := range ch1 {
			d := e.(event.SessionCreatedEvent)
			fmt.Printf("\nsession created: %s\n", d.PeerID.String())
		}
	}()

	ch2, _ := bus.Subscribe(event.SessionClosedEvent{})
	go func() {
		for e := range ch2 {
			d := e.(event.SessionClosedEvent)
			fmt.Printf("\nsession closed: %s\n", d.PeerID.String())
		}
	}()

	ch3, _ := bus.Subscribe(event.MessageEvent{})
	go func() {
		for e := range ch3 {
			d := e.(event.MessageEvent)
			fmt.Println("")
			fmt.Println("--------- message ---------")
			fmt.Println("from:", d.From.String())
			fmt.Println("content:", d.Text)
			fmt.Println("time:", time.UnixMilli(d.Timestamp).Format("2006-01-02 15:04:05"))
			fmt.Println("---------------------------")
			printPrompt()
		}
	}()

	ch4, _ := bus.Subscribe(event.FileMetaEvent{})
	go func() {
		for e := range ch4 {
			d := e.(event.FileMetaEvent)
			fmt.Println("")
			fmt.Println("--------- file ---------")
			fmt.Println("from:", d.From.String())
			fmt.Println("transfer ID:", d.TransferID)
			fmt.Println("name:", d.Name)
			fmt.Println("size:", d.Size)
			fmt.Println("hash algo:", d.HashAlgo)
			fmt.Println("hash:", d.Hash)
			fmt.Println("time:", time.UnixMilli(d.Timestamp).Format("2006-01-02 15:04:05"))
			fmt.Println("------------------------")
			printPrompt()
		}
	}()

	ch5, _ := bus.Subscribe(event.FileReceivedEvent{})
	go func() {
		for e := range ch5 {
			d := e.(event.FileReceivedEvent)
			fmt.Printf("\nfile received: %s, save path: %s\n", d.TransferID, d.SavePath)
			printPrompt()
		}
	}()
}

func handleCommand(line string) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return
	}

	cmd := parts[0]
	args := parts[1:]

	switch cmd {
	case "help":
		help()
	case "peers":
		listPeers()
	case "use":
		usePeer(args)
	case "file":
		sendFile(args)
	case "accept":
		acceptFile(args)
	case "reject":
		rejectFile(args)
	case "info":
		printInfo()
	case "clear":
		clear()
	case "exit":
		os.Exit(0)
	default:
		println("unknown command:", cmd)
	}
}

func help() {
	fmt.Println()
	fmt.Println("commands:")
	fmt.Println("  /help")
	fmt.Println("  /peers")
	fmt.Println("  /use <index>")
	fmt.Println("  /file <path>")
	fmt.Println("  /accept <fileID>")
	fmt.Println("  /reject <fileID>")
	fmt.Println("  /info")
	fmt.Println("  /clear")
	fmt.Println("  /exit")
	fmt.Println()
}

func listPeers() {
	peers := node.ActivePeers()

	if len(peers) == 0 {
		println("no peers")
		return
	}

	fmt.Println()

	for i, p := range peers {
		flag := " "
		if p == selectedPeer {
			flag = "*"
		}

		fmt.Printf("[%d]%s %s\n", i, flag, p.String())
	}

	fmt.Println()
}

func usePeer(args []string) {
	if len(args) < 1 {
		println("usage: /use <index>")
		return
	}

	i, err := strconv.Atoi(args[0])
	if err != nil {
		println("invalid index")
		return
	}

	peers := node.ActivePeers()
	if i < 0 || i >= len(peers) {
		println("peer not found")
		return
	}

	selectedPeer = peers[i]
}

func sendFile(args []string) {
	if selectedPeer == "" {
		println("no peer selected (/use <index>)")
		return
	}

	if len(args) < 1 {
		println("usage: /file <path>")
		return
	}

	path := strings.Join(args, " ")

	err := node.SendFile(selectedPeer, path)
	if err != nil {
		println("send file failed:", err)
		return
	}

	println("sending file:", path)
}

func acceptFile(args []string) {
	if selectedPeer == "" {
		println("no peer selected (/use <index>)")
		return
	}

	if len(args) < 1 {
		println("usage: /accept <fileID>")
		return
	}

	fileID := strings.Join(args, " ")

	err := node.AcceptFile(selectedPeer, fileID)
	if err != nil {
		println("accept file failed:", err)
		return
	}

	println("accept file:", fileID)
}

func rejectFile(args []string) {
	if selectedPeer == "" {
		println("no peer selected (/use <index>)")
		return
	}

	if len(args) < 1 {
		println("usage: /reject <fileID>")
		return
	}

	fileID := strings.Join(args, " ")

	err := node.RejectFile(selectedPeer, fileID)
	if err != nil {
		println("reject file failed:", err)
		return
	}

	println("reject file:", fileID)
}

func printInfo() {
	fmt.Println()
	fmt.Println("id:", node.PeerID())
	fmt.Println()
}

func clear() {
	fmt.Print("\033[H\033[2J")
}

func sendText(text string) {
	if selectedPeer == "" {
		println("no peer selected (/use <index>)")
		return
	}

	err := node.Send(selectedPeer, protocol.MessageText{Text: text})

	if err != nil {
		println("send failed:", err)
		return
	}
}

func printPrompt() {
	if selectedPeer == "" {
		fmt.Print("[p2p-chat]> ")
		return
	}
	fmt.Printf("[p2p-chat][%s]> ", selectedPeer)
}

func println(a ...any) {
	fmt.Print("\r")
	fmt.Println(a...)
}
