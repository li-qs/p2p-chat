package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"p2pchat/internal/config"
	"p2pchat/internal/transport"
	"p2pchat/internal/transport/message"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

type CLI struct {
	node        *transport.Node
	reader      *bufio.Reader
	currentPeer peer.ID

	mu sync.Mutex
}

func main() {
	var cfg config.Config

	if err := config.LoadConfig("./config.yaml", &cfg); err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	cli := &CLI{
		reader: bufio.NewReader(os.Stdin),
	}

	maddr := fmt.Sprintf("/ip4/%s/tcp/%d", cfg.Host, cfg.Port)

	node, err := transport.NewNode(ctx, maddr, &Handler{PrintPrompt: cli.printPrompt})
	if err != nil {
		panic(err)
	}
	defer node.Close()

	cli.node = node

	fmt.Println("--- p2p-chat started ---")
	fmt.Println("id:", node.PeerID())
	fmt.Println("------------------------")

	go func() {
		<-sigCh
		cli.println("\nshutdown...")
		cancel()
		node.Close()
		os.Exit(0)
	}()

	cli.Run()
}

func (c *CLI) Run() {
	for {
		c.mu.Lock()
		c.printPrompt()
		c.mu.Unlock()

		line, err := c.reader.ReadString('\n')
		if err != nil {
			c.println("read error:", err)
			return
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// 命令行模式
		if strings.HasPrefix(line, "/") {
			c.handleCommand(line[1:])
			continue
		}

		// 默认模式：发送文本消息
		c.sendText(line)
	}
}

func (c *CLI) handleCommand(line string) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return
	}

	cmd := parts[0]
	args := parts[1:]

	switch cmd {

	case "help":
		c.help()

	case "peers":
		c.listPeers()

	case "use":
		c.usePeer(args)

	case "file":
		c.sendFile(args)

	case "accept":
		c.acceptFile(args)

	case "reject":
		c.rejectFile(args)

	case "info":
		c.printInfo()

	case "clear":
		c.clear()

	case "exit":
		os.Exit(0)

	default:
		c.println("unknown command:", cmd)
	}
}

func (c *CLI) help() {
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

func (c *CLI) printInfo() {
	fmt.Println()
	fmt.Println("id:", c.node.PeerID())
	fmt.Println()
}

func (c *CLI) listPeers() {
	peers := c.node.ActivePeers()

	if len(peers) == 0 {
		c.println("no peers")
		return
	}

	fmt.Println()

	for i, p := range peers {
		flag := " "
		if p == c.currentPeer {
			flag = "*"
		}

		fmt.Printf("[%d]%s %s\n", i, flag, p.String())
	}

	fmt.Println()
}

func (c *CLI) usePeer(args []string) {
	if len(args) < 1 {
		c.println("usage: /use <index>")
		return
	}

	i, err := strconv.Atoi(args[0])
	if err != nil {
		c.println("invalid index")
		return
	}

	peers := c.node.ActivePeers()
	if i < 0 || i >= len(peers) {
		c.println("peer not found")
		return
	}

	c.currentPeer = peers[i]
	c.println("current peer:", c.currentPeer)
}

func (c *CLI) sendText(text string) {
	if c.currentPeer == "" {
		c.println("no peer selected (/use <index>)")
		return
	}

	err := c.node.Send(c.currentPeer, message.TextPayload{Text: text})

	if err != nil {
		c.println("send failed:", err)
		return
	}
}

func (c *CLI) sendFile(args []string) {
	if c.currentPeer == "" {
		c.println("no peer selected (/use <index>)")
		return
	}

	if len(args) < 1 {
		c.println("usage: /file <path>")
		return
	}

	path := strings.Join(args, " ")

	err := c.node.SendFile(c.currentPeer, path)
	if err != nil {
		c.println("send file failed:", err)
		return
	}

	c.println("sending file:", path)
}

func (c *CLI) acceptFile(args []string) {
	if c.currentPeer == "" {
		c.println("no peer selected (/use <index>)")
		return
	}

	if len(args) < 1 {
		c.println("usage: /accept <fileID>")
		return
	}

	fileID := strings.Join(args, " ")

	err := c.node.AcceptFile(c.currentPeer, fileID)
	if err != nil {
		c.println("accept file failed:", err)
		return
	}

	c.println("accept file:", fileID)
}

func (c *CLI) rejectFile(args []string) {
	if c.currentPeer == "" {
		c.println("no peer selected (/use <index>)")
		return
	}

	if len(args) < 1 {
		c.println("usage: /reject <fileID>")
		return
	}

	fileID := strings.Join(args, " ")

	err := c.node.RejectFile(c.currentPeer, fileID)
	if err != nil {
		c.println("reject file failed:", err)
		return
	}

	c.println("reject file:", fileID)
}

func (c *CLI) clear() {
	fmt.Print("\033[H\033[2J")
}

func (c *CLI) printPrompt() {
	if c.currentPeer == "" {
		fmt.Print("[p2p-chat]> ")
		return
	}
	fmt.Printf("[p2p-chat][%s]> ", c.currentPeer)
}

func (c *CLI) println(a ...any) {
	fmt.Print("\r")
	fmt.Println(a...)
}

type Handler struct {
	PrintPrompt func()
}

func (h *Handler) OnSessionClosed(peerID peer.ID, err error) {
	fmt.Printf("\nsession closed: %s, error: %v\n", peerID.String(), err)
	h.PrintPrompt()
}

func (h *Handler) OnTextMessage(peerID peer.ID, text string, timestamp int64) {
	fmt.Println("")
	fmt.Println("--------- message ---------")
	fmt.Println("from:", peerID.String())
	fmt.Println("content:", text)
	fmt.Println("time:", time.UnixMilli(timestamp).Format("2006-01-02 15:04:05"))
	fmt.Println("---------------------------")
	h.PrintPrompt()
}

func (h *Handler) OnFileMeta(peerID peer.ID, meta *message.FileMetaPayload, timestamp int64) {
	fmt.Println("")
	fmt.Println("--------- file ---------")
	fmt.Println("from:", peerID.String())
	fmt.Println("file ID:", meta.FileID)
	fmt.Println("name:", meta.Name)
	fmt.Println("size:", meta.Size)
	fmt.Println("hash algo:", meta.HashAlgo)
	fmt.Println("hash:", meta.Hash)
	fmt.Println("time:", time.UnixMilli(timestamp).Format("2006-01-02 15:04:05"))
	fmt.Println("------------------------")
	h.PrintPrompt()
}

func (h *Handler) OnFileReceived(peerID peer.ID, fileID string) {
	fmt.Printf("\nfile received: %s, from: %s\n", fileID, peerID.String())
	h.PrintPrompt()
}
