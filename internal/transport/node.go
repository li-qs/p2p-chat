package transport

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"p2pchat/internal/transport/discovery"
	"p2pchat/internal/transport/message"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
)

type Node struct {
	host    host.Host
	mdns    *discovery.MdnsService
	handler Handler

	manager *SessionManager

	ctx    context.Context
	cancel context.CancelFunc
	closed atomic.Bool
}

const (
	MainProtocolID = protocol.ID("/chat/main/1.0.0")
	FileProtocolID = protocol.ID("/chat/file/1.0.0")
)

func NewNode(ctx context.Context, maddr string, handler Handler) (*Node, error) {
	prvKey, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
	if err != nil {
		return nil, err
	}

	h, err := libp2p.New(
		libp2p.ListenAddrStrings(maddr),
		libp2p.Identity(prvKey),
	)
	if err != nil {
		return nil, err
	}

	subCtx, cancel := context.WithCancel(ctx)

	mdnsService, err := discovery.InitMdns(subCtx, h)
	if err != nil {
		cancel()
		return nil, err
	}

	node := &Node{
		host:    h,
		mdns:    mdnsService,
		manager: NewSessionManager(),
		handler: handler,
		ctx:     subCtx,
		cancel:  cancel,
	}

	h.SetStreamHandler(MainProtocolID, node.streamHandler)
	h.SetStreamHandler(FileProtocolID, node.fileStreamHandler)

	go node.cleanupLoop(30 * time.Second)

	return node, nil
}

// 本机 ID
func (n *Node) PeerID() peer.ID {
	return n.host.ID()
}

// 本机地址
func (n *Node) Addrs() []multiaddr.Multiaddr {
	return n.host.Addrs()
}

// 活跃的节点
func (n *Node) ActivePeers() []peer.ID {
	return n.host.Network().Peers()
}

// 已知的节点：包括活跃和不活跃的节点
func (n *Node) AllPeers() []peer.ID {
	return n.host.Peerstore().Peers()
}

func (n *Node) Close() {
	if !n.closed.CompareAndSwap(false, true) {
		return
	}

	n.cancel()

	if n.mdns != nil {
		_ = n.mdns.Close()
	}

	_ = n.host.Close()

	n.manager.Clear()
}

// 发送消息
func (n *Node) Send(peerID peer.ID, payload message.Payload) error {
	session, _ := n.getOrCreateSession(peerID)
	return session.Send(payload)
}

// 广播消息：谨慎使用！！！并非真实的广播机制，实际是给每个节点单独发送一次
func (n *Node) Broadcast(payload message.Payload) {
	n.manager.Range(func(peerID peer.ID, s *Session) bool {
		_ = s.Send(payload)
		return true
	})
}

// 发送文件
func (n *Node) SendFile(peerID peer.ID, path string) error {
	session, _ := n.getOrCreateSession(peerID)
	return session.SendFile(path, time.Minute)
}

// 同意接收文件
func (n *Node) AcceptFile(peerID peer.ID, fileID string) error {
	session, _ := n.getOrCreateSession(peerID)
	return session.AcceptFile(fileID)
}

// 拒绝接收文件
func (n *Node) RejectFile(peerID peer.ID, fileID string) error {
	session, _ := n.getOrCreateSession(peerID)
	return session.RejectFile(fileID)
}

// 当对端主动建立 stream：绑定到 session，如果绑定失败，则强制关闭这个 stream
func (n *Node) streamHandler(s network.Stream) {
	session, _ := n.getOrCreateSession(s.Conn().RemotePeer())
	err := session.BindStream(s)
	if err != nil {
		_ = s.Reset()
	}
}

// 当对端主动建立文件 stream：开始读取文件，如果读取失败，则强行关闭这个 stream
func (n *Node) fileStreamHandler(s network.Stream) {
	tempDir := "./temp"
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		fmt.Println(err)
		_ = s.Reset()
		return
	}

	session, _ := n.getOrCreateSession(s.Conn().RemotePeer())
	if err := session.ReadFile(s, tempDir); err != nil {
		_ = s.Reset()
		fmt.Println(err)
		return
	}

	_ = s.Close()
}

func (n *Node) getOrCreateSession(peerID peer.ID) (session *Session, loaded bool) {
	s := NewSession(n.ctx, n.host, peerID, n.handler)
	return n.manager.LoadOrStore(peerID, s)
}

// 定期清理不活跃的 session
func (n *Node) cleanupLoop(d time.Duration) {
	if d < time.Second {
		d = time.Second
	}

	ticker := time.NewTicker(d)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.manager.Range(func(peerID peer.ID, s *Session) bool {
				if n.closed.Load() {
					return false
				}
				// 采取宽松策略，放大过期时间：网络抖动等因素，可能导致活跃时间更新不及时
				if !s.IsActive(3 * d) {
					s.Close()
					n.manager.Delete(peerID)
				}
				return true
			})
		case <-n.ctx.Done():
			return
		}
	}
}
