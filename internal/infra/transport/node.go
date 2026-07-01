package transport

import (
	"context"
	"crypto/rand"
	"fmt"
	"os"
	"p2pchat/internal/infra/transport/protocol"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	p2pProtocol "github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
)

type Node struct {
	host     host.Host
	mdns     *discovery
	sessions sync.Map

	eventCh chan<- Event

	fileSaveDir string

	ctx    context.Context
	cancel context.CancelFunc
	closed atomic.Bool
}

const (
	msgProtocolID  = p2pProtocol.ID("/chat/msg/1.0.0")
	fileProtocolID = p2pProtocol.ID("/chat/file/1.0.0")
)

func NewNode(ctx context.Context, maddrs []string, eventCh chan<- Event, fileSaveDir string) (*Node, error) {
	if err := os.MkdirAll(fileSaveDir, 0755); err != nil {
		return nil, err
	}

	prvKey, _, err := crypto.GenerateKeyPairWithReader(crypto.RSA, 2048, rand.Reader)
	if err != nil {
		return nil, err
	}

	h, err := libp2p.New(
		libp2p.ListenAddrStrings(maddrs...),
		libp2p.Identity(prvKey),
	)
	if err != nil {
		return nil, err
	}
	h.Network().Notify(&network.NotifyBundle{
		ConnectedF: func(n network.Network, c network.Conn) {
			eventCh <- PeerConnectedEvent{PeerID: c.RemotePeer()}
		},
		DisconnectedF: func(n network.Network, c network.Conn) {
			eventCh <- PeerConnectedEvent{PeerID: c.RemotePeer()}
		},
	})

	subCtx, cancel := context.WithCancel(ctx)

	mdnsService, err := initDiscovery(subCtx, h)
	if err != nil {
		cancel()
		return nil, err
	}

	node := &Node{
		host:        h,
		mdns:        mdnsService,
		eventCh:     eventCh,
		fileSaveDir: fileSaveDir,
		ctx:         subCtx,
		cancel:      cancel,
	}

	h.SetStreamHandler(msgProtocolID, node.streamHandler)
	h.SetStreamHandler(fileProtocolID, node.fileStreamHandler)

	go node.cleanSessoin(30 * time.Second)

	return node, nil
}

// 本机 ID
func (n *Node) PeerID() peer.ID {
	return n.host.ID()
}

// 本机地址
func (n *Node) Multiaddrs() []multiaddr.Multiaddr {
	return n.host.Addrs()
}

// 活跃的节点
func (n *Node) ActivePeers() []peer.ID {
	return n.host.Network().Peers()
}

// 已知的节点：包括活跃和不活跃的节点
func (n *Node) Peers() []peer.ID {
	return n.host.Peerstore().Peers()
}

func (n *Node) Close() {
	if !n.closed.CompareAndSwap(false, true) {
		return
	}

	n.cancel()

	if n.mdns != nil {
		_ = n.mdns.close()
	}

	_ = n.host.Close()

	n.sessions.Clear()
}

// 发送消息
func (n *Node) Send(ctx context.Context, peerID peer.ID, msg protocol.Message) error {
	return n.getOrCreateSession(peerID).send(ctx, msg)
}

// 发送文件
func (n *Node) SendFile(ctx context.Context, peerID peer.ID, msg protocol.MessageFileMeta, filePath string) error {
	return n.getOrCreateSession(peerID).sendFile(ctx, msg, filePath)
}

// 发消息：同意传输文件
func (n *Node) AcceptFile(ctx context.Context, peerID peer.ID, msg protocol.MessageFileAccept) error {
	return n.getOrCreateSession(peerID).send(ctx, msg)
}

// 发消息：拒绝传输文件
func (n *Node) RejectFile(ctx context.Context, peerID peer.ID, msg protocol.MessageFileReject) error {
	return n.getOrCreateSession(peerID).send(ctx, msg)
}

// 当对端主动建立 stream：绑定到 session，如果绑定失败，则强制关闭这个 stream
func (n *Node) streamHandler(s network.Stream) {
	peerID := s.Conn().RemotePeer()
	err := n.getOrCreateSession(peerID).bindStream(s)
	if err != nil {
		// TODO: 错误事件
		_ = s.Reset()
	}
}

// 当对端主动建立文件 stream：开始读取文件，如果读取失败，则强行关闭这个 stream
func (n *Node) fileStreamHandler(s network.Stream) {
	peerID := s.Conn().RemotePeer()
	err := n.getOrCreateSession(peerID).readFile(s, n.fileSaveDir)
	if err != nil {
		// TODO: 错误事件
		_ = s.Reset()
		return
	}

	_ = s.Close()
}

// 定期清理不活跃的 session
func (n *Node) cleanSessoin(d time.Duration) {
	if d < time.Second {
		d = time.Second
	}

	ticker := time.NewTicker(d)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.deleteInactiveSessions(3 * d)
		case <-n.ctx.Done():
			return
		}
	}
}

func (n *Node) getOrCreateSession(peerID peer.ID) *session {
	newSession := newSession(n.ctx, peerID, n.host, n.eventCh)

	actual, loaded := n.sessions.LoadOrStore(peerID, newSession)
	if loaded {
		newSession.close()
		return actual.(*session)
	}

	return newSession
}

func (n *Node) deleteInactiveSessions(timeout time.Duration) {
	n.sessions.Range(func(key, value any) bool {
		if n.closed.Load() {
			return false
		}
		sess := value.(*session)
		if !sess.isActive(timeout) {
			n.sessions.Delete(key)
			sess.closeWithError(fmt.Errorf(ErrSessionNotActive))
		}
		return true
	})
}
