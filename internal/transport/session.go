package transport

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"p2pchat/internal/event"
	"p2pchat/internal/transport/protocol"
	"p2pchat/internal/utils"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

type Session struct {
	host       host.Host
	bus        *event.EventBus
	PeerID     peer.ID
	lastAcvite atomic.Int64

	stream network.Stream
	rw     *bufio.ReadWriter
	mu     sync.RWMutex

	pendingTransfer sync.Map

	ctx      context.Context
	cancel   context.CancelFunc
	closed   atomic.Bool
	closeErr error
}

type Transfer struct {
	TransferID string
	accepted   chan struct{}
	reject     chan struct{}
}

const (
	ErrSessionClosed    = "session closed"
	ErrSessionNotActive = "session not active"
	ErrStreamExisted    = "stream existed"
	ErrUnknownMessage   = "unknown message"
	ErrRemoteClosed     = "remote peer closed"
)

func NewSession(ctx context.Context, host host.Host, bus *event.EventBus, peerID peer.ID) *Session {
	subCtx, cancel := context.WithCancel(ctx)
	s := &Session{
		host:            host,
		bus:             bus,
		PeerID:          peerID,
		lastAcvite:      atomic.Int64{},
		pendingTransfer: sync.Map{},
		ctx:             subCtx,
		cancel:          cancel,
	}
	s.bus.Publish(event.SessionCreatedEvent{PeerID: peerID})
	return s
}

func (s *Session) Close() {
	s.closeWithError(nil)
}

// 查询 close 错误信息
func (s *Session) Error() error {
	return s.closeErr
}

// 是否活跃
func (s *Session) IsActive(d time.Duration) bool {
	last := s.lastAcvite.Load()
	now := time.Now().UnixMilli()
	return now-last < int64(d.Milliseconds())
}

// 更新活跃时间
func (s *Session) UpdateLastActive() {
	s.lastAcvite.Store(time.Now().UnixMilli())
}

func (s *Session) closeWithError(err error) {
	if !s.closed.CompareAndSwap(false, true) {
		return
	}

	s.closeErr = err

	if s.cancel != nil {
		s.cancel()
	}

	s.mu.Lock()
	stream := s.stream
	s.stream = nil
	s.rw = nil
	s.mu.Unlock()

	if stream != nil {
		_ = stream.Close()
	}

	s.bus.Publish(event.SessionClosedEvent{PeerID: s.PeerID})
}

// 绑定 stream，并开始监听消息。如果已经绑定过，则拒绝替换
func (s *Session) BindStream(stream network.Stream) error {
	if s.closed.Load() {
		return errors.New(ErrSessionClosed)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stream != nil {
		return errors.New(ErrStreamExisted)
	}
	s.stream = stream
	s.rw = bufio.NewReadWriter(bufio.NewReader(stream), bufio.NewWriter(stream))

	go s.listen()
	return nil
}

// 确保有一个 stream
func (s *Session) ensureStream() error {
	if s.closed.Load() {
		return errors.New(ErrSessionClosed)
	}

	s.mu.RLock()
	if s.stream != nil {
		s.mu.RUnlock()
		return nil
	}
	s.mu.RUnlock()

	stream, err := s.host.NewStream(s.ctx, s.PeerID, MainProtocolID)
	if err != nil {
		return err
	}

	if err := s.BindStream(stream); err != nil {
		_ = stream.Reset()
		return err
	}
	return nil
}

// 发送消息
func (s *Session) Send(msg protocol.Message) error {
	p, err := protocol.Marshal(msg)
	if err != nil {
		return err
	}
	if err := s.ensureStream(); err != nil {
		return err
	}
	if err := protocol.Write(s.rw.Writer, p); err != nil {
		return err
	}
	s.UpdateLastActive()
	return nil
}

// 发送文件：先询问对方是否愿意接收，并等待对方响应，再根据响应决定是否发送文件
func (s *Session) SendFile(path string, timeout time.Duration) error {
	if s.closed.Load() {
		return errors.New(ErrSessionClosed)
	}

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	fileStat, err := file.Stat()
	if err != nil {
		return err
	}
	md5, err := utils.FileMD5(file)
	if err != nil {
		return err
	}

	transID := uuid.NewString()

	// 创建并记录文件传输信号
	trans := &Transfer{
		TransferID: transID,
		accepted:   make(chan struct{}),
		reject:     make(chan struct{}),
	}
	s.pendingTransfer.Store(transID, trans)
	defer s.pendingTransfer.Delete(transID)

	// 发送询问：是否接收文件
	err = s.Send(protocol.MessageFileMeta{
		TransferID: transID,
		Name:       fileStat.Name(),
		Size:       fileStat.Size(),
		HashAlgo:   "md5",
		Hash:       md5,
	})
	if err != nil {
		return err
	}

	// TODO: 分离等待过程，在后台等待，函数立即 return

	// 等待信号：允许/拒绝发送文件，超时退出
	select {
	case <-trans.accepted:
		s.bus.Publish(event.FileAcceptedEvent{TransferID: transID})
		return s.sendFile(file, transID)
	case <-trans.reject:
		s.bus.Publish(event.FileRejectedEvent{TransferID: transID})
		return nil
	case <-time.After(timeout):
		s.bus.Publish(event.FileTimeoutEvent{TransferID: transID})
		return errors.New("waiting for file transfer timed out")
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// 发信号：允许发送文件
func (s *Session) startTransferFile(transID string) {
	val, ok := s.pendingTransfer.Load(transID)
	if ok {
		val.(*Transfer).accepted <- struct{}{}
	}
}

// 发信号：拒绝发送文件
func (s *Session) stopTransferFile(transID string) {
	val, ok := s.pendingTransfer.Load(transID)
	if ok {
		val.(*Transfer).reject <- struct{}{}
	}
}

// 发送文件
func (s *Session) sendFile(file *os.File, transID string) error {
	stream, err := s.host.NewStream(s.ctx, s.PeerID, FileProtocolID)
	if err != nil {
		return err
	}

	if err := s.WriteFile(stream, file, transID); err != nil {
		_ = stream.Reset()
		return err
	}

	_ = stream.Close()
	return nil
}

// 发消息：允许你给我发送文件
func (s *Session) AcceptFile(transID string) error {
	// TODO: 插入 save path
	return s.Send(protocol.MessageFileAccept{TransferID: transID})
}

// 发消息：拒绝你给我发送文件
func (s *Session) RejectFile(transID string) error {
	return s.Send(protocol.MessageFileReject{TransferID: transID})
}

// 向 stream 中写文件：先写入文件头，然后分片写入文件
// 一个文件一个 stream
func (s *Session) WriteFile(stream network.Stream, file *os.File, transID string) error {
	w := bufio.NewWriter(stream)
	err := protocol.WriteFileInfo(w, &protocol.FileIntoPacket{TransferID: transID})
	if err != nil {
		return err
	}
	s.UpdateLastActive()

	err = protocol.WriteFileChunks(w, file, func() {
		s.UpdateLastActive()
	})
	if err != nil {
		return err
	}

	return nil
}

// 从 stream 中读文件：先读取文件头，然后读取文件分片，按读取顺序拼接文件（因为发送分片也是顺序的）
// 一个文件一个 stream
func (s *Session) ReadFile(stream network.Stream, tempDir string) error {
	r := bufio.NewReader(stream)
	h, err := protocol.ReadFileInfo(r)
	if err != nil {
		return err
	}
	s.UpdateLastActive()

	path := fmt.Sprintf("%s/%d_%s", tempDir, time.Now().UnixMilli(), h.TransferID)
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	err = protocol.ReadFileChunks(r, file, func() {
		s.UpdateLastActive()
	})
	if err != nil {
		return err
	}

	s.bus.Publish(event.FileReceivedEvent{
		TransferID: h.TransferID,
		SavePath:   path,
	})

	return nil
}

// 监听消息
func (s *Session) listen() {
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		p, err := protocol.Read(s.rw.Reader)
		if err != nil {
			fmt.Println(err)
			return
		}

		s.UpdateLastActive()
		s.handlePacket(p)
	}
}

func (s *Session) handlePacket(p *protocol.Packet) {
	switch p.MessageType {
	case protocol.TypeText:
		s.handleTextMessage(p)
	case protocol.TypeFileMeta:
		s.handleFileMeta(p)
	case protocol.TypeFileAccept:
		s.handleFileAccept(p)
	case protocol.TypeFileReject:
		s.handleFileReject(p)
	default:
		s.closeWithError(errors.New(ErrUnknownMessage))
	}
}

// 处理文本消息
func (s *Session) handleTextMessage(p *protocol.Packet) {
	msg, err := protocol.Unmarshal[protocol.MessageText](p)
	if err != nil {
		fmt.Printf("decode payload error: %v", err)
		return
	}
	s.bus.Publish(event.MessageEvent{
		From:      s.PeerID,
		Timestamp: p.Timestamp,
		Text:      msg.Text,
	})
}

// 处理传文件请求：对方想要发送一个文件，询问你是否接收
func (s *Session) handleFileMeta(p *protocol.Packet) {
	msg, err := protocol.Unmarshal[protocol.MessageFileMeta](p)
	if err != nil {
		fmt.Printf("decode payload error: %v", err)
		return
	}
	s.bus.Publish(event.FileMetaEvent{
		From:       s.PeerID,
		Timestamp:  p.Timestamp,
		TransferID: msg.TransferID,
		Name:       msg.Name,
		Size:       msg.Size,
		HashAlgo:   msg.HashAlgo,
		Hash:       msg.Hash,
	})
}

// 处理传文件响应：对方同意接收你发送的文件
func (s *Session) handleFileAccept(p *protocol.Packet) {
	msg, err := protocol.Unmarshal[protocol.MessageFileAccept](p)
	if err != nil {
		fmt.Printf("decode payload error: %v", err)
		return
	}
	s.startTransferFile(msg.TransferID)
}

// 处理传文件响应：对方拒绝接收你发送的文件
func (s *Session) handleFileReject(p *protocol.Packet) {
	msg, err := protocol.Unmarshal[protocol.MessageFileReject](p)
	if err != nil {
		fmt.Printf("decode payload error: %v", err)
		return
	}
	s.stopTransferFile(msg.TransferID)
}
