package transport

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"p2pchat/internal/transport/message"
	"p2pchat/internal/utils"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

type Handler interface {
	OnSessionClosed(peerID peer.ID, err error)
	OnTextMessage(peerID peer.ID, text string, timestamp int64)
	OnFileMeta(peerID peer.ID, meta *message.FileMetaPayload, timestamp int64)
	OnFileReceived(peerID peer.ID, path string)
}

type Session struct {
	host       host.Host
	PeerID     peer.ID
	lastAcvite atomic.Int64
	handler    Handler

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
	FileID   string
	accepted chan struct{}
	reject   chan struct{}
}

const (
	ErrSessionClosed  = "session closed"
	ErrStreamExisted  = "stream existed"
	ErrUnknownMessage = "unknown message"
	ErrRemoteClosed   = "remote peer closed"
)

func NewSession(ctx context.Context, host host.Host, peerID peer.ID, handler Handler) *Session {
	subCtx, cancel := context.WithCancel(ctx)
	s := &Session{
		host:            host,
		PeerID:          peerID,
		lastAcvite:      atomic.Int64{},
		handler:         handler,
		pendingTransfer: sync.Map{},
		ctx:             subCtx,
		cancel:          cancel,
	}
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
	return time.Now().UnixMilli()-s.lastAcvite.Load() > int64(d.Milliseconds())
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

	if s.handler != nil {
		s.handler.OnSessionClosed(s.PeerID, err)
	}
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
func (s *Session) Send(payload message.Payload) error {
	msg, err := message.NewMessage(s.host.ID(), s.PeerID.String(), payload)
	if err != nil {
		return err
	}
	if err := s.send(msg); err != nil {
		return err
	}
	return nil
}

func (s *Session) send(msg *message.Message) error {
	if err := s.ensureStream(); err != nil {
		return err
	}
	if err := message.WriteMessage(s.rw.Writer, msg); err != nil {
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

	fileID := uuid.NewString()

	// 创建并记录文件传输信号
	trans := &Transfer{
		FileID:   fileID,
		accepted: make(chan struct{}),
		reject:   make(chan struct{}),
	}
	s.pendingTransfer.Store(fileID, trans)
	defer s.pendingTransfer.Delete(fileID)

	// 发送询问：是否接收文件
	err = s.Send(message.FileMetaPayload{
		FileID:   fileID,
		Name:     fileStat.Name(),
		Size:     fileStat.Size(),
		HashAlgo: "md5",
		Hash:     md5,
	})
	if err != nil {
		return err
	}

	// 等待信号：允许/拒绝发送文件，超时退出
	select {
	case <-trans.accepted:
		return s.sendFile(file, fileID)
	case <-trans.reject:
		return nil
	case <-time.After(timeout):
		return errors.New("waiting for file transfer timed out")
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// 发信号：允许发送文件
func (s *Session) startTransferFile(fileID string) {
	val, ok := s.pendingTransfer.Load(fileID)
	if ok {
		val.(*Transfer).accepted <- struct{}{}
	}
}

// 发信号：拒绝发送文件
func (s *Session) stopTransferFile(fileID string) {
	val, ok := s.pendingTransfer.Load(fileID)
	if ok {
		val.(*Transfer).reject <- struct{}{}
	}
}

// 发送文件
func (s *Session) sendFile(file *os.File, fileID string) error {
	stream, err := s.host.NewStream(s.ctx, s.PeerID, FileProtocolID)
	if err != nil {
		return err
	}

	if err := s.WriteFile(stream, file, fileID); err != nil {
		_ = stream.Reset()
		return err
	}

	_ = stream.Close()
	return nil
}

// 发消息：允许你给我发送文件
func (s *Session) AcceptFile(fileID string) error {
	return s.Send(message.FileAcceptPayload{FileID: fileID})
}

// 发消息：拒绝你给我发送文件
func (s *Session) RejectFile(fileID string) error {
	return s.Send(message.FileRejectPayload{FileID: fileID})
}

// 向 stream 中写文件：先写入文件头，然后分片写入文件
// 一个文件一个 stream
func (s *Session) WriteFile(stream network.Stream, file *os.File, fileID string) error {
	w := bufio.NewWriter(stream)
	err := message.WriteFileHeader(w, &message.FileHeader{FileID: fileID})
	if err != nil {
		return err
	}
	s.UpdateLastActive()

	buf := make([]byte, message.FileChunkSize)
	for {
		n, err := file.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if err = message.WriteFileChunk(w, buf[:n]); err != nil {
			return err
		}

		s.UpdateLastActive()
	}

	return nil
}

// 从 stream 中读文件：先读取文件头，然后读取文件分片，按读取顺序拼接文件（因为发送分片也是顺序的）
// 一个文件一个 stream
func (s *Session) ReadFile(stream network.Stream, tempDir string) error {
	r := bufio.NewReader(stream)
	h, err := message.ReadFileHeader(r)
	if err != nil {
		return err
	}
	s.UpdateLastActive()

	path := fmt.Sprintf("%s/%d_%s", tempDir, time.Now().UnixMilli(), h.FileID)
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	for {
		chunk, err := message.ReadFileChunk(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if _, err := file.Write(chunk); err != nil {
			return err
		}

		s.UpdateLastActive()
	}

	s.handler.OnFileReceived(s.PeerID, path)
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

		msg, err := message.ReadMessage(s.rw.Reader)
		if err != nil {
			fmt.Println(err)
			return
		}

		s.UpdateLastActive()
		s.handleMessage(msg)
	}
}

func (s *Session) handleMessage(msg *message.Message) {
	switch msg.Type {
	case message.MessageText:
		s.handleTextMessage(msg)
	case message.MessageFileMeta:
		s.handleFileMeta(msg)
	case message.MessageFileAccept:
		s.handleFileAccept(msg)
	case message.MessageFileReject:
		s.handleFileReject(msg)
	default:
		s.closeWithError(errors.New(ErrUnknownMessage))
	}
}

// 处理文本消息
func (s *Session) handleTextMessage(msg *message.Message) {
	p, err := message.GetPayload[message.TextPayload](msg)
	if err != nil {
		fmt.Printf("decode payload error: %v", err)
		return
	}
	s.handler.OnTextMessage(s.PeerID, p.Text, msg.Timestamp)
}

// 处理传文件请求：对方想要发送一个文件，询问你是否接收
func (s *Session) handleFileMeta(msg *message.Message) {
	p, err := message.GetPayload[message.FileMetaPayload](msg)
	if err != nil {
		fmt.Printf("decode payload error: %v", err)
		return
	}
	s.handler.OnFileMeta(s.PeerID, p, msg.Timestamp)
}

// 处理传文件响应：对方同意接收你发送的文件
func (s *Session) handleFileAccept(msg *message.Message) {
	p, err := message.GetPayload[message.FileAcceptPayload](msg)
	if err != nil {
		fmt.Printf("decode payload error: %v", err)
		return
	}
	s.startTransferFile(p.FileID)
}

// 处理传文件响应：对方拒绝接收你发送的文件
func (s *Session) handleFileReject(msg *message.Message) {
	p, err := message.GetPayload[message.FileRejectPayload](msg)
	if err != nil {
		fmt.Printf("decode payload error: %v", err)
		return
	}
	s.stopTransferFile(p.FileID)
}
