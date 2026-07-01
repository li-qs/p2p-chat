package transport

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"p2pchat/internal/infra/transport/protocol"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

type session struct {
	host       host.Host
	peerID     peer.ID
	lastActive atomic.Int64

	eventCh chan<- Event

	stream network.Stream
	rw     *bufio.ReadWriter
	mu     sync.RWMutex

	ctx      context.Context
	cancel   context.CancelFunc
	closed   atomic.Bool
	closeErr error
}

const (
	ErrSessionClosed    = "session closed"
	ErrSessionNotActive = "session not active"
	ErrStreamExisted    = "stream existed"
	ErrUnknownMessage   = "unknown message"
	ErrRemoteClosed     = "remote peer closed"
)

func newSession(ctx context.Context, peerID peer.ID, host host.Host, eventCh chan<- Event) *session {
	subCtx, cancel := context.WithCancel(ctx)
	s := &session{
		host:       host,
		peerID:     peerID,
		lastActive: atomic.Int64{},
		eventCh:    eventCh,
		ctx:        subCtx,
		cancel:     cancel,
	}
	return s
}

func (s *session) close() {
	s.closeWithError(nil)
}

// 查询 close 错误信息
func (s *session) error() error {
	return s.closeErr
}

// 是否活跃
func (s *session) isActive(d time.Duration) bool {
	last := s.lastActive.Load()
	now := time.Now().UnixMilli()
	return now-last < int64(d.Milliseconds())
}

// 更新活跃时间
func (s *session) updateLastActive() {
	s.lastActive.Store(time.Now().UnixMilli())
}

func (s *session) closeWithError(err error) {
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
}

// 绑定 stream，并开始监听消息。如果已经绑定过，则拒绝替换
func (s *session) bindStream(stream network.Stream) error {
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
func (s *session) ensureStream() error {
	if s.closed.Load() {
		return errors.New(ErrSessionClosed)
	}

	s.mu.RLock()
	if s.stream != nil {
		s.mu.RUnlock()
		return nil
	}
	s.mu.RUnlock()

	stream, err := s.host.NewStream(s.ctx, s.peerID, msgProtocolID)
	if err != nil {
		addrs := s.host.Peerstore().Addrs(s.peerID)
		conns := s.host.Network().ConnsToPeer(s.peerID)
		slog.Warn("dial failed", "peer", s.peerID.ShortString(), "addrs", len(addrs), "conns", len(conns), "err", err)
		return fmt.Errorf("peer %s unreachable: %w", s.peerID.ShortString(), err)
	}

	if err := s.bindStream(stream); err != nil {
		_ = stream.Reset()
		return err
	}
	return nil
}

// 发消息
func (s *session) send(ctx context.Context, msg protocol.Message) error {
	if s.closed.Load() {
		return errors.New(ErrSessionClosed)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

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
	s.updateLastActive()
	return nil
}

// 发送文件：直接推送数据，不等待确认
func (s *session) sendFile(filePath, transID, fileName string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	stream, err := s.host.NewStream(s.ctx, s.peerID, fileProtocolID)
	if err != nil {
		return err
	}

	if err := s.writeFile(stream, file, transID, fileName); err != nil {
		_ = stream.Reset()
		return err
	}

	_ = stream.Close()
	return nil
}

func (s *session) writeFile(stream network.Stream, file *os.File, transID, fileName string) error {
	w := bufio.NewWriter(stream)
	err := protocol.WriteFileInfo(w, &protocol.FileIntoPacket{TransferID: transID, FileName: fileName})
	if err != nil {
		return err
	}
	s.updateLastActive()

	err = protocol.WriteFileChunks(w, file, func() {
		s.updateLastActive()
	})
	if err != nil {
		return err
	}

	return nil
}

// 从 stream 中读文件
func (s *session) readFile(stream network.Stream, saveDir string) error {
	r := bufio.NewReader(stream)
	h, err := protocol.ReadFileInfo(r)
	if err != nil {
		return err
	}

	path := filepath.Join(saveDir, h.FileName)
	if _, err := os.Stat(path); err == nil {
		ext := filepath.Ext(h.FileName)
		base := h.FileName[:len(h.FileName)-len(ext)]
		path = filepath.Join(saveDir, fmt.Sprintf("%s_%d%s", base, time.Now().UnixMilli(), ext))
	}

	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	err = protocol.ReadFileChunks(r, file, func() {
		s.updateLastActive()
	})
	if err != nil {
		return err
	}

	s.eventCh <- FileReceivedEvent{
		TransferID: h.TransferID,
		SavePath:   path,
	}

	return nil
}

// 监听消息
func (s *session) listen() {
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		p, err := protocol.Read(s.rw.Reader)
		if err != nil {
			slog.Warn("protocol read error", "err", err)
			return
		}

		s.updateLastActive()
		s.handlePacket(p)
	}
}

func (s *session) handlePacket(p *protocol.Packet) {
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
func (s *session) handleTextMessage(p *protocol.Packet) {
	msg, err := protocol.Unmarshal[protocol.MessageText](p)
	if err != nil {
		slog.Debug("decode payload", "type", "text", "err", err)
		return
	}
	s.eventCh <- MessageEvent{
		From:      s.peerID,
		Timestamp: msg.Timestamp,
		Text:      msg.Text,
	}
}

// 处理传文件请求：对方想要发送一个文件，询问你是否接收
func (s *session) handleFileMeta(p *protocol.Packet) {
	msg, err := protocol.Unmarshal[protocol.MessageFileMeta](p)
	if err != nil {
		slog.Debug("decode payload", "type", "file", "err", err)
		return
	}
	s.eventCh <- FileMetaEvent{
		From:       s.peerID,
		Timestamp:  msg.Timestamp,
		TransferID: msg.TransferID,
		Name:       msg.Name,
		Size:       msg.Size,
	}
}

// 处理传文件响应：对方同意接收你发送的文件
func (s *session) handleFileAccept(p *protocol.Packet) {
	msg, err := protocol.Unmarshal[protocol.MessageFileAccept](p)
	if err != nil {
		slog.Debug("decode payload", "type", "file", "err", err)
		return
	}
	s.eventCh <- FileAcceptedEvent{From: s.peerID, TransferID: msg.TransferID}
}

// 处理传文件响应：对方拒绝接收你发送的文件
func (s *session) handleFileReject(p *protocol.Packet) {
	msg, err := protocol.Unmarshal[protocol.MessageFileReject](p)
	if err != nil {
		slog.Debug("decode payload", "type", "file", "err", err)
		return
	}
	s.eventCh <- FileRejectedEvent{From: s.peerID, TransferID: msg.TransferID}
}
