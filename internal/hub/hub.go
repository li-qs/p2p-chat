package hub

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"p2pchat/internal/domain"
	"p2pchat/internal/hub/web"
	"p2pchat/internal/infra/storage"
	"p2pchat/internal/infra/transport"
	"p2pchat/internal/infra/transport/protocol"

	"github.com/libp2p/go-libp2p/core/peer"
)

type Hub struct {
	ctx     context.Context
	eventCh chan transport.Event
	wsCh    chan web.WsMessage
	db      *storage.SQLite
	node    *transport.Node
	mux     http.Handler
	fileDir string
}

func New(ctx context.Context) (*Hub, error) {
	const defaultFileDir = "data/files"

	eventCh := make(chan transport.Event, 128)
	wsCh := make(chan web.WsMessage, 64)

	db, err := storage.NewSQLite("data/db/data.db")
	if err != nil {
		return nil, err
	}

	node, err := transport.NewNode(ctx, eventCh, defaultFileDir)
	if err != nil {
		return nil, err
	}

	h := &Hub{
		ctx:     ctx,
		eventCh: eventCh,
		wsCh:    wsCh,
		db:      db,
		node:    node,
		fileDir: defaultFileDir,
	}

	mux, err := web.NewServeMux(ctx, wsCh, h)
	if err != nil {
		return nil, err
	}
	h.mux = mux

	go h.run()

	return h, nil
}

func (h *Hub) run() {
	for {
		select {
		case <-h.ctx.Done():
			h.close()
			return
		case e := <-h.eventCh:
			switch ev := e.(type) {
			case transport.PeerConnectedEvent:
				h.onPeerConnectedEvent(ev)
			case transport.PeerDisconnectedEvent:
				h.onPeerDisconnectedEvent(ev)
			case transport.MessageEvent:
				h.onMessageEvent(ev)
			case transport.FileMetaEvent:
				h.onFileMetaEvent(ev)
			case transport.FileAcceptedEvent:
				h.onFileAcceptedEvent(ev)
			case transport.FileRejectedEvent:
				h.onFileRejectedEvent(ev)
			case transport.FileTimeoutEvent:
				h.onFileTimeoutEvent(ev)
			case transport.FileReceivedEvent:
				h.onFileReceivedEvent(ev)
			default:
				slog.Warn("unknown event", "type", fmt.Sprintf("%T", e))
			}
		}
	}
}

func (h *Hub) Start() (port int, err error) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0, err
	}
	port = l.Addr().(*net.TCPAddr).Port
	go http.Serve(l, h.mux)
	return port, nil
}

func (h *Hub) close() {
	h.node.Close()
	_ = h.db.DB.Close()
	close(h.eventCh)
	close(h.wsCh)
}

func toPeerID(s string) peer.ID {
	pid, err := peer.Decode(s)
	if err != nil {
		return peer.ID(s)
	}
	return pid
}

func (h *Hub) push(msg web.WsMessage) {
	select {
	case h.wsCh <- msg:
	default:
		slog.Warn("ws channel full, dropping message", "action", msg.Action)
	}
}

func (h *Hub) pushRefresh() {
	h.push(web.WsMessage{Action: web.WsRefresh, Data: "refresh"})
}

func (h *Hub) onPeerConnectedEvent(e transport.PeerConnectedEvent) {
	err := h.SaveFriend(e.PeerID)
	if err != nil {
		slog.Error("save friend", "peer", e.PeerID, "err", err)
		return
	}
	h.pushRefresh()
	h.push(web.WsMessage{Action: web.WsPeerStatus, Data: map[string]any{
		"peerId": e.PeerID.String(),
		"online": true,
	}})
}

func (h *Hub) onPeerDisconnectedEvent(e transport.PeerDisconnectedEvent) {
	slog.Debug("peer disconnected", "peer", e.PeerID)
	h.pushRefresh()
	h.push(web.WsMessage{Action: web.WsPeerStatus, Data: map[string]any{
		"peerId": e.PeerID.String(),
		"online": false,
	}})
}

func (h *Hub) onMessageEvent(e transport.MessageEvent) {
	err := h.SaveReceivedMessage(e.From, e.Text, e.Timestamp)
	if err != nil {
		slog.Error("save message", "from", e.From, "err", err)
		return
	}
	count, _ := h.GetUnreadCount(e.From)
	h.push(web.WsMessage{Action: web.WsNewMessage, Data: map[string]any{
		"peerId":    e.From.String(),
		"content":   e.Text,
		"timestamp": e.Timestamp,
		"direction": "received",
	}})
	h.push(web.WsMessage{Action: web.WsUnreadUpdate, Data: map[string]any{
		"peerId": e.From.String(),
		"count":  count,
	}})
}

func (h *Hub) onFileMetaEvent(e transport.FileMetaEvent) {
	msg := protocol.MessageFileMeta{
		TransferID: e.TransferID,
		Name:       e.Name,
		Size:       e.Size,
		Timestamp:  e.Timestamp,
	}
	err := h.SaveFileMeta(e.From, msg)
	if err != nil {
		slog.Error("save file meta", "from", e.From, "err", err)
		return
	}
	count, _ := h.GetUnreadCount(e.From)
	h.push(web.WsMessage{Action: web.WsUnreadUpdate, Data: map[string]any{
		"peerId": e.From.String(),
		"count":  count,
	}})
	h.push(web.WsMessage{Action: web.WsNewMessage, Data: map[string]any{
		"peerId":    e.From.String(),
		"type":      int(protocol.TypeFileMeta),
		"content":   e.TransferID,
		"timestamp": e.Timestamp,
		"direction": "received",
		"fileName":  e.Name,
		"fileSize":  e.Size,
		"transId":   e.TransferID,
		"status":    int(domain.TransferPending),
	}})
	h.pushRefresh()
}

func (h *Hub) onFileAcceptedEvent(e transport.FileAcceptedEvent) {
	transfer, err := h.db.GetTransfer(e.TransferID)
	if err != nil {
		slog.Error("get transfer", "id", e.TransferID, "err", err)
		return
	}
	if toPeerID(transfer.To) != e.From {
		slog.Warn("file accepted from wrong peer", "transfer", e.TransferID, "from", e.From, "expected", transfer.To)
		return
	}

	err = h.SetTransferAccepted(e.TransferID)
	if err != nil {
		slog.Error("set transfer accepted", "transferId", e.TransferID, "err", err)
		return
	}

	cachePath := filepath.Join(h.fileDir, e.TransferID)
	h.push(web.WsMessage{Action: web.WsFileStatus, Data: map[string]any{
		"type":       "file_accepted",
		"transferId": e.TransferID,
	}})
	h.pushRefresh()

	go func() {
		h.push(web.WsMessage{Action: web.WsFileStatus, Data: map[string]any{
			"type":       "file_progress",
			"transferId": e.TransferID,
			"progress":   50,
		}})
		if err := h.node.SendFile(h.ctx, e.From, cachePath, e.TransferID, transfer.FileName); err != nil {
			slog.Error("send file data", "transferId", e.TransferID, "err", err)
			h.SetTransferFailed(e.TransferID)
			h.push(web.WsMessage{Action: web.WsFileStatus, Data: map[string]any{
				"type":       "file_failed",
				"transferId": e.TransferID,
			}})
			h.pushRefresh()
			return
		}
		h.SetTransferSuccess(e.TransferID)
		h.push(web.WsMessage{Action: web.WsFileStatus, Data: map[string]any{
			"type":       "file_received",
			"transferId": e.TransferID,
			"progress":   100,
		}})
		h.pushRefresh()
	}()
}

func (h *Hub) onFileRejectedEvent(e transport.FileRejectedEvent) {
	transfer, err := h.db.GetTransfer(e.TransferID)
	if err != nil {
		slog.Error("get transfer", "id", e.TransferID, "err", err)
		return
	}
	if toPeerID(transfer.To) != e.From {
		slog.Warn("file rejected from wrong peer", "transfer", e.TransferID, "from", e.From, "expected", transfer.To)
		return
	}

	err = h.SetTransferRejected(e.TransferID)
	if err != nil {
		slog.Error("set transfer rejected", "transferId", e.TransferID, "err", err)
		return
	}
	h.push(web.WsMessage{Action: web.WsFileStatus, Data: map[string]any{
		"type":       "file_rejected",
		"transferId": e.TransferID,
	}})
	h.pushRefresh()
}

func (h *Hub) onFileTimeoutEvent(e transport.FileTimeoutEvent) {
	err := h.SetTransferFailed(e.TransferID)
	if err != nil {
		slog.Error("set transfer failed", "transferId", e.TransferID, "err", err)
		return
	}
	h.push(web.WsMessage{Action: web.WsFileStatus, Data: map[string]any{
		"type":       "file_failed",
		"transferId": e.TransferID,
	}})
	h.pushRefresh()
}

func (h *Hub) onFileReceivedEvent(e transport.FileReceivedEvent) {
	err := h.SetTransferSuccess(e.TransferID)
	if err != nil {
		slog.Error("set transfer success", "transferId", e.TransferID, "err", err)
		return
	}
	h.push(web.WsMessage{Action: web.WsFileStatus, Data: map[string]any{
		"type":       "file_received",
		"transferId": e.TransferID,
		"savePath":   e.SavePath,
	}})
	h.pushRefresh()
}
