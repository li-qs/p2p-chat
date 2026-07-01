package hub

import (
	"context"
	"log"
	"net"
	"net/http"
	"p2pchat/internal/config"
	"p2pchat/internal/hub/web"
	"p2pchat/internal/infra/storage"
	"p2pchat/internal/infra/transport"
	"p2pchat/internal/infra/transport/protocol"
)

type Hub struct {
	ctx     context.Context
	eventCh chan transport.Event
	wsCh    chan web.WsMessage
	db      *storage.SQLite
	node    *transport.Node
	mux     http.Handler
}

func New(ctx context.Context) (*Hub, error) {
	cfg, err := config.Load("./config.yaml")
	if err != nil {
		return nil, err
	}

	eventCh := make(chan transport.Event, 128)
	wsCh := make(chan web.WsMessage, 64)

	db, err := storage.NewSQLite(cfg.DBPath)
	if err != nil {
		return nil, err
	}

	node, err := transport.NewNode(ctx, cfg.Multiaddrs, eventCh, cfg.FileDir)
	if err != nil {
		return nil, err
	}

	h := &Hub{
		ctx:     ctx,
		eventCh: eventCh,
		wsCh:    wsCh,
		db:      db,
		node:    node,
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
				log.Printf("unknown event type: %T", e)
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

func (h *Hub) onPeerConnectedEvent(e transport.PeerConnectedEvent) {
	err := h.SaveFriend(e.PeerID)
	if err != nil {
		log.Println(err)
		return
	}
}

func (h *Hub) onPeerDisconnectedEvent(e transport.PeerDisconnectedEvent) {
	log.Println("peer disconnected: ", e.PeerID)
}

func (h *Hub) onMessageEvent(e transport.MessageEvent) {
	err := h.SaveReceivedMessage(e.From, e.Text, e.Timestamp)
	if err != nil {
		log.Println(err)
		return
	}
}

func (h *Hub) onFileMetaEvent(e transport.FileMetaEvent) {
	msg := protocol.MessageFileMeta{
		TransferID: e.TransferID,
		Name:       e.Name,
		Size:       e.Size,
		HashAlgo:   e.HashAlgo,
		Hash:       e.Hash,
		Timestamp:  e.Timestamp,
	}
	err := h.SaveFileMeta(e.From, msg)
	if err != nil {
		log.Println(err)
		return
	}
}

func (h *Hub) onFileAcceptedEvent(e transport.FileAcceptedEvent) {
	err := h.SetTransferAccepted(e.TransferID)
	if err != nil {
		log.Println(err)
		return
	}
}

func (h *Hub) onFileRejectedEvent(e transport.FileRejectedEvent) {
	err := h.SetTransferRejected(e.TransferID)
	if err != nil {
		log.Println(err)
		return
	}
}

func (h *Hub) onFileTimeoutEvent(e transport.FileTimeoutEvent) {
	err := h.SetTransferFailed(e.TransferID)
	if err != nil {
		log.Println(err)
		return
	}
}

func (h *Hub) onFileReceivedEvent(e transport.FileReceivedEvent) {
	err := h.SetTransferSuccess(e.TransferID)
	if err != nil {
		log.Println(err)
		return
	}
}
