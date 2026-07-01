package web

import (
	"context"
	"embed"
	"encoding/json"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"

	"p2pchat/internal/domain"

	"github.com/gorilla/websocket"
	"github.com/libp2p/go-libp2p/core/peer"
)

type Service interface {
	MyPeerID() string
	GetFriends() ([]domain.Friend, error)
	GetOnlineFriends() ([]domain.Friend, error)
	GetHistory(peerID peer.ID, lastID int64) ([]domain.Message, error)
	SendMessage(ctx context.Context, peerID peer.ID, text string) error
	MarkAsRead(peerID peer.ID) error
	GetUnreadCount(peerID peer.ID) (int, error)
	SendFile(ctx context.Context, peerID peer.ID, filePath string) error
	AcceptTransfer(ctx context.Context, transferID string, filePath string) error
	RejectTransfer(ctx context.Context, transferID string) error
}

type WsMessage struct {
	Action int `json:"action"`
	Data   any `json:"data"`
}

type handler struct {
	service Service
}

//go:embed tmpl/*
var templates embed.FS

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func NewServeMux(ctx context.Context, wsCh chan WsMessage, service Service) (*http.ServeMux, error) {
	h := handler{service: service}

	indexHTML, err := fs.ReadFile(templates, "tmpl/index.html")
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /ui", h.webUI(indexHTML))
	mux.HandleFunc("GET /ws", h.upgradeWS(ctx, wsCh))
	mux.HandleFunc("GET /api/friends", h.getFriends)
	mux.HandleFunc("GET /api/{peerID}/messages", h.getHistoryMessages)
	mux.HandleFunc("GET /api/{peerID}/unread", h.getUnreadCount)
	mux.HandleFunc("POST /api/{peerID}/message", h.sendMessage)
	mux.HandleFunc("POST /api/{peerID}/read", h.markAsRead)
	mux.HandleFunc("POST /api/{peerID}/file", h.sendFile)
	mux.HandleFunc("POST /api/{peerID}/accept-file", h.acceptFile)
	mux.HandleFunc("POST /api/{peerID}/reject-file", h.rejectFile)

	return mux, nil
}

func (h *handler) webUI(indexHTML []byte) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	}
}

func (h *handler) upgradeWS(ctx context.Context, wsCh chan WsMessage) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := wsUpgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("ws upgrade:", err)
			return
		}
		defer conn.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-wsCh:
				if !ok {
					return
				}
				if err := conn.WriteJSON(msg); err != nil {
					return
				}
			}
		}
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func (h *handler) getFriends(w http.ResponseWriter, r *http.Request) {
	friends, err := h.service.GetFriends()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	online, _ := h.service.GetOnlineFriends()
	onlineSet := make(map[string]bool, len(online))
	for _, f := range online {
		onlineSet[f.PeerID] = true
	}

	type friendInfo struct {
		PeerID   string `json:"peerId"`
		Nickname string `json:"nickname"`
		Online   bool   `json:"online"`
	}

	result := make([]friendInfo, len(friends))
	for i, f := range friends {
		result[i] = friendInfo{
			PeerID:   f.PeerID,
			Nickname: f.Nickname,
			Online:   onlineSet[f.PeerID],
		}
	}

	writeJSON(w, result)
}

func (h *handler) getHistoryMessages(w http.ResponseWriter, r *http.Request) {
	peerID := r.PathValue("peerID")

	lastID := int64(0)
	if v := r.URL.Query().Get("lastID"); v != "" {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil {
			lastID = parsed
		}
	}

	msgs, err := h.service.GetHistory(peer.ID(peerID), lastID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	type msgInfo struct {
		ID        int64  `json:"id"`
		From      string `json:"from"`
		To        string `json:"to"`
		Read      int    `json:"read"`
		Type      int    `json:"type"`
		Content   string `json:"content"`
		Timestamp int64  `json:"timestamp"`
		Direction string `json:"direction"`
	}

	myPeerID := h.service.MyPeerID()
	result := make([]msgInfo, len(msgs))
	for i, m := range msgs {
		dir := "received"
		if m.From == myPeerID {
			dir = "sent"
		}
		result[i] = msgInfo{
			ID:        m.ID,
			From:      m.From,
			To:        m.To,
			Read:      m.Read,
			Type:      m.Type,
			Content:   m.Content,
			Timestamp: m.Timestamp,
			Direction: dir,
		}
	}

	hasMore := len(result) == 100
	writeJSON(w, map[string]any{
		"messages": result,
		"hasMore":  hasMore,
	})
}

type sendMessageReq struct {
	Text string `json:"text"`
}

func (h *handler) sendMessage(w http.ResponseWriter, r *http.Request) {
	peerID := r.PathValue("peerID")

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "read body failed")
		return
	}

	var req sendMessageReq
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if req.Text == "" {
		writeError(w, http.StatusBadRequest, "text is required")
		return
	}

	err = h.service.SendMessage(r.Context(), peer.ID(peerID), req.Text)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, map[string]string{"status": "ok"})
}

func (h *handler) markAsRead(w http.ResponseWriter, r *http.Request) {
	peerID := r.PathValue("peerID")

	if err := h.service.MarkAsRead(peer.ID(peerID)); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, map[string]string{"status": "ok"})
}

func (h *handler) getUnreadCount(w http.ResponseWriter, r *http.Request) {
	peerID := r.PathValue("peerID")

	count, err := h.service.GetUnreadCount(peer.ID(peerID))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, map[string]any{"peerId": peerID, "unread": count})
}

type sendFileReq struct {
	FilePath string `json:"filePath"`
}

func (h *handler) sendFile(w http.ResponseWriter, r *http.Request) {
	peerID := r.PathValue("peerID")

	contentType := r.Header.Get("Content-Type")
	var filePath string

	if contentType == "application/json" || contentType == "" {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, "read body failed")
			return
		}
		var req sendFileReq
		if err := json.Unmarshal(body, &req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid json")
			return
		}
		filePath = req.FilePath
	} else {
		file, header, err := r.FormFile("file")
		if err != nil {
			writeError(w, http.StatusBadRequest, "read file failed: "+err.Error())
			return
		}
		defer file.Close()

		filePath = filepath.Join(os.TempDir(), header.Filename)
		dst, err := os.Create(filePath)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "create file failed")
			return
		}
		defer dst.Close()
		if _, err := io.Copy(dst, file); err != nil {
			writeError(w, http.StatusInternalServerError, "save file failed")
			return
		}
	}

	if filePath == "" {
		writeError(w, http.StatusBadRequest, "filePath is required")
		return
	}

	err := h.service.SendFile(r.Context(), peer.ID(peerID), filePath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, map[string]string{"status": "ok"})
}

type acceptFileReq struct {
	TransferID string `json:"transferID"`
	FilePath   string `json:"filePath"`
}

func (h *handler) acceptFile(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "read body failed")
		return
	}

	var req acceptFileReq
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if req.TransferID == "" {
		writeError(w, http.StatusBadRequest, "transferID is required")
		return
	}

	err = h.service.AcceptTransfer(r.Context(), req.TransferID, req.FilePath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, map[string]string{"status": "ok"})
}

type rejectFileReq struct {
	TransferID string `json:"transferID"`
}

func (h *handler) rejectFile(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "read body failed")
		return
	}

	var req rejectFileReq
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json")
		return
	}
	if req.TransferID == "" {
		writeError(w, http.StatusBadRequest, "transferID is required")
		return
	}

	err = h.service.RejectTransfer(r.Context(), req.TransferID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, map[string]string{"status": "ok"})
}
