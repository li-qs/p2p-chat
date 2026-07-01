package hub

import (
	"context"
	"os"
	"p2pchat/internal/domain"
	"p2pchat/internal/infra/transport/protocol"
	"p2pchat/pkg/utils"
	"time"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p/core/peer"
)

func (h *Hub) MyPeerID() string {
	return h.node.PeerID().String()
}

func (h *Hub) SaveFriend(peerID peer.ID) error {
	return h.db.SaveFriend(&domain.Friend{
		PeerID:   peerID.String(),
		LastSeen: time.Now().UnixMilli(),
	})
}

func (h *Hub) SaveReceivedMessage(peerID peer.ID, text string, timestamp int64) error {
	return h.db.SaveMessage(&domain.Message{
		From:      h.node.PeerID().String(),
		To:        peerID.String(),
		Read:      domain.MessageUnread,
		Type:      int(protocol.TypeText),
		Content:   text,
		Timestamp: timestamp,
	})
}

func (h *Hub) SendMessage(ctx context.Context, peerID peer.ID, text string) error {
	msg := protocol.MessageText{Text: text, Timestamp: time.Now().UnixMilli()}

	err := h.db.SaveMessage(&domain.Message{
		From:      h.node.PeerID().String(),
		To:        peerID.String(),
		Read:      domain.MessageRead,
		Type:      int(msg.MessageType()),
		Content:   msg.Text,
		Timestamp: msg.Timestamp,
	})
	if err != nil {
		return err
	}

	return h.node.Send(ctx, peerID, msg)
}

func (h *Hub) GetHistory(peerID peer.ID, lastID int64) ([]domain.Message, error) {
	return h.db.GetMessages(peerID.String(), lastID, 100)
}

func (h *Hub) MarkAsRead(peerID peer.ID) error {
	return h.db.MarkReadMessages(peerID.String())
}

func (h *Hub) GetUnreadCount(peerID peer.ID) (int, error) {
	return h.db.CountUnreadMessages(peerID.String())
}

func (h *Hub) GetFriends() ([]domain.Friend, error) {
	return h.db.GetFriends()
}

func (h *Hub) GetOnlineFriends() ([]domain.Friend, error) {
	activePeerIDs := h.node.ActivePeers()
	peerIDs := []string{}
	for _, p := range activePeerIDs {
		peerIDs = append(peerIDs, p.String())
	}
	friends, err := h.db.GetFriendsByPeerIDs(peerIDs)
	if err != nil {
		return nil, err
	}
	return friends, nil
}

func (h *Hub) SetNickname(peerID peer.ID, nickname string) error {
	return h.db.UpdateFriendNickname(peerID.String(), nickname)
}

func (h *Hub) SendFile(ctx context.Context, peerID peer.ID, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}

	md5, err := utils.FileMD5(file)
	if err != nil {
		return err
	}

	transID := uuid.NewString()

	msg := protocol.MessageFileMeta{
		TransferID: transID,
		Name:       fileInfo.Name(),
		Size:       fileInfo.Size(),
		HashAlgo:   "md5",
		Hash:       md5,
		Timestamp:  time.Now().UnixMilli(),
	}

	err = h.db.SaveTransfer(&domain.FileTransfer{
		From:       h.node.PeerID().String(),
		To:         peerID.String(),
		Status:     domain.TransferPending,
		TransferID: msg.TransferID,
		FileName:   msg.Name,
		Size:       msg.Size,
		HashAlgo:   msg.HashAlgo,
		Hash:       msg.Hash,
		Timestamp:  msg.Timestamp,
	})
	if err != nil {
		return err
	}

	err = h.db.SaveMessage(&domain.Message{
		From:      h.node.PeerID().String(),
		To:        peerID.String(),
		Read:      domain.MessageRead,
		Type:      int(msg.MessageType()),
		Content:   msg.TransferID,
		Timestamp: msg.Timestamp,
	})
	if err != nil {
		return err
	}

	return h.node.SendFile(ctx, peerID, msg, filePath)
}

func (h *Hub) SaveFileMeta(peerID peer.ID, msg protocol.MessageFileMeta) error {
	return h.db.SaveTransfer(&domain.FileTransfer{
		From:       peerID.String(),
		To:         h.node.PeerID().String(),
		Status:     domain.TransferPending,
		TransferID: msg.TransferID,
		FileName:   msg.Name,
		Size:       msg.Size,
		HashAlgo:   msg.HashAlgo,
		Hash:       msg.Hash,
		Timestamp:  msg.Timestamp,
	})
}

func (h *Hub) SetTransferAccepted(transferID string) error {
	err := h.db.UpdateTransferStatus(transferID, domain.TransferAccepted)
	if err != nil {
		return err
	}
	return h.db.UpdateTransferStartTime(transferID, time.Now().UnixMilli())
}

func (h *Hub) SetTransferRejected(transferID string) error {
	return h.db.UpdateTransferStatus(transferID, domain.TransferRejected)
}

func (h *Hub) SetTransferFailed(transferID string) error {
	return h.db.UpdateTransferStatus(transferID, domain.TransferFailed)
}

func (h *Hub) SetTransferSuccess(transferID string) error {
	err := h.db.UpdateTransferStatus(transferID, domain.TransferSuccess)
	if err != nil {
		return err
	}
	return h.db.UpdateTransferEndTime(transferID, time.Now().UnixMilli())
}

func (h *Hub) AcceptTransfer(ctx context.Context, transferID string, filePath string) error {
	transfer, err := h.db.GetTransfer(transferID)
	if err != nil {
		return err
	}

	err = h.db.UpdateTransferStatus(transferID, domain.TransferAccepted)
	if err != nil {
		return err
	}

	err = h.db.UpdateTransferFilePath(transferID, filePath)
	if err != nil {
		return err
	}

	msg := protocol.MessageFileAccept{TransferID: transferID, Timestamp: time.Now().UnixMilli()}
	return h.node.AcceptFile(ctx, peer.ID(transfer.From), msg)
}

func (h *Hub) RejectTransfer(ctx context.Context, transferID string) error {
	transfer, err := h.db.GetTransfer(transferID)
	if err != nil {
		return err
	}

	err = h.db.UpdateTransferStatus(transferID, domain.TransferRejected)
	if err != nil {
		return err
	}

	msg := protocol.MessageFileReject{TransferID: transferID, Timestamp: time.Now().UnixMilli()}
	return h.node.RejectFile(ctx, peer.ID(transfer.From), msg)
}
