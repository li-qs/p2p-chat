package event

import "github.com/libp2p/go-libp2p/core/peer"

type SessionCreatedEvent struct {
	PeerID peer.ID
}

type SessionClosedEvent struct {
	PeerID peer.ID
}

type MessageReceivedEvent struct {
	From      peer.ID
	Timestamp int64
	Text      string
}

type FileMetaReceivedEvent struct {
	From      peer.ID
	Timestamp int64
	FileID    string
	Name      string
	Size      int64
	HashAlgo  string
	Hash      string
}

type FileAcceptReceivedEvent struct {
	FileID string
}

type FileRejectReceivedEvent struct {
	FileID string
}

type FileReceivedEvent struct {
	FileID   string
	FilePath string
}
