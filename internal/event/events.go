package event

import "github.com/libp2p/go-libp2p/core/peer"

// session 创建
type SessionCreatedEvent struct {
	PeerID peer.ID
}

// session 关闭
type SessionClosedEvent struct {
	PeerID peer.ID
}

// 收到消息
type MessageEvent struct {
	From      peer.ID
	Timestamp int64
	Text      string
}

// 收到文件传输请求
type FileMetaEvent struct {
	From       peer.ID
	Timestamp  int64
	TransferID string
	Name       string
	Size       int64
	HashAlgo   string
	Hash       string
}

// 文件传输被接受
type FileAcceptedEvent struct {
	TransferID string
}

// 文件传输被拒绝
type FileRejectedEvent struct {
	TransferID string
}

// 传输请求等待超时
type FileTimeoutEvent struct {
	TransferID string
}

// 收到文件
type FileReceivedEvent struct {
	TransferID string
	SavePath   string
}
