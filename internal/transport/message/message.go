package message

import (
	"bufio"
	"time"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/vmihailenco/msgpack/v5"
)

const maxMessagePacketSize = 1024 * 1024

type MessageType int

const (
	MessageText MessageType = iota
	MessageFileMeta
	MessageFileAccept
	MessageFileReject
)

type Message struct {
	ID        string      `msgpack:"id"` // message ID
	Type      MessageType `msgpack:"type"`
	Timestamp int64       `msgpack:"timestamp"`
	Payload   []byte      `msgpack:"payload"`
}

type TextPayload struct {
	Text string `msgpack:"text"`
}

type FileMetaPayload struct {
	FileID   string `msgpack:"file_id"`
	Name     string `msgpack:"name"`
	Size     int64  `msgpack:"size"`
	HashAlgo string `msgpack:"hash_algo"`
	Hash     string `msgpack:"hash"`
}

type FileAcceptPayload struct {
	FileID string `msgpack:"file_id"`
}

type FileRejectPayload struct {
	FileID string `msgpack:"file_id"`
}

type Payload interface{ MessageType() MessageType }

func (TextPayload) MessageType() MessageType       { return MessageText }
func (FileMetaPayload) MessageType() MessageType   { return MessageFileMeta }
func (FileAcceptPayload) MessageType() MessageType { return MessageFileAccept }
func (FileRejectPayload) MessageType() MessageType { return MessageFileReject }

func NewMessage(from peer.ID, to string, payload Payload) (*Message, error) {
	raw, err := msgpack.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return &Message{
		ID:        uuid.NewString(),
		Type:      payload.MessageType(),
		Timestamp: time.Now().UnixMilli(),
		Payload:   raw,
	}, nil
}

func GetPayload[T any](m *Message) (*T, error) {
	var p T
	if err := msgpack.Unmarshal(m.Payload, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

func WriteMessage(w *bufio.Writer, msg *Message) error {
	data, err := msgpack.Marshal(msg)
	if err != nil {
		return err
	}

	return writeData(w, data, maxMessagePacketSize)
}

func ReadMessage(r *bufio.Reader) (*Message, error) {
	_, data, err := readData(r, maxMessagePacketSize)
	if err != nil {
		return nil, err
	}

	var msg Message
	if err := msgpack.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}
