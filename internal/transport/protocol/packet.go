package protocol

import (
	"bufio"
	"time"

	"github.com/google/uuid"
	"github.com/vmihailenco/msgpack/v5"
)

const maxPacketSize = 1024 * 1024

type Packet struct {
	ID          string      `msgpack:"id"` // message ID
	MessageType MessageType `msgpack:"tp"`
	Timestamp   int64       `msgpack:"ts"`
	Message     []byte      `msgpack:"msg"`
}

func Marshal(m Message) (*Packet, error) {
	raw, err := msgpack.Marshal(m)
	if err != nil {
		return nil, err
	}

	return &Packet{
		ID:          uuid.NewString(),
		MessageType: m.MessageType(),
		Timestamp:   time.Now().UnixMilli(),
		Message:     raw,
	}, nil
}

func Unmarshal[T any](p *Packet) (*T, error) {
	var m T
	if err := msgpack.Unmarshal(p.Message, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func Write(w *bufio.Writer, p *Packet) error {
	data, err := msgpack.Marshal(p)
	if err != nil {
		return err
	}

	return writeData(w, data)
}

func Read(r *bufio.Reader) (*Packet, error) {
	_, data, err := readData(r)
	if err != nil {
		return nil, err
	}

	var p Packet
	if err := msgpack.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	return &p, nil
}
