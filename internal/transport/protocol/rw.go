package protocol

import (
	"bufio"
	"encoding/binary"
	"io"
)

func writeData(w *bufio.Writer, data []byte) error {
	if err := binary.Write(w, binary.BigEndian, uint32(len(data))); err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	return w.Flush()
}

func readData(r *bufio.Reader) (uint32, []byte, error) {
	var size uint32
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return 0, nil, err
	}
	b := make([]byte, size)
	if _, err := io.ReadFull(r, b); err != nil {
		return 0, nil, err
	}
	return size, b, nil
}
