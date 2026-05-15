package message

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

func writeData(w *bufio.Writer, data []byte, maxSize uint32) error {
	size := uint32(len(data))
	if size > maxSize {
		return fmt.Errorf("packet too large: %d", size)
	}
	if err := binary.Write(w, binary.BigEndian, uint32(size)); err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	return w.Flush()
}

func readData(r *bufio.Reader, maxSize uint32) (uint32, []byte, error) {
	var size uint32
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return 0, nil, err
	}
	if size > maxSize {
		return 0, nil, fmt.Errorf("packet too large: %d", size)
	}
	b := make([]byte, size)
	if _, err := io.ReadFull(r, b); err != nil {
		return 0, nil, err
	}
	return size, b, nil
}
