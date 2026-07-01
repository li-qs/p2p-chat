package protocol

import (
	"bufio"
	"errors"
	"io"
	"os"

	"github.com/vmihailenco/msgpack/v5"
)

const fileChunkSize = 64 * 1024

type FileIntoPacket struct {
	TransferID string `msgpack:"trans_id"`
}

func WriteFileInfo(w *bufio.Writer, p *FileIntoPacket) error {
	data, err := msgpack.Marshal(p)
	if err != nil {
		return err
	}
	if err := writeData(w, data); err != nil {
		return err
	}
	return nil
}

func ReadFileInfo(r *bufio.Reader) (*FileIntoPacket, error) {
	_, data, err := readData(r)
	if err != nil {
		return nil, err
	}
	var p FileIntoPacket
	if err := msgpack.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

func WriteFileChunks(w *bufio.Writer, file *os.File, onChunkWrited func()) error {
	buf := make([]byte, fileChunkSize)
	for {
		n, err := file.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		if err := writeData(w, buf[:n]); err != nil {
			return err
		}

		onChunkWrited()
	}
}

func ReadFileChunks(r *bufio.Reader, file *os.File, onChunkRead func()) error {
	for {
		_, chunk, err := readData(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		if _, err := file.Write(chunk); err != nil {
			return err
		}

		onChunkRead()
	}
}
