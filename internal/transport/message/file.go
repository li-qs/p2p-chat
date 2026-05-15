package message

import (
	"bufio"
	"errors"
	"io"
	"os"

	"github.com/vmihailenco/msgpack/v5"
)

const FileChunkSize = 64 * 1024

type FileHeader struct {
	FileID string `msgpack:"file_id"`
}

func WriteFileHeader(w *bufio.Writer, fileHeader *FileHeader) error {
	data, err := msgpack.Marshal(fileHeader)
	if err != nil {
		return err
	}
	if err := writeData(w, data, FileChunkSize); err != nil {
		return err
	}
	return nil
}

func ReadFileHeader(r *bufio.Reader) (*FileHeader, error) {
	_, data, err := readData(r, FileChunkSize)
	if err != nil {
		return nil, err
	}
	var h FileHeader
	if err := msgpack.Unmarshal(data, &h); err != nil {
		return nil, err
	}
	return &h, nil
}

func WriteFileChunk(w *bufio.Writer, chunk []byte) error {
	return writeData(w, chunk, FileChunkSize)
}

func ReadFileChunk(r *bufio.Reader) ([]byte, error) {
	_, chunk, err := readData(r, FileChunkSize)
	return chunk, err
}

func WriteFile(w *bufio.Writer, file *os.File) error {
	buf := make([]byte, FileChunkSize)
	for {
		n, err := file.Read(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		if err := writeData(w, buf[:n], FileChunkSize); err != nil {
			return err
		}
	}
}

func ReadFile(r *bufio.Reader, file *os.File) error {
	for {
		_, chunk, err := readData(r, FileChunkSize)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		if _, err := file.Write(chunk); err != nil {
			return err
		}
	}
}
