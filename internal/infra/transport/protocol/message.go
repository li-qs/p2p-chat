package protocol

type MessageType int

const (
	TypeText MessageType = iota + 1
	TypeFileMeta
	TypeFileAccept
	TypeFileReject
)

type Message interface{ MessageType() MessageType }

func (MessageText) MessageType() MessageType       { return TypeText }
func (MessageFileMeta) MessageType() MessageType   { return TypeFileMeta }
func (MessageFileAccept) MessageType() MessageType { return TypeFileAccept }
func (MessageFileReject) MessageType() MessageType { return TypeFileReject }

type MessageText struct {
	Text      string `msgpack:"text"`
	Timestamp int64  `msgpack:"ts"`
}

type MessageFileMeta struct {
	TransferID string `msgpack:"trans_id"`
	Name       string `msgpack:"name"`
	Size       int64  `msgpack:"size"`
	HashAlgo   string `msgpack:"hash_algo"`
	Hash       string `msgpack:"hash"`
	Timestamp  int64  `msgpack:"ts"`
}

type MessageFileAccept struct {
	TransferID string `msgpack:"trans_id"`
	Timestamp  int64  `msgpack:"ts"`
}

type MessageFileReject struct {
	TransferID string `msgpack:"trans_id"`
	Timestamp  int64  `msgpack:"ts"`
}
