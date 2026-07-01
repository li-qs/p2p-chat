package domain

const (
	TransferPending  TransferStatus = iota + 1
	TransferAccepted
	TransferRejected
	TransferProgress
	TransferSuccess
	TransferFailed
)

type TransferStatus int

type FileTransfer struct {
	ID         int64          `db:"id"`
	From       string         `db:"from"`
	To         string         `db:"to"`
	Status     TransferStatus `db:"status"`
	StartTime  int64          `db:"start_time"`
	EndTime    int64          `db:"end_time"`
	FilePath   string         `db:"file_path"`
	TransferID string         `db:"trans_id"`
	FileName   string         `db:"file_name"`
	Size       int64          `db:"size"`
	Timestamp  int64          `db:"ts"`
}
