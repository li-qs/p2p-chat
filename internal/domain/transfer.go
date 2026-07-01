package domain

const (
	TransferPending   TransferStatus = iota + 1 // 等待对方确认
	TransferAccepted                            // 已同意，准备传输
	TransferRejected                            // 已拒绝
	TransferProgress                            // 传输中
	TransferSuccess                             // 传输完成
	TransferFailed                              // 传输失败
	TransferCancelled                           // 用户取消
)

type TransferStatus int

type FileTransfer struct {
	ID         int64          `db:"id"`
	From       string         `db:"from"`
	To         string         `db:"to"`
	Status     TransferStatus `db:"status"`
	Progress   int64          `db:"progress"`
	StartTime  int64          `db:"start_time"`
	EndTime    int64          `db:"end_time"`
	FilePath   string         `db:"file_path"`
	TransferID string         `db:"trans_id"`
	FileName   string         `db:"file_name"`
	Size       int64          `db:"size"`
	HashAlgo   string         `db:"hash_algo"`
	Hash       string         `db:"hash"`
	Timestamp  int64          `db:"ts"`
}
