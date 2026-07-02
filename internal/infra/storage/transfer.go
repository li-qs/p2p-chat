package storage

import (
	"p2pchat/internal/domain"
)

const transferSchema = `
	CREATE TABLE IF NOT EXISTS file_transfer (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		"from" TEXT NOT NULL,
		"to" TEXT NOT NULL,
		status INTEGER NOT NULL,
		start_time INTEGER NOT NULL,
		end_time INTEGER NOT NULL,
		file_path TEXT NOT NULL,
		trans_id TEXT NOT NULL,
		file_name TEXT NOT NULL,
		size INTEGER NOT NULL,
		ts INTEGER NOT NULL
    );
`

func (db *SQLite) SaveTransfer(transfer *domain.FileTransfer) error {
	_, err := db.NamedExec(`
		INSERT INTO file_transfer (
			"from",
			"to",
			status,
			start_time,
			end_time,
			file_path,
			trans_id,
			file_name,
			size,
			ts
		)
		VALUES (
			:from,
			:to,
			:status,
			:start_time,
			:end_time,
			:file_path,
			:trans_id,
			:file_name,
			:size,
			:ts
		)
	`, transfer)
	return err
}

func (db *SQLite) GetTransfer(transferID string) (*domain.FileTransfer, error) {
	var transfer domain.FileTransfer
	err := db.Get(&transfer, `
		SELECT *
		FROM file_transfer
		WHERE trans_id=?
		
	`, transferID)
	return &transfer, err
}

func (db *SQLite) UpdateTransferStatus(transferID string, status domain.TransferStatus) error {
	_, err := db.Exec(`
		UPDATE file_transfer
		SET status=?
		WHERE trans_id=?
		
	`, status, transferID)
	return err
}


func (db *SQLite) UpdateTransferStartTime(transferID string, startTime int64) error {
	_, err := db.Exec(`
		UPDATE file_transfer
		SET start_time=?
		WHERE trans_id=?
		
	`, startTime, transferID)
	return err
}

func (db *SQLite) UpdateTransferEndTime(transferID string, endTime int64) error {
	_, err := db.Exec(`
		UPDATE file_transfer
		SET end_time=?
		WHERE trans_id=?
		
	`, endTime, transferID)
	return err
}

func (db *SQLite) UpdateTransferFilePath(transferID string, filePath string) error {
	_, err := db.Exec(`
		UPDATE file_transfer
		SET file_path=?
		WHERE trans_id=?
		
	`, filePath, transferID)
	return err
}
