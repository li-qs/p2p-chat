package storage

import (
	"p2pchat/internal/domain"

	"github.com/jmoiron/sqlx"
)

const messageSchema = `
	CREATE TABLE IF NOT EXISTS message (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		"from" TEXT NOT NULL,
		"to" TEXT NOT NULL,
		read INTEGER NOT NULL DEFAULT 0,
		type INTEGER NOT NULL,
		content TEXT NOT NULL,
		timestamp INTEGER NOT NULL
    );
`

func (db *SQLite) SaveMessage(msg *domain.Message) error {
	res, err := db.NamedExec(`
		INSERT INTO message ("from", "to", read, type, content, timestamp)
		VALUES (:from, :to, :read, :type, :content, :timestamp)
	`, msg)
	if err != nil {
		return err
	}

	id, err := res.LastInsertId()
	if err == nil {
		msg.ID = id
	}
	return nil
}

func (db *SQLite) GetMessages(peerID string, lastID int64, limit int) ([]domain.Message, error) {
	var messages []domain.Message
	err := db.Select(&messages, `
		SELECT *
		FROM message
		WHERE ("from"=:from OR "to"=:to)
			AND id < :lastID
		ORDER BY id DESC
		LIMIT :limit
	`, map[string]any{
		"from":   peerID,
		"to":     peerID,
		"limit":  limit,
		"lastID": lastID,
	})
	return messages, err
}

func (db *SQLite) CountUnreadMessages(peerID string) (int, error) {
	var count int
	err := db.Get(&count, `
		SELECT COUNT(*)
		FROM message
		WHERE "from"=?
	`, peerID)
	return count, err
}

func (db *SQLite) MarkReadMessages(peerID string) error {
	_, err := db.Exec(`
		UPDATE message
		SET read=1
		WHERE "from"=?
	`, peerID)
	return err
}

func (db *SQLite) DeleteMessages(ids []int64) error {
	query, args, err := sqlx.In(`
		DELETE FROM message
		WHERE id IN (?)
		LIMIT ?
	`, ids, len(ids))
	if err != nil {
		return err
	}

	_, err = db.Exec(query, args...)
	return err
}

func (db *SQLite) ClearMessages(peerID string) error {
	_, err := db.Exec(`
		DELETE FROM message
		WHERE "from"=? OR "to"=?
	`, peerID, peerID)
	return err
}
