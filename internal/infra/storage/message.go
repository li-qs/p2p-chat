package storage

import (
	"database/sql"
	"errors"
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
	var filter string
	if lastID > 0 {
		filter = "AND id < :lastID"
	}
	err := db.NamedSelect(&messages, `
		SELECT *
		FROM message
		WHERE ("from"=:from OR "to"=:to)
			`+filter+`
		ORDER BY id ASC
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
		WHERE "from"=? AND read=0
	`, peerID)
	return count, err
}

func (db *SQLite) CountUnreadMessagesByPeers(peerIDs []string) (map[string]int, error) {
	if len(peerIDs) == 0 {
		return map[string]int{}, nil
	}
	type row struct {
		From  string `db:"from"`
		Count int    `db:"c"`
	}
	query, args, err := sqlx.In(`
		SELECT "from", COUNT(*) AS c
		FROM message
		WHERE "from" IN (?) AND read=0
		GROUP BY "from"
	`, peerIDs)
	if err != nil {
		return nil, err
	}
	var rows []row
	if err := db.Select(&rows, query, args...); err != nil {
		return nil, err
	}
	result := make(map[string]int, len(rows))
	for _, r := range rows {
		result[r.From] = r.Count
	}
	return result, nil
}

func (db *SQLite) MarkReadMessages(peerID string) error {
	_, err := db.Exec(`
		UPDATE message
		SET read=1
		WHERE "from"=?
	`, peerID)
	return err
}


func (db *SQLite) GetLastMessage(peerID string) (*domain.Message, error) {
	var msg domain.Message
	err := db.Get(&msg, `
		SELECT * FROM message
		WHERE "from"=? OR "to"=?
		ORDER BY id DESC
		LIMIT 1
	`, peerID, peerID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &msg, nil
}
