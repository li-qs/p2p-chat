package storage

import (
	"p2pchat/internal/domain"
	"time"

	"github.com/jmoiron/sqlx"
)

const friendSchema = `
	CREATE TABLE IF NOT EXISTS friend (
		peer_id TEXT PRIMARY KEY,
		nickname TEXT NOT NULL,
		last_seen INTEGER NOT NULL
    );
`

func (db *SQLite) SaveFriend(friend *domain.Friend) error {
	_, err := db.NamedExec(`
		INSERT INTO friend (peer_id, nickname, last_seen)
		VALUES (:peer_id, :nickname, :last_seen)
	`, friend)
	return err
}

func (db *SQLite) GetFriends() ([]domain.Friend, error) {
	var friends []domain.Friend
	err := db.Select(&friends, `
		SELECT *
		FROM friend
		ORDER BY last_seen DESC
	`)
	return friends, err
}

func (db *SQLite) GetFriendsByPeerIDs(peerIDs []string) ([]domain.Friend, error) {
	query, args, err := sqlx.In(`
		SELECT *
		FROM friend
		WHERE peer_id=?
		ORDER BY last_seen DESC
	`, peerIDs)
	if err != nil {
		return nil, err
	}

	var friends []domain.Friend
	err = db.Select(&friends, query, args...)
	return friends, err
}

func (db *SQLite) UpdateFriendNickname(peerID, nickname string) error {
	_, err := db.Exec(`
		UPDATE friend
		SET nickname=?
		WHERE peer_id=?
	`, nickname, peerID)
	return err
}

func (db *SQLite) UpdateFriendLastseen(peerID string, lastSeen time.Time) error {
	_, err := db.Exec(`
		UPDATE friend
		SET last_seen=?
		WHERE peer_id=?
	`, lastSeen, peerID)
	return err
}

func (db *SQLite) DeleteFriend(peerID string) error {
	_, err := db.Exec(`
		DELETE FROM friend
		WHERE peer_id=?
		LIMIT 1
	`, peerID)
	return err
}
