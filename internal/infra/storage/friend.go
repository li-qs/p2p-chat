package storage

import (
	"p2pchat/internal/domain"

	"github.com/jmoiron/sqlx"
)

const friendSchema = `
	CREATE TABLE IF NOT EXISTS friend (
		peer_id TEXT PRIMARY KEY,
		last_seen INTEGER NOT NULL
    );
`

func (db *SQLite) SaveFriend(friend *domain.Friend) error {
	_, err := db.NamedExec(`
		INSERT INTO friend (peer_id, last_seen)
		VALUES (:peer_id, :last_seen)
		ON CONFLICT(peer_id) DO UPDATE SET last_seen=excluded.last_seen
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
	if len(peerIDs) == 0 {
		return nil, nil
	}
	query, args, err := sqlx.In(`
		SELECT *
		FROM friend
		WHERE peer_id IN (?)
		ORDER BY last_seen DESC
	`, peerIDs)
	if err != nil {
		return nil, err
	}

	var friends []domain.Friend
	err = db.Select(&friends, query, args...)
	return friends, err
}
