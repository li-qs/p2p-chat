package domain

type Friend struct {
	PeerID   string `db:"peer_id"`
	Nickname string `db:"nickname"`
	LastSeen int64  `db:"last_seen"`
}
