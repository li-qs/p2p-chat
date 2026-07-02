package domain

type Friend struct {
	PeerID   string `db:"peer_id"`
	LastSeen int64  `db:"last_seen"`
}
