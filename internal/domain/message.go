package domain

const (
	MessageUnread int = iota
	MessageRead
)

type Message struct {
	ID        int64  `db:"id"`
	From      string `db:"from"`
	To        string `db:"to"`
	Read      int    `db:"read"`
	Type      int    `db:"type"`
	Content   string `db:"content"`
	Timestamp int64  `db:"timestamp"`
}
