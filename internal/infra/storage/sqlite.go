package storage

import (
	"os"
	"path/filepath"

	"github.com/jmoiron/sqlx"
	_ "modernc.org/sqlite"
)

type SQLite struct {
	*sqlx.DB
}

func NewSQLite(dbPath string) (*SQLite, error) {
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	db, err := sqlx.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}

	sqlite := &SQLite{db}

	if err := sqlite.migrate(); err != nil {
		return nil, err
	}

	return sqlite, nil
}

func (db *SQLite) migrate() error {
	if _, err := db.Exec(friendSchema); err != nil {
		return err
	}
	if _, err := db.Exec(messageSchema); err != nil {
		return err
	}
	if _, err := db.Exec(transferSchema); err != nil {
		return err
	}
	return nil
}

func (db *SQLite) NamedSelect(dest any, query string, arg any) error {
	q, args, err := sqlx.Named(query, arg)
	if err != nil {
		return err
	}
	q = db.Rebind(q)
	return db.Select(dest, q, args...)
}

func (db *SQLite) NamedGet(dest any, query string, arg any) error {
	q, args, err := sqlx.Named(query, arg)
	if err != nil {
		return err
	}
	q = db.Rebind(q)
	return db.Get(dest, q, args...)
}
