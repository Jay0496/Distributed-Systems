package storage

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
)

// InitDB initializes the local SQLite storage daemon for a node container
func InitDB(filepath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", filepath)
	if err != nil {
		return nil, err
	}
	
	query := `CREATE TABLE IF NOT EXISTS store (key TEXT PRIMARY KEY, value BLOB, is_parity BOOLEAN);`
	_, err = db.Exec(query)
	return db, err
}