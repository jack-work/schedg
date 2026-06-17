package source

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "modernc.org/sqlite"
)

func init() { Register("sqlite", newSQLite) }

func newSQLite(cfg Config) (Source, error) {
	if cfg.Path == "" {
		return nil, fmt.Errorf("sqlite: empty path")
	}
	db, err := sql.Open("sqlite", cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("sqlite: %w", err)
	}
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, err
	}
	if _, err := db.Exec("PRAGMA foreign_keys=ON"); err != nil {
		db.Close()
		return nil, err
	}
	return newSQLSource(db, cfg.Schema, sqliteDialect{}), nil
}

// --- SQLite dialect ---

type sqliteDialect struct{}

func (sqliteDialect) name() string { return "sqlite" }

func (sqliteDialect) hasTable(ctx context.Context, db *sql.DB, table string) (bool, error) {
	var x int
	err := db.QueryRowContext(ctx,
		"SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&x)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, err
}

func (sqliteDialect) hasColumn(ctx context.Context, db *sql.DB, table, col string) (bool, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, typ string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			return false, err
		}
		if name == col {
			return true, nil
		}
	}
	return false, rows.Err()
}

func (sqliteDialect) upsertSQL(table string, keyCols, valCols []string) string {
	allCols := append(append([]string{}, keyCols...), valCols...)
	ph := make([]string, len(allCols))
	for i := range ph {
		ph[i] = "?"
	}
	return fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
		table, strings.Join(allCols, ", "), strings.Join(ph, ", "))
}
