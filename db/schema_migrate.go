package db

import (
	"database/sql"
	"fmt"
	"strings"
)

// columnSpec describes a column expected in __marmot_vector_indexes.
type columnSpec struct {
	name     string
	typeDef  string // type fragment used in ADD COLUMN and for type checking
	baseType string // canonical SQLite type for compatibility check (upper-case)
}

// vecIndexColumns is the full, ordered set of columns __marmot_vector_indexes
// must contain. The type-check compares the stored type string (upper-cased)
// against baseType; a mismatch is a hard error (fix P, design §3.5).
var vecIndexColumns = []columnSpec{
	{"index_name", "TEXT PRIMARY KEY", "TEXT"},
	{"table_name", "TEXT NOT NULL", "TEXT"},
	{"column_name", "TEXT NOT NULL", "TEXT"},
	{"database_name", "TEXT NOT NULL", "TEXT"},
	{"metric", "TEXT NOT NULL", "TEXT"},
	{"dim", "INTEGER NOT NULL", "INTEGER"},
	{"nlist", "INTEGER NOT NULL DEFAULT 64", "INTEGER"},
	{"nprobe", "INTEGER NOT NULL DEFAULT 8", "INTEGER"},
	{"max_norm", "REAL NOT NULL DEFAULT 0.0", "REAL"},
	{"status", "TEXT NOT NULL DEFAULT 'building'", "TEXT"},
	{"created_at", "INTEGER NOT NULL", "INTEGER"},
}

const vecIndexMetaCreate = `CREATE TABLE IF NOT EXISTS __marmot_vector_indexes (
	index_name    TEXT PRIMARY KEY,
	table_name    TEXT NOT NULL,
	column_name   TEXT NOT NULL,
	database_name TEXT NOT NULL,
	metric        TEXT NOT NULL,
	dim           INTEGER NOT NULL,
	nlist         INTEGER NOT NULL DEFAULT 64,
	nprobe        INTEGER NOT NULL DEFAULT 8,
	max_norm      REAL NOT NULL DEFAULT 0.0,
	status        TEXT NOT NULL DEFAULT 'building',
	created_at    INTEGER NOT NULL,
	UNIQUE(table_name, column_name)
)`

// MigrateVectorIndexesSchema ensures __marmot_vector_indexes exists and has
// all required columns with compatible types (design §3.5, fix P).
//
// On a fresh database the table is created. On an older schema, missing
// columns are added via ALTER TABLE ADD COLUMN. An existing column whose
// declared type is incompatible with the expected base type causes a
// hard error — manual intervention is required.
func MigrateVectorIndexesSchema(db *sql.DB) error {
	if _, err := db.Exec(vecIndexMetaCreate); err != nil {
		return fmt.Errorf("ensure __marmot_vector_indexes: %w", err)
	}

	existing, err := currentVecIndexColumns(db)
	if err != nil {
		return err
	}

	for _, col := range vecIndexColumns {
		got, present := existing[col.name]
		if !present {
			if _, err := db.Exec(fmt.Sprintf(
				`ALTER TABLE __marmot_vector_indexes ADD COLUMN %s %s`,
				col.name, col.typeDef,
			)); err != nil {
				return fmt.Errorf("schema migration: add column %s: %w", col.name, err)
			}
			continue
		}
		// Type compatibility: compare upper-cased prefix of the stored type.
		// SQLite stores types as the full affinity string (e.g. "TEXT", "INTEGER").
		// We only check the first word to tolerate DEFAULT clauses stored inline.
		gotBase := strings.ToUpper(strings.Fields(got)[0])
		if gotBase != col.baseType {
			return fmt.Errorf(
				"schema migration: column %s has incompatible type %q, expected %s; "+
					"manual schema repair required",
				col.name, got, col.baseType,
			)
		}
	}
	return nil
}

// currentVecIndexColumns returns a map of column name → declared type string
// for __marmot_vector_indexes, queried via PRAGMA table_info.
// Returns an empty map (not an error) if the table does not exist.
func currentVecIndexColumns(db *sql.DB) (map[string]string, error) {
	rows, err := db.Query(`PRAGMA table_info("__marmot_vector_indexes")`)
	if err != nil {
		return nil, fmt.Errorf("PRAGMA table_info __marmot_vector_indexes: %w", err)
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var r tableInfoRow
		if err := rows.Scan(&r.cid, &r.name, &r.colType, &r.notnull, &r.dfltValue, &r.pk); err != nil {
			return nil, fmt.Errorf("scan table_info: %w", err)
		}
		result[r.name] = r.colType
	}
	return result, rows.Err()
}
