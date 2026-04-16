package db

import (
	"database/sql"
	"fmt"
	"strings"
)

// tableInfoRow mirrors a row from PRAGMA table_info.
type tableInfoRow struct {
	cid       int
	name      string
	colType   string
	notnull   int
	dfltValue sql.NullString
	pk        int
}

// ValidateBaseTableForVectorIndex checks that tableName has exactly one column
// declared as INTEGER PRIMARY KEY, which SQLite aliases to the stable rowid.
// Without this guarantee, members-table rowid references can silently
// invalidate after VACUUM or rowid renumbering (design §6.1, fix R6).
//
// Returns MARMOT-VEC-011 if the requirement is not met.
func ValidateBaseTableForVectorIndex(db *sql.DB, tableName string) error {
	rows, err := db.Query(fmt.Sprintf(`PRAGMA table_info("%s")`, escapeQuote(tableName)))
	if err != nil {
		return fmt.Errorf("PRAGMA table_info(%s): %w", tableName, err)
	}
	defer rows.Close()

	// Collect all PK columns from PRAGMA table_info.
	// pk=0 → not part of PK. pk>=1 → position in PK (1-based).
	// SQLite rowid alias requires: exactly ONE PK column with type "INTEGER".
	type pkCol struct {
		name    string
		colType string
	}
	var pkCols []pkCol
	var rowCount int

	for rows.Next() {
		var r tableInfoRow
		if err := rows.Scan(&r.cid, &r.name, &r.colType, &r.notnull, &r.dfltValue, &r.pk); err != nil {
			return fmt.Errorf("scan table_info row: %w", err)
		}
		rowCount++
		if r.pk > 0 {
			pkCols = append(pkCols, pkCol{name: r.name, colType: r.colType})
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("table_info iteration: %w", err)
	}
	if rowCount == 0 {
		return fmt.Errorf("table %q does not exist", tableName)
	}

	// Exactly one PK column, and its affinity must be INTEGER (not INT, BIGINT, etc.).
	// SQLite's rowid alias rule: only "INTEGER" (case-insensitive, exact word) qualifies.
	if len(pkCols) == 1 && strings.EqualFold(pkCols[0].colType, "INTEGER") {
		return nil
	}

	return fmt.Errorf(
		"MARMOT-VEC-011: table %q must declare a single INTEGER PRIMARY KEY column "+
			"for stable rowid references; add `id INTEGER PRIMARY KEY` or equivalent",
		tableName,
	)
}

// escapeQuote escapes double-quote characters in a SQL identifier by doubling
// them — the standard SQL identifier escaping strategy.
func escapeQuote(s string) string {
	return strings.ReplaceAll(s, `"`, `""`)
}
