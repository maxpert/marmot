package protocol

import (
	"regexp"
	"strings"
)

// DDL rewriting patterns (simplified without negative lookahead)
var (
	// Prefix patterns are intentionally identifier-agnostic (quoted names,
	// schema-qualified names, etc.) and only match the leading DDL verb.
	createTablePrefixRe = regexp.MustCompile(`(?i)^\s*CREATE\s+(?:TEMP(?:ORARY)?\s+)?TABLE\s+`)
	dropTablePrefixRe   = regexp.MustCompile(`(?i)^\s*DROP\s+TABLE\s+`)
	createDBPrefixRe    = regexp.MustCompile(`(?i)^\s*CREATE\s+DATABASE\s+`)
	dropDBPrefixRe      = regexp.MustCompile(`(?i)^\s*DROP\s+DATABASE\s+`)
	createUniqueIdxRe   = regexp.MustCompile(`(?i)^\s*CREATE\s+UNIQUE\s+INDEX\s+`)
	createIdxPrefixRe   = regexp.MustCompile(`(?i)^\s*CREATE\s+INDEX\s+`)
	dropIdxPrefixRe     = regexp.MustCompile(`(?i)^\s*DROP\s+INDEX\s+`)

	// TRUNCATE TABLE patterns (SQLite doesn't support TRUNCATE, convert to DELETE)
	truncateTableRe = regexp.MustCompile(`(?i)^\s*TRUNCATE\s+TABLE\s+(\w+)`)
	truncateRe      = regexp.MustCompile(`(?i)^\s*TRUNCATE\s+(\w+)`)
)

func insertClauseAfterPrefix(sql string, prefixRe *regexp.Regexp, clause string) (string, bool) {
	match := prefixRe.FindStringIndex(sql)
	if len(match) != 2 || match[0] != 0 {
		return sql, false
	}
	return sql[:match[1]] + clause + sql[match[1]:], true
}

// RewriteDDLForIdempotency rewrites a DDL statement to be idempotent
// This allows DDL to be safely replayed after node failures or partitions
//
// Transformations:
// - CREATE TABLE foo → CREATE TABLE IF NOT EXISTS foo
// - DROP TABLE foo → DROP TABLE IF EXISTS foo
// - CREATE INDEX idx → CREATE INDEX IF NOT EXISTS idx
// - DROP INDEX idx → DROP INDEX IF EXISTS idx
//
// Note: SQLite has limited support for ALTER TABLE IF NOT EXISTS,
// so ALTER TABLE statements are NOT rewritten and may fail on replay.
// This is acceptable as ALTER TABLE is less common in production.
func RewriteDDLForIdempotency(sql string) string {
	trimmed := strings.TrimSpace(sql)
	upperSQL := strings.ToUpper(trimmed)

	// CREATE TABLE
	if strings.HasPrefix(upperSQL, "CREATE TABLE") ||
		strings.HasPrefix(upperSQL, "CREATE TEMP TABLE") ||
		strings.HasPrefix(upperSQL, "CREATE TEMPORARY TABLE") {
		// Check if already has IF NOT EXISTS
		if strings.Contains(upperSQL, "IF NOT EXISTS") {
			return trimmed
		}
		if rewritten, ok := insertClauseAfterPrefix(trimmed, createTablePrefixRe, "IF NOT EXISTS "); ok {
			return rewritten
		}
		return trimmed
	}

	// DROP TABLE
	if strings.HasPrefix(upperSQL, "DROP TABLE") {
		// Check if already has IF EXISTS
		if strings.Contains(upperSQL, "IF EXISTS") {
			return trimmed
		}
		if rewritten, ok := insertClauseAfterPrefix(trimmed, dropTablePrefixRe, "IF EXISTS "); ok {
			return rewritten
		}
		return trimmed
	}

	// CREATE DATABASE
	if strings.HasPrefix(upperSQL, "CREATE DATABASE") {
		// Check if already has IF NOT EXISTS
		if strings.Contains(upperSQL, "IF NOT EXISTS") {
			return trimmed
		}
		if rewritten, ok := insertClauseAfterPrefix(trimmed, createDBPrefixRe, "IF NOT EXISTS "); ok {
			return rewritten
		}
		return trimmed
	}

	// DROP DATABASE
	if strings.HasPrefix(upperSQL, "DROP DATABASE") {
		// Check if already has IF EXISTS
		if strings.Contains(upperSQL, "IF EXISTS") {
			return trimmed
		}
		if rewritten, ok := insertClauseAfterPrefix(trimmed, dropDBPrefixRe, "IF EXISTS "); ok {
			return rewritten
		}
		return trimmed
	}

	// CREATE INDEX (including UNIQUE INDEX)
	if strings.HasPrefix(upperSQL, "CREATE") && strings.Contains(upperSQL, "INDEX") {
		// Check if already has IF NOT EXISTS
		if strings.Contains(upperSQL, "IF NOT EXISTS") {
			return trimmed
		}
		if rewritten, ok := insertClauseAfterPrefix(trimmed, createUniqueIdxRe, "IF NOT EXISTS "); ok {
			return rewritten
		}
		if rewritten, ok := insertClauseAfterPrefix(trimmed, createIdxPrefixRe, "IF NOT EXISTS "); ok {
			return rewritten
		}
		return trimmed
	}

	// DROP INDEX
	if strings.HasPrefix(upperSQL, "DROP INDEX") {
		// Check if already has IF EXISTS
		if strings.Contains(upperSQL, "IF EXISTS") {
			return trimmed
		}
		if rewritten, ok := insertClauseAfterPrefix(trimmed, dropIdxPrefixRe, "IF EXISTS "); ok {
			return rewritten
		}
		return trimmed
	}

	// TRUNCATE TABLE - SQLite doesn't support TRUNCATE, convert to DELETE FROM
	if strings.HasPrefix(upperSQL, "TRUNCATE") {
		// TRUNCATE TABLE tablename → DELETE FROM tablename
		if match := truncateTableRe.FindStringSubmatch(trimmed); len(match) >= 2 {
			tableName := match[1]
			return "DELETE FROM " + tableName
		}
		// TRUNCATE tablename (without TABLE keyword) → DELETE FROM tablename
		if match := truncateRe.FindStringSubmatch(trimmed); len(match) >= 2 {
			tableName := match[1]
			return "DELETE FROM " + tableName
		}
	}

	// ALTER TABLE and other DDL statements: return as-is
	// SQLite does not support IF NOT EXISTS for ALTER TABLE
	// These will fail on replay, but ALTER TABLE is less common
	return trimmed
}

// IsDDLIdempotent checks if a DDL statement is already idempotent
func IsDDLIdempotent(sql string) bool {
	upperSQL := strings.ToUpper(strings.TrimSpace(sql))

	// Check for IF NOT EXISTS
	if strings.Contains(upperSQL, "IF NOT EXISTS") {
		return true
	}

	// Check for IF EXISTS
	if strings.Contains(upperSQL, "IF EXISTS") {
		return true
	}

	// ALTER TABLE statements are never guaranteed idempotent in SQLite
	if strings.HasPrefix(upperSQL, "ALTER TABLE") {
		return false
	}

	return false
}
