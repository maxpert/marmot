package protocol

import (
	"regexp"
	"strings"
)

// DDL rewriting patterns (simplified without negative lookahead)
var (
	// CREATE TABLE patterns
	createTableRe = regexp.MustCompile(`(?i)^\s*CREATE\s+TABLE\s+(\w+)`)

	// DROP TABLE patterns
	dropTableRe = regexp.MustCompile(`(?i)^\s*DROP\s+TABLE\s+(\w+)`)

	// CREATE DATABASE patterns
	createDatabaseRe = regexp.MustCompile(`(?i)^\s*CREATE\s+DATABASE\s+(\w+)`)

	// DROP DATABASE patterns
	dropDatabaseRe = regexp.MustCompile(`(?i)^\s*DROP\s+DATABASE\s+(\w+)`)

	// CREATE INDEX patterns
	createIndexRe = regexp.MustCompile(`(?i)^\s*CREATE\s+(UNIQUE\s+)?INDEX\s+(\w+)`)

	// DROP INDEX patterns
	dropIndexRe = regexp.MustCompile(`(?i)^\s*DROP\s+INDEX\s+(\w+)`)

	// TRUNCATE TABLE patterns (SQLite doesn't support TRUNCATE, convert to DELETE)
	truncateTableRe = regexp.MustCompile(`(?i)^\s*TRUNCATE\s+TABLE\s+(\w+)`)
	truncateRe      = regexp.MustCompile(`(?i)^\s*TRUNCATE\s+(\w+)`)
)

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
	if strings.HasPrefix(upperSQL, "CREATE TABLE") {
		// Check if already has IF NOT EXISTS
		if strings.Contains(upperSQL, "IF NOT EXISTS") {
			return trimmed
		}
		// Insert "IF NOT EXISTS" after CREATE TABLE
		rewritten := createTableRe.ReplaceAllString(trimmed, "CREATE TABLE IF NOT EXISTS $1")
		return rewritten
	}

	// DROP TABLE
	if strings.HasPrefix(upperSQL, "DROP TABLE") {
		// Check if already has IF EXISTS
		if strings.Contains(upperSQL, "IF EXISTS") {
			return trimmed
		}
		// Insert "IF EXISTS" after DROP TABLE
		rewritten := dropTableRe.ReplaceAllString(trimmed, "DROP TABLE IF EXISTS $1")
		return rewritten
	}

	// CREATE DATABASE
	if strings.HasPrefix(upperSQL, "CREATE DATABASE") {
		// Check if already has IF NOT EXISTS
		if strings.Contains(upperSQL, "IF NOT EXISTS") {
			return trimmed
		}
		// Insert "IF NOT EXISTS" after CREATE DATABASE
		rewritten := createDatabaseRe.ReplaceAllString(trimmed, "CREATE DATABASE IF NOT EXISTS $1")
		return rewritten
	}

	// DROP DATABASE
	if strings.HasPrefix(upperSQL, "DROP DATABASE") {
		// Check if already has IF EXISTS
		if strings.Contains(upperSQL, "IF EXISTS") {
			return trimmed
		}
		// Insert "IF EXISTS" after DROP DATABASE
		rewritten := dropDatabaseRe.ReplaceAllString(trimmed, "DROP DATABASE IF EXISTS $1")
		return rewritten
	}

	// CREATE INDEX (including UNIQUE INDEX)
	if strings.HasPrefix(upperSQL, "CREATE") && strings.Contains(upperSQL, "INDEX") {
		// Check if already has IF NOT EXISTS
		if strings.Contains(upperSQL, "IF NOT EXISTS") {
			return trimmed
		}
		// Handle both CREATE INDEX and CREATE UNIQUE INDEX
		if match := createIndexRe.FindStringSubmatch(trimmed); len(match) >= 3 {
			if match[1] != "" {
				// UNIQUE INDEX case
				rewritten := createIndexRe.ReplaceAllString(trimmed, "CREATE UNIQUE INDEX IF NOT EXISTS $2")
				return rewritten
			}
			// Regular INDEX case
			rewritten := createIndexRe.ReplaceAllString(trimmed, "CREATE INDEX IF NOT EXISTS $2")
			return rewritten
		}
	}

	// DROP INDEX
	if strings.HasPrefix(upperSQL, "DROP INDEX") {
		// Check if already has IF EXISTS
		if strings.Contains(upperSQL, "IF EXISTS") {
			return trimmed
		}
		// Insert "IF EXISTS" after DROP INDEX
		rewritten := dropIndexRe.ReplaceAllString(trimmed, "DROP INDEX IF EXISTS $1")
		return rewritten
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
