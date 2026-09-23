package common

import "strings"

// InternalTablePrefix marks every SQLite table Marmot creates for its own
// protocol state rather than for client data - e.g. AutoIncClaimTableName
// below. It is the single Go definition of this literal: both the protocol
// package (which must refuse client SQL naming such a table) and the db
// package (which owns the tables themselves) import it from here, so the two
// can never drift apart.
//
// NOTE: several "name NOT LIKE '__marmot__%'" filters exist elsewhere in this
// codebase (protocol/handlers) as SQLite queries. SQLite's LIKE treats "_" as
// a single-character wildcard, so that pattern matches more than the literal
// prefix - it is not a substitute for this constant, and this constant must
// not be turned into a LIKE pattern either.
const InternalTablePrefix = "__marmot__"

// AutoIncClaimTableName is the hidden per-database table holding each
// narrow AUTO_INCREMENT table's allocation base. db.AutoIncClaimTable aliases
// this constant; see db/autoinc_claim.go for the schema and write path.
const AutoIncClaimTableName = InternalTablePrefix + "autoinc"

// IsInternalTableName reports whether name - compared case-insensitively, as
// MySQL/SQLite identifiers are - names one of Marmot's own internal tables
// rather than a client table. Used to refuse client SQL that targets Marmot's
// internal state as a class, not just the one table that exists today.
func IsInternalTableName(name string) bool {
	return strings.HasPrefix(strings.ToLower(name), InternalTablePrefix)
}
