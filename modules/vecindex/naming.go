package vecindex

import (
	"fmt"
	"strings"
)

// Maximum length of a user-supplied index name. Keeps the derived object
// names (table, index, trigger) well under SQLite's practical identifier
// limits.
const MaxIndexNameLen = 48

// ValidateIndexName rejects names that would produce unsafe or ambiguous
// SQLite identifiers. Rules:
//   - non-empty, length <= MaxIndexNameLen
//   - first character is an ASCII letter
//   - remaining characters are ASCII letters, digits, or underscore
//   - must not begin with the reserved `marmot` prefix (reserved for
//     generated names such as `_marmot_vec_*` and `__marmot_vec_*`).
func ValidateIndexName(name string) error {
	if name == "" {
		return fmt.Errorf("MARMOT-VEC-015: index name must not be empty")
	}
	if len(name) > MaxIndexNameLen {
		return fmt.Errorf("MARMOT-VEC-015: index name %q exceeds %d characters", name, MaxIndexNameLen)
	}
	first := name[0]
	if !isLetter(first) {
		return fmt.Errorf("MARMOT-VEC-015: index name %q must start with a letter", name)
	}
	for i := 1; i < len(name); i++ {
		c := name[i]
		if !isLetter(c) && !isDigit(c) && c != '_' {
			return fmt.Errorf("MARMOT-VEC-015: index name %q contains invalid character %q", name, c)
		}
	}
	if strings.HasPrefix(strings.ToLower(name), "marmot") {
		return fmt.Errorf("MARMOT-VEC-015: index name %q uses reserved prefix 'marmot'", name)
	}
	return nil
}

func isLetter(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

// CentroidsTable returns the replicated centroids table name for the given
// index. Single leading underscore marks it for CDC replication (see
// design §3.4).
func CentroidsTable(idx string) string {
	return "_marmot_vec_" + idx + "_centroids"
}

// MembersTable returns the CDC-excluded members table name (double
// leading underscore, see design §3.4).
func MembersTable(idx string) string {
	return "__marmot_vec_" + idx + "_members"
}

// MembersRowidIndex returns the secondary index name on members(rowid).
func MembersRowidIndex(idx string) string {
	return "__marmot_vec_" + idx + "_members_rid"
}

// MembersRowidUniqueIndex returns the unique secondary index name that
// enforces one live sidecar row per base-table rowid.
func MembersRowidUniqueIndex(idx string) string {
	return "__marmot_vec_" + idx + "_members_rowid_uq"
}

// TriggerInsert returns the AFTER INSERT trigger name on the base table.
func TriggerInsert(idx string) string {
	return "__marmot_vec_" + idx + "_ai"
}

// TriggerUpdate returns the AFTER UPDATE OF <column> trigger name on the
// base table.
func TriggerUpdate(idx string) string {
	return "__marmot_vec_" + idx + "_au"
}

// TriggerDelete returns the AFTER DELETE trigger name on the base table.
func TriggerDelete(idx string) string {
	return "__marmot_vec_" + idx + "_ad"
}

// TriggerCentroidChange returns the AFTER INSERT trigger name on the
// centroids table used to notify replicas of a centroid rebuild (design
// §8.8).
func TriggerCentroidChange(idx string) string {
	return "__marmot_vec_" + idx + "_centroids_ai"
}

// TriggerCentroidsVersionUpdate returns the AFTER UPDATE OF version trigger
// name on the centroids table (design §8.8). Fires when a remote REINDEX
// bumps the version column.
func TriggerCentroidsVersionUpdate(idx string) string {
	return "__marmot_vec_" + idx + "_centroids_au"
}

// StagingTable returns the ephemeral staging members table name used during
// a shadow-swap REINDEX (design §8.3). Dropped and recreated each REINDEX.
func StagingTable(idx string) string {
	return "__marmot_vec_" + idx + "_members_next"
}

// StagingRowidIndex returns the transient rowid index name used during
// REINDEX populate before the staging table is swapped into members.
func StagingRowidIndex(idx string) string {
	return "__marmot_vec_" + idx + "_members_next_rid"
}

// StagingRowidUniqueIndex returns the transient unique rowid index name used
// during REINDEX populate to enforce one staged row per base-table rowid.
func StagingRowidUniqueIndex(idx string) string {
	return "__marmot_vec_" + idx + "_members_next_rowid_uq"
}

// vecLocalPrefix is the double-underscore prefix for CDC-excluded vec objects.
const vecLocalPrefix = "__marmot_vec_"

// IsVecLocalTable reports whether tableName is a CDC-excluded vector index
// object (members, staging, triggers). These use the double-underscore prefix
// per design §3.4. Used by the CDC applier to tolerate "no such table"
// errors during DROP races (fix R7).
func IsVecLocalTable(tableName string) bool {
	return strings.HasPrefix(tableName, vecLocalPrefix)
}
