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

// vecLocalPrefix is the double-underscore prefix for local-only vector index
// artifacts. The current implementation keeps serving state in local files,
// but we still tolerate legacy local SQLite object names during upgrade/drop
// races and crash recovery.
const vecLocalPrefix = "__marmot_vec_"

// IsVecLocalTable reports whether tableName is a legacy local-only vector
// object. Used by the CDC applier to tolerate DROP races against pre-cutover
// SQLite artifacts without treating them as replicated user tables.
func IsVecLocalTable(tableName string) bool {
	return strings.HasPrefix(tableName, vecLocalPrefix)
}
