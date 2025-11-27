package filter

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/cespare/xxhash/v2"
)

// NullSentinel is used to represent NULL values in row keys.
// Using a value that cannot appear in base64 encoding for safety.
// This must be consistent across all row key generation paths.
const NullSentinel = "\x00NULL\x00"

// IsSimpleNumeric checks if the value is a simple numeric string
// (digits only, optionally with leading minus sign).
// Used to determine if a PK value can be used as-is or needs base64 encoding.
func IsSimpleNumeric(v []byte) bool {
	if len(v) == 0 {
		return false
	}
	start := 0
	if v[0] == '-' {
		if len(v) == 1 {
			return false
		}
		start = 1
	}
	for i := start; i < len(v); i++ {
		if v[i] < '0' || v[i] > '9' {
			return false
		}
	}
	return true
}

// SerializeRowKey creates a deterministic row key from table name and PK values.
// This is the canonical implementation used by both hook-based and AST-based paths.
//
// Formats:
//   - Single numeric PK: "table:value" (e.g., "users:123")
//   - Single non-numeric PK: "table:b64:<base64>" (e.g., "users:b64:YTpi")
//   - Composite PK: "table:c:<base64>:<base64>:..." with NullSentinel for missing values
//
// Parameters:
//   - table: table name
//   - pkColumns: ordered list of PK column names (will be sorted alphabetically)
//   - pkValues: map of column name to serialized value bytes
func SerializeRowKey(table string, pkColumns []string, pkValues map[string][]byte) string {
	if len(pkColumns) == 0 {
		return ""
	}

	// Single-column PK - use simpler format
	if len(pkColumns) == 1 {
		col := pkColumns[0]
		v, ok := pkValues[col]
		if !ok || v == nil {
			return fmt.Sprintf("%s:%s", table, NullSentinel)
		}

		// For simple numeric values, use raw format for efficiency
		if IsSimpleNumeric(v) {
			return fmt.Sprintf("%s:%s", table, string(v))
		}

		// For non-numeric values, use base64 to prevent separator collision
		return fmt.Sprintf("%s:b64:%s", table, base64.RawStdEncoding.EncodeToString(v))
	}

	// Composite keys: sort column names alphabetically for determinism
	sortedCols := make([]string, len(pkColumns))
	copy(sortedCols, pkColumns)
	sortStrings(sortedCols)

	// Build parts with explicit NULL representation
	var parts []string
	for _, col := range sortedCols {
		v, ok := pkValues[col]
		if !ok || v == nil {
			// Explicit NULL representation prevents collision
			parts = append(parts, NullSentinel)
		} else {
			// Base64 encode to prevent separator collision
			parts = append(parts, base64.RawStdEncoding.EncodeToString(v))
		}
	}

	return fmt.Sprintf("%s:c:%s", table, strings.Join(parts, ":"))
}

// sortStrings sorts a string slice in place (avoids importing sort package)
func sortStrings(s []string) {
	for i := 0; i < len(s); i++ {
		for j := i + 1; j < len(s); j++ {
			if s[i] > s[j] {
				s[i], s[j] = s[j], s[i]
			}
		}
	}
}

// HashRowKeyXXH64 creates a uint64 hash from a row key string using XXH64.
func HashRowKeyXXH64(rowKey string) uint64 {
	return xxhash.Sum64String(rowKey)
}

// HashPrimaryKeyXXH64 creates a uint64 hash from table name and primary key values.
func HashPrimaryKeyXXH64(table string, pkValues map[string][]byte) uint64 {
	h := xxhash.New()
	h.Write([]byte(table))

	// Sort keys for deterministic hashing
	keys := make([]string, 0, len(pkValues))
	for k := range pkValues {
		keys = append(keys, k)
	}
	sortStrings(keys)

	for _, k := range keys {
		h.Write([]byte(k))
		h.Write(pkValues[k])
	}

	return h.Sum64()
}

// KeyHashCollector collects XXH64 hashes of row keys for conflict detection.
// Stores only unique hashes for O(1) intersection checking.
type KeyHashCollector struct {
	keys map[uint64]struct{}
}

// NewKeyHashCollector creates a new collector.
func NewKeyHashCollector() *KeyHashCollector {
	return &KeyHashCollector{
		keys: make(map[uint64]struct{}),
	}
}

// AddRowKey hashes and adds a row key string.
func (c *KeyHashCollector) AddRowKey(rowKey string) {
	c.keys[HashRowKeyXXH64(rowKey)] = struct{}{}
}

// Count returns the number of unique keys.
func (c *KeyHashCollector) Count() int {
	return len(c.keys)
}

// Keys returns all hashes as a slice.
func (c *KeyHashCollector) Keys() []uint64 {
	result := make([]uint64, 0, len(c.keys))
	for k := range c.keys {
		result = append(result, k)
	}
	return result
}
