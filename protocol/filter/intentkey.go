package filter

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/cespare/xxhash/v2"
)

// Binary intent key version and type markers
const (
	IntentKeyVersionDML byte = 0x01 // DML row operations
	IntentKeyMarkerDDL  byte = 0xFE // DDL operations
	IntentKeyMarkerDBOp byte = 0xFF // Database operations
)

// PK value type tags for binary encoding
const (
	PKTypeNull    byte = 0x00
	PKTypeInt64   byte = 0x01
	PKTypeFloat64 byte = 0x02
	PKTypeString  byte = 0x03
	PKTypeBytes   byte = 0x04
)

// Errors for binary intent key operations
var (
	ErrInvalidIntentKey    = errors.New("invalid intent key format")
	ErrUnknownPKType       = errors.New("unknown PK type tag")
	ErrInvalidPKValueCount = errors.New("PK value count mismatch")
)

// NullSentinel is used to represent NULL values in intent keys.
// Using a value that cannot appear in base64 encoding for safety.
// This must be consistent across all intent key generation paths.
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

// SerializeIntentKey creates a deterministic intent key from table name and PK values.
// This is the canonical implementation used by both hook-based and AST-based paths.
//
// Formats:
//   - Single numeric PK: "table:value" (e.g., "users:123")
//   - Single non-numeric PK: "table:b64:<base64>" (e.g., "users:b64:YTpi")
//   - Composite PK: "table:c:<base64>:<base64>:..." with NullSentinel for missing values
//
// Parameters:
//   - table: table name
//   - pkColumns: PK column names in declaration order (NOT sorted - order is preserved)
//   - pkValues: map of column name to serialized value bytes
func SerializeIntentKey(table string, pkColumns []string, pkValues map[string][]byte) string {
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

	// Composite keys: use PK declaration order (caller provides correctly ordered pkColumns)
	// Build parts with explicit NULL representation
	var parts []string
	for _, col := range pkColumns {
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

// HashIntentKeyXXH64 creates a uint64 hash from an intent key string using XXH64.
func HashIntentKeyXXH64(intentKey string) uint64 {
	return xxhash.Sum64String(intentKey)
}

// HashPrimaryKeyXXH64 creates a uint64 hash from table name and primary key values.
func HashPrimaryKeyXXH64(table string, pkValues map[string][]byte) uint64 {
	h := xxhash.New()
	_, _ = h.Write([]byte(table))

	// Sort keys for deterministic hashing
	keys := make([]string, 0, len(pkValues))
	for k := range pkValues {
		keys = append(keys, k)
	}
	sortStrings(keys)

	for _, k := range keys {
		_, _ = h.Write([]byte(k))
		_, _ = h.Write(pkValues[k])
	}

	return h.Sum64()
}

// KeyHashCollector collects XXH64 hashes of intent keys for conflict detection.
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

// AddIntentKey hashes and adds an intent key string.
func (c *KeyHashCollector) AddIntentKey(intentKey string) {
	c.keys[HashIntentKeyXXH64(intentKey)] = struct{}{}
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

// TypedPKValue holds a PK value with its type tag for binary encoding.
type TypedPKValue struct {
	Type  byte   // PKTypeNull, PKTypeInt64, PKTypeFloat64, PKTypeString, PKTypeBytes
	Value []byte // Raw value bytes (empty for NULL)
}

// EncodeIntentKey creates a binary intent key for DML operations.
// Format: version(1) + uvarint(tableLen) + table + uvarint(pkCount) + [type(1) + value]...
func EncodeIntentKey(table string, pkValues []TypedPKValue) []byte {
	// Estimate capacity: version + table len varint + table + pk count varint + values
	estimatedSize := 1 + binary.MaxVarintLen64 + len(table) + binary.MaxVarintLen64 + len(pkValues)*10
	buf := make([]byte, 0, estimatedSize)

	// Version byte
	buf = append(buf, IntentKeyVersionDML)

	// Table name: uvarint length + bytes
	buf = binary.AppendUvarint(buf, uint64(len(table)))
	buf = append(buf, table...)

	// PK count
	buf = binary.AppendUvarint(buf, uint64(len(pkValues)))

	// Each PK value
	for _, pv := range pkValues {
		buf = append(buf, pv.Type)
		switch pv.Type {
		case PKTypeNull:
			// No value bytes for NULL
		case PKTypeInt64:
			// 8 bytes big-endian
			buf = append(buf, pv.Value...)
		case PKTypeFloat64:
			// 8 bytes big-endian IEEE-754
			buf = append(buf, pv.Value...)
		case PKTypeString, PKTypeBytes:
			// uvarint length + bytes
			buf = binary.AppendUvarint(buf, uint64(len(pv.Value)))
			buf = append(buf, pv.Value...)
		}
	}

	return buf
}

// EncodeIntentKeyWithPrefix creates a binary intent key using a precomputed table prefix.
// The prefix should contain: version(1) + uvarint(tableLen) + table
func EncodeIntentKeyWithPrefix(prefix []byte, pkValues []TypedPKValue) []byte {
	// Estimate capacity
	estimatedSize := len(prefix) + binary.MaxVarintLen64 + len(pkValues)*10
	buf := make([]byte, 0, estimatedSize)

	// Copy prefix (version + table)
	buf = append(buf, prefix...)

	// PK count
	buf = binary.AppendUvarint(buf, uint64(len(pkValues)))

	// Each PK value
	for _, pv := range pkValues {
		buf = append(buf, pv.Type)
		switch pv.Type {
		case PKTypeNull:
			// No value bytes for NULL
		case PKTypeInt64:
			buf = append(buf, pv.Value...)
		case PKTypeFloat64:
			buf = append(buf, pv.Value...)
		case PKTypeString, PKTypeBytes:
			buf = binary.AppendUvarint(buf, uint64(len(pv.Value)))
			buf = append(buf, pv.Value...)
		}
	}

	return buf
}

// BuildIntentKeyPrefix creates the precomputed prefix for a table.
// Format: version(1) + uvarint(tableLen) + table
func BuildIntentKeyPrefix(table string) []byte {
	buf := make([]byte, 0, 1+binary.MaxVarintLen64+len(table))
	buf = append(buf, IntentKeyVersionDML)
	buf = binary.AppendUvarint(buf, uint64(len(table)))
	buf = append(buf, table...)
	return buf
}

// EncodeDDLIntentKey creates a binary intent key for DDL operations.
// Format: marker(0xFE) + uvarint(tableLen) + table
func EncodeDDLIntentKey(table string) []byte {
	buf := make([]byte, 0, 1+binary.MaxVarintLen64+len(table))
	buf = append(buf, IntentKeyMarkerDDL)
	buf = binary.AppendUvarint(buf, uint64(len(table)))
	buf = append(buf, table...)
	return buf
}

// EncodeDBOpIntentKey creates a binary intent key for database operations.
// Format: marker(0xFF) + uvarint(dbLen) + db
func EncodeDBOpIntentKey(database string) []byte {
	buf := make([]byte, 0, 1+binary.MaxVarintLen64+len(database))
	buf = append(buf, IntentKeyMarkerDBOp)
	buf = binary.AppendUvarint(buf, uint64(len(database)))
	buf = append(buf, database...)
	return buf
}

// DecodeIntentKey decodes a binary intent key and returns its components.
// Returns table name, PK values, and type (DML/DDL/DBOp marker).
func DecodeIntentKey(key []byte) (table string, pkValues []TypedPKValue, keyType byte, err error) {
	if len(key) == 0 {
		return "", nil, 0, ErrInvalidIntentKey
	}

	keyType = key[0]
	offset := 1

	// Read table/database name
	tableLen, n := binary.Uvarint(key[offset:])
	if n <= 0 || offset+n+int(tableLen) > len(key) {
		return "", nil, 0, ErrInvalidIntentKey
	}
	offset += n
	table = string(key[offset : offset+int(tableLen)])
	offset += int(tableLen)

	// DDL and DBOp don't have PK values
	if keyType == IntentKeyMarkerDDL || keyType == IntentKeyMarkerDBOp {
		return table, nil, keyType, nil
	}

	// DML: read PK values
	if keyType != IntentKeyVersionDML {
		return "", nil, 0, ErrInvalidIntentKey
	}

	pkCount, n := binary.Uvarint(key[offset:])
	if n <= 0 {
		return "", nil, 0, ErrInvalidIntentKey
	}
	offset += n

	pkValues = make([]TypedPKValue, 0, pkCount)
	for i := uint64(0); i < pkCount; i++ {
		if offset >= len(key) {
			return "", nil, 0, ErrInvalidIntentKey
		}

		typeTag := key[offset]
		offset++

		var value []byte
		switch typeTag {
		case PKTypeNull:
			// No value bytes
		case PKTypeInt64:
			if offset+8 > len(key) {
				return "", nil, 0, ErrInvalidIntentKey
			}
			value = key[offset : offset+8]
			offset += 8
		case PKTypeFloat64:
			if offset+8 > len(key) {
				return "", nil, 0, ErrInvalidIntentKey
			}
			value = key[offset : offset+8]
			offset += 8
		case PKTypeString, PKTypeBytes:
			valLen, n := binary.Uvarint(key[offset:])
			if n <= 0 || offset+n+int(valLen) > len(key) {
				return "", nil, 0, ErrInvalidIntentKey
			}
			offset += n
			value = key[offset : offset+int(valLen)]
			offset += int(valLen)
		default:
			return "", nil, 0, ErrUnknownPKType
		}

		pkValues = append(pkValues, TypedPKValue{Type: typeTag, Value: value})
	}

	return table, pkValues, keyType, nil
}

// EncodeInt64 encodes an int64 value for use in binary intent keys.
// Returns 8-byte big-endian representation.
func EncodeInt64(v int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(v))
	return buf
}

// DecodeInt64 decodes an int64 value from binary intent key format.
func DecodeInt64(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}

// EncodeFloat64 encodes a float64 value for use in binary intent keys.
// Returns 8-byte big-endian IEEE-754 representation.
// NaN is normalized to canonical form, -0 is normalized to +0.
func EncodeFloat64(v float64) []byte {
	// Normalize special values
	if math.IsNaN(v) {
		v = math.NaN() // Canonical NaN
	}
	if v == 0 && math.Signbit(v) {
		v = 0 // Normalize -0 to +0
	}

	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, math.Float64bits(v))
	return buf
}

// DecodeFloat64 decodes a float64 value from binary intent key format.
func DecodeFloat64(b []byte) float64 {
	return math.Float64frombits(binary.BigEndian.Uint64(b))
}

// GetIntentKeyTable extracts just the table name from a binary intent key.
// Faster than full DecodeIntentKey when only table is needed.
func GetIntentKeyTable(key []byte) (string, error) {
	if len(key) < 2 {
		return "", ErrInvalidIntentKey
	}

	offset := 1
	tableLen, n := binary.Uvarint(key[offset:])
	if n <= 0 || offset+n+int(tableLen) > len(key) {
		return "", ErrInvalidIntentKey
	}
	offset += n
	return string(key[offset : offset+int(tableLen)]), nil
}

// HashBinaryIntentKey creates a uint64 hash from a binary intent key using XXH64.
func HashBinaryIntentKey(intentKey []byte) uint64 {
	return xxhash.Sum64(intentKey)
}

// IntentKeyToBase64 encodes a binary intent key to base64url for JSON/URLs.
func IntentKeyToBase64(key []byte) string {
	return base64.RawURLEncoding.EncodeToString(key)
}

// IntentKeyFromBase64 decodes a binary intent key from base64url.
func IntentKeyFromBase64(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(s)
}
