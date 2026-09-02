//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBlobFidelity_RoundTrip_Insert reproduces the LLDAP-shaped bug: a BLOB
// column (password hash / BINARY(16) UUID equivalent) must keep its BLOB
// storage class end to end through capture, msgpack encode/decode, and apply
// on a replica - not be silently coerced to TEXT.
func TestBlobFidelity_RoundTrip_Insert(t *testing.T) {
	source := newRowidTestDatabase(t, 1)
	replica := newRowidTestDatabase(t, 2)

	const ddl = `CREATE TABLE creds (id INTEGER PRIMARY KEY, name TEXT, pwhash BLOB)`
	require.NoError(t, execAndReload(source, ddl))
	require.NoError(t, execAndReload(replica, ddl))

	blobVal := []byte{0x00, 0xFF, 0x10, 0xAB, 0x00, 0x01, 0x02, 0xDE, 0xAD, 0xBE, 0xEF}

	ctx := context.Background()
	session, err := StartEphemeralSession(ctx, source.hookDB, source.metaStore, source.schemaCache, 9001)
	require.NoError(t, err)
	defer session.Rollback()
	require.NoError(t, session.BeginTx(ctx))
	_, err = session.ExecContext(ctx,
		"INSERT INTO creds (id, name, pwhash) VALUES (?, ?, ?)", 1, "alice", blobVal)
	require.NoError(t, err)
	entries, err := session.GetIntentEntries()
	require.NoError(t, err)
	require.NoError(t, session.Commit())
	require.Len(t, entries, 1)

	applyEntries(t, replica, entries)

	var typeofPwhash, typeofName string
	var gotBlob []byte
	var gotName string
	require.NoError(t, replica.GetWriteDB().QueryRow(
		`SELECT typeof(pwhash), typeof(name), pwhash, name FROM creds WHERE id = 1`).
		Scan(&typeofPwhash, &typeofName, &gotBlob, &gotName))

	assert.Equal(t, "blob", typeofPwhash, "BLOB column must keep BLOB storage class on the replica")
	assert.Equal(t, "text", typeofName, "TEXT column must stay TEXT")
	assert.Equal(t, blobVal, gotBlob, "BLOB bytes must round-trip exactly")
	assert.Equal(t, "alice", gotName)
}

// TestBlobFidelity_RoundTrip_Update verifies an UPDATE that changes a BLOB
// column also preserves BLOB storage class on apply.
func TestBlobFidelity_RoundTrip_Update(t *testing.T) {
	source := newRowidTestDatabase(t, 1)
	replica := newRowidTestDatabase(t, 2)

	const ddl = `CREATE TABLE creds (id INTEGER PRIMARY KEY, pwhash BLOB)`
	require.NoError(t, execAndReload(source, ddl))
	require.NoError(t, execAndReload(replica, ddl))

	ctx := context.Background()
	session, err := StartEphemeralSession(ctx, source.hookDB, source.metaStore, source.schemaCache, 9102)
	require.NoError(t, err)
	require.NoError(t, session.BeginTx(ctx))
	_, err = session.ExecContext(ctx, "INSERT INTO creds (id, pwhash) VALUES (?, ?)", 1, []byte{0x01, 0x02})
	require.NoError(t, err)
	entries, err := session.GetIntentEntries()
	require.NoError(t, err)
	require.NoError(t, session.Commit())
	applyEntries(t, replica, entries)

	newBlob := []byte{0xCA, 0xFE, 0x00, 0xBA, 0xBE}
	session2, err := StartEphemeralSession(ctx, source.hookDB, source.metaStore, source.schemaCache, 9103)
	require.NoError(t, err)
	require.NoError(t, session2.BeginTx(ctx))
	_, err = session2.ExecContext(ctx, "UPDATE creds SET pwhash = ? WHERE id = 1", newBlob)
	require.NoError(t, err)
	updateEntries, err := session2.GetIntentEntries()
	require.NoError(t, err)
	require.NoError(t, session2.Commit())
	require.Len(t, updateEntries, 1)
	applyEntries(t, replica, updateEntries)

	var typeofPwhash string
	var gotBlob []byte
	require.NoError(t, replica.GetWriteDB().QueryRow(
		`SELECT typeof(pwhash), pwhash FROM creds WHERE id = 1`).Scan(&typeofPwhash, &gotBlob))
	assert.Equal(t, "blob", typeofPwhash)
	assert.Equal(t, newBlob, gotBlob)
}

// TestBlobFidelity_NullBlob verifies a NULL value in a BLOB column round trips
// as NULL, not as an empty string or empty blob.
func TestBlobFidelity_NullBlob(t *testing.T) {
	source := newRowidTestDatabase(t, 1)
	replica := newRowidTestDatabase(t, 2)

	const ddl = `CREATE TABLE creds (id INTEGER PRIMARY KEY, pwhash BLOB)`
	require.NoError(t, execAndReload(source, ddl))
	require.NoError(t, execAndReload(replica, ddl))

	ctx := context.Background()
	session, err := StartEphemeralSession(ctx, source.hookDB, source.metaStore, source.schemaCache, 9201)
	require.NoError(t, err)
	require.NoError(t, session.BeginTx(ctx))
	_, err = session.ExecContext(ctx, "INSERT INTO creds (id, pwhash) VALUES (?, NULL)", 1)
	require.NoError(t, err)
	entries, err := session.GetIntentEntries()
	require.NoError(t, err)
	require.NoError(t, session.Commit())
	applyEntries(t, replica, entries)

	var typeofPwhash string
	require.NoError(t, replica.GetWriteDB().QueryRow(
		`SELECT typeof(pwhash) FROM creds WHERE id = 1`).Scan(&typeofPwhash))
	assert.Equal(t, "null", typeofPwhash)
}

// TestBlobFidelity_EmptyBlob verifies a zero-length BLOB round trips as an
// empty blob, not NULL and not an empty string.
func TestBlobFidelity_EmptyBlob(t *testing.T) {
	source := newRowidTestDatabase(t, 1)
	replica := newRowidTestDatabase(t, 2)

	const ddl = `CREATE TABLE creds (id INTEGER PRIMARY KEY, pwhash BLOB)`
	require.NoError(t, execAndReload(source, ddl))
	require.NoError(t, execAndReload(replica, ddl))

	ctx := context.Background()
	session, err := StartEphemeralSession(ctx, source.hookDB, source.metaStore, source.schemaCache, 9301)
	require.NoError(t, err)
	require.NoError(t, session.BeginTx(ctx))
	_, err = session.ExecContext(ctx, "INSERT INTO creds (id, pwhash) VALUES (?, ?)", 1, []byte{})
	require.NoError(t, err)
	entries, err := session.GetIntentEntries()
	require.NoError(t, err)
	require.NoError(t, session.Commit())
	applyEntries(t, replica, entries)

	var typeofPwhash string
	var gotBlob []byte
	require.NoError(t, replica.GetWriteDB().QueryRow(
		`SELECT typeof(pwhash), pwhash FROM creds WHERE id = 1`).Scan(&typeofPwhash, &gotBlob))
	assert.Equal(t, "blob", typeofPwhash)
	assert.Len(t, gotBlob, 0)
}

// TestBlobFidelity_VarcharAffinity verifies a VARCHAR-declared column (TEXT
// affinity via the CHAR substring rule, hence not BLOB affinity) round trips
// as text like a plain TEXT column, confirming isBlobAffinity's precedence.
func TestBlobFidelity_VarcharAffinity(t *testing.T) {
	source := newRowidTestDatabase(t, 1)
	replica := newRowidTestDatabase(t, 2)

	const ddl = `CREATE TABLE users (id INTEGER PRIMARY KEY, email VARCHAR(255))`
	require.NoError(t, execAndReload(source, ddl))
	require.NoError(t, execAndReload(replica, ddl))

	ctx := context.Background()
	session, err := StartEphemeralSession(ctx, source.hookDB, source.metaStore, source.schemaCache, 9401)
	require.NoError(t, err)
	require.NoError(t, session.BeginTx(ctx))
	_, err = session.ExecContext(ctx, "INSERT INTO users (id, email) VALUES (?, ?)", 1, "a@example.com")
	require.NoError(t, err)
	entries, err := session.GetIntentEntries()
	require.NoError(t, err)
	require.NoError(t, session.Commit())
	applyEntries(t, replica, entries)

	var typeofEmail, email string
	require.NoError(t, replica.GetWriteDB().QueryRow(
		`SELECT typeof(email), email FROM users WHERE id = 1`).Scan(&typeofEmail, &email))
	assert.Equal(t, "text", typeofEmail)
	assert.Equal(t, "a@example.com", email)
}

// TestBlobFidelity_NumericAffinityHoldingText verifies that SQLite's dynamic
// typing case - a NUMERIC-affinity column (declared DECIMAL, which matches
// none of SQLite's INT/CHAR/CLOB/TEXT/BLOB/REAL/FLOA/DOUB substring rules and
// so falls to the NUMERIC catch-all) holding a value that doesn't parse as a
// number - replicates as TEXT storage class, not BLOB. NUMERIC affinity only
// converts well-formed numeric-looking text on INSERT; anything else is left
// as TEXT storage class untouched, so the preupdate hook's []byte for it must
// decode as string here, the same as for a declared TEXT column.
func TestBlobFidelity_NumericAffinityHoldingText(t *testing.T) {
	source := newRowidTestDatabase(t, 1)
	replica := newRowidTestDatabase(t, 2)

	const ddl = `CREATE TABLE prices (id INTEGER PRIMARY KEY, amount DECIMAL)`
	require.NoError(t, execAndReload(source, ddl))
	require.NoError(t, execAndReload(replica, ddl))

	ctx := context.Background()
	session, err := StartEphemeralSession(ctx, source.hookDB, source.metaStore, source.schemaCache, 9501)
	require.NoError(t, err)
	require.NoError(t, session.BeginTx(ctx))
	_, err = session.ExecContext(ctx, "INSERT INTO prices (id, amount) VALUES (?, ?)", 1, "call for price")
	require.NoError(t, err)
	entries, err := session.GetIntentEntries()
	require.NoError(t, err)
	require.NoError(t, session.Commit())
	applyEntries(t, replica, entries)

	var typeofAmount, amount string
	require.NoError(t, replica.GetWriteDB().QueryRow(
		`SELECT typeof(amount), amount FROM prices WHERE id = 1`).Scan(&typeofAmount, &amount))
	assert.Equal(t, "text", typeofAmount, "a DECIMAL (NUMERIC affinity) column holding non-numeric text must replicate as TEXT storage class")
	assert.Equal(t, "call for price", amount)
}

// TestBlobFidelity_IntegerAffinityHoldingText is the same case as
// TestBlobFidelity_NumericAffinityHoldingText for INTEGER affinity
// specifically (declared type containing "INT", SQLite's highest-precedence
// affinity rule).
func TestBlobFidelity_IntegerAffinityHoldingText(t *testing.T) {
	source := newRowidTestDatabase(t, 1)
	replica := newRowidTestDatabase(t, 2)

	const ddl = `CREATE TABLE items (id INTEGER PRIMARY KEY, code INTEGER)`
	require.NoError(t, execAndReload(source, ddl))
	require.NoError(t, execAndReload(replica, ddl))

	ctx := context.Background()
	session, err := StartEphemeralSession(ctx, source.hookDB, source.metaStore, source.schemaCache, 9502)
	require.NoError(t, err)
	require.NoError(t, session.BeginTx(ctx))
	_, err = session.ExecContext(ctx, "INSERT INTO items (id, code) VALUES (?, ?)", 1, "ABC-123")
	require.NoError(t, err)
	entries, err := session.GetIntentEntries()
	require.NoError(t, err)
	require.NoError(t, session.Commit())
	applyEntries(t, replica, entries)

	var typeofCode, code string
	require.NoError(t, replica.GetWriteDB().QueryRow(
		`SELECT typeof(code), code FROM items WHERE id = 1`).Scan(&typeofCode, &code))
	assert.Equal(t, "text", typeofCode, "an INTEGER-affinity column holding non-numeric text must replicate as TEXT storage class")
	assert.Equal(t, "ABC-123", code)
}

// TestBlobFidelity_UndeclaredTypeStaysBlob verifies a column with NO declared
// type at all (valid SQLite syntax, e.g. WordPress/ORM-generated schemas
// sometimes emit these) gets BLOB affinity per SQLite's rules and a genuine
// BLOB value in it still round-trips as BLOB storage class.
func TestBlobFidelity_UndeclaredTypeStaysBlob(t *testing.T) {
	source := newRowidTestDatabase(t, 1)
	replica := newRowidTestDatabase(t, 2)

	const ddl = `CREATE TABLE untyped (id INTEGER PRIMARY KEY, data)`
	require.NoError(t, execAndReload(source, ddl))
	require.NoError(t, execAndReload(replica, ddl))

	schema, err := source.schemaCache.GetSchemaFor("untyped")
	require.NoError(t, err)
	dataIdx := -1
	for i, col := range schema.Columns {
		if col == "data" {
			dataIdx = i
		}
	}
	require.GreaterOrEqual(t, dataIdx, 0)
	require.True(t, schema.BlobAffinityCols[dataIdx], "a column with no declared type must get BLOB affinity")

	blobVal := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	ctx := context.Background()
	session, err := StartEphemeralSession(ctx, source.hookDB, source.metaStore, source.schemaCache, 9503)
	require.NoError(t, err)
	require.NoError(t, session.BeginTx(ctx))
	_, err = session.ExecContext(ctx, "INSERT INTO untyped (id, data) VALUES (?, ?)", 1, blobVal)
	require.NoError(t, err)
	entries, err := session.GetIntentEntries()
	require.NoError(t, err)
	require.NoError(t, session.Commit())
	applyEntries(t, replica, entries)

	var typeofData string
	var gotData []byte
	require.NoError(t, replica.GetWriteDB().QueryRow(
		`SELECT typeof(data), data FROM untyped WHERE id = 1`).Scan(&typeofData, &gotData))
	assert.Equal(t, "blob", typeofData)
	assert.Equal(t, blobVal, gotData)
}

// TestEncodeValuesWithSchema_CountMismatchErrors verifies encodeValuesWithSchema
// fails loud instead of silently truncating when the captured value count
// disagrees with the schema's column count (defect: stale schema vs. row).
func TestEncodeValuesWithSchema_CountMismatchErrors(t *testing.T) {
	schema := &TableSchema{
		Columns:          []string{"a", "b", "c"},
		BlobAffinityCols: []bool{false, false, false},
	}
	_, err := encodeValuesWithSchema(schema, []interface{}{int64(1), int64(2)})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "out of range")
}

// TestEncodeValuesWithSchema_BlobVsText verifies the affinity-driven encoding
// decision directly: a []byte value for a TEXT-affinity column is converted
// to string before encoding, while a []byte value for a BLOB-affinity column
// (or unknown/empty declared type) is preserved as bytes.
func TestEncodeValuesWithSchema_BlobVsText(t *testing.T) {
	schema := &TableSchema{
		Columns:          []string{"txt", "blob"},
		BlobAffinityCols: []bool{false, true},
	}
	result, err := encodeValuesWithSchema(schema, []interface{}{
		[]byte("hello"),
		[]byte{0x00, 0xFF},
	})
	require.NoError(t, err)

	decodedText, err := unmarshalCDCValue(result["txt"])
	require.NoError(t, err)
	assert.Equal(t, "hello", decodedText, "TEXT-affinity column must decode as string")

	decodedBlob, err := unmarshalCDCValue(result["blob"])
	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0xFF}, decodedBlob, "BLOB-affinity column must decode as []byte")
}
