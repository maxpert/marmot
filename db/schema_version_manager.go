package db

import "fmt"

// SchemaVersionManager reads the schema version of every user database from
// its own source of truth: the __marmot_schema_version table inside that
// database's SQLite file, cached on its *ReplicatedDatabase. It
// replaces the old pebble-backed counter, which could diverge from the DDL
// it counted across a crash.
//
// There is no increment/set method: a DDL's tx bumps the version itself
// (db/schema_version_table.go's bumpSchemaVersionInTx, called from the DDL's
// own commit or replay path), so every writer of the version is also the
// writer of the DDL it counts. This manager only reads.
type SchemaVersionManager struct {
	dbMgr *DatabaseManager
}

// NewSchemaVersionManager creates a schema version manager reading through
// dbMgr's open databases.
func NewSchemaVersionManager(dbMgr *DatabaseManager) *SchemaVersionManager {
	return &SchemaVersionManager{dbMgr: dbMgr}
}

// GetSchemaVersion returns database's current schema version. The system
// database has no version and always returns 0. An unknown database is an
// error: unlike the old cache, there is no version to fall back to.
func (svm *SchemaVersionManager) GetSchemaVersion(database string) (uint64, error) {
	if database == "" || database == SystemDatabaseName {
		return 0, nil
	}
	mdb, err := svm.dbMgr.GetDatabase(database)
	if err != nil {
		return 0, fmt.Errorf("failed to get schema version: %w", err)
	}
	return mdb.SchemaVersion(), nil
}

// GetAllSchemaVersions returns the current schema version of every user
// database (never the system database, which ListDatabases already excludes).
func (svm *SchemaVersionManager) GetAllSchemaVersions() (map[string]uint64, error) {
	names := svm.dbMgr.ListDatabases()
	result := make(map[string]uint64, len(names))
	for _, name := range names {
		mdb, err := svm.dbMgr.GetDatabase(name)
		if err != nil {
			continue // dropped between ListDatabases and GetDatabase; skip it
		}
		result[name] = mdb.SchemaVersion()
	}
	return result, nil
}
