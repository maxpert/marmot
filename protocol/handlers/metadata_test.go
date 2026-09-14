package handlers

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/maxpert/marmot/protocol"
)

// fakeDatabaseProvider is a minimal DatabaseProvider for metadata handler tests.
type fakeDatabaseProvider struct {
	exists bool
}

func (f *fakeDatabaseProvider) ListDatabases() []string { return nil }

func (f *fakeDatabaseProvider) DatabaseExists(name string) bool { return f.exists }

func (f *fakeDatabaseProvider) GetDatabaseConnection(name string) (*sql.DB, error) {
	return nil, errors.New("not implemented")
}

func TestHandleUseDatabase_ExistingDatabase(t *testing.T) {
	m := NewMetadataHandler(&fakeDatabaseProvider{exists: true}, "system")

	err := m.HandleUseDatabase("mydb")
	if err != nil {
		t.Fatalf("expected nil error for existing database, got: %v", err)
	}
}

func TestHandleUseDatabase_MissingDatabase_ReturnsTypedMySQLError(t *testing.T) {
	m := NewMetadataHandler(&fakeDatabaseProvider{exists: false}, "system")

	err := m.HandleUseDatabase("missing_db")
	if err == nil {
		t.Fatal("expected non-nil error for missing database")
	}

	var mysqlErr *protocol.MySQLError
	if !errors.As(err, &mysqlErr) {
		t.Fatalf("expected error to be *protocol.MySQLError, got %T: %v", err, err)
	}

	if mysqlErr.Code != protocol.ErrCodeBadDB {
		t.Errorf("expected Code %d (ErrCodeBadDB), got %d", protocol.ErrCodeBadDB, mysqlErr.Code)
	}

	if mysqlErr.SQLState != "42000" {
		t.Errorf("expected SQLState \"42000\", got %q", mysqlErr.SQLState)
	}
}

func TestHandleUseDatabase_MissingDatabase_ConvertsToMySQLErrorCode1049(t *testing.T) {
	m := NewMetadataHandler(&fakeDatabaseProvider{exists: false}, "system")

	err := m.HandleUseDatabase("missing_db")
	if err == nil {
		t.Fatal("expected non-nil error for missing database")
	}

	converted := protocol.ConvertToMySQLError(err)
	if converted == nil {
		t.Fatal("expected ConvertToMySQLError to return a non-nil *MySQLError")
	}

	if converted.Code != protocol.ErrCodeBadDB {
		t.Errorf("expected ConvertToMySQLError code %d (1049), got %d", protocol.ErrCodeBadDB, converted.Code)
	}
}
