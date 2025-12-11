package protocol

import (
	"testing"

	"github.com/maxpert/marmot/hlc"
)

func TestConsistencyLevel_String(t *testing.T) {
	tests := []struct {
		level ConsistencyLevel
		want  string
	}{
		{ConsistencyLocalOne, "LOCAL_ONE"},
		{ConsistencyOne, "ONE"},
		{ConsistencyQuorum, "QUORUM"},
		{ConsistencyAll, "ALL"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.level.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseConsistencyLevel(t *testing.T) {
	tests := []struct {
		input     string
		want      ConsistencyLevel
		wantError bool
	}{
		{"LOCAL_ONE", ConsistencyLocalOne, false},
		{"ONE", ConsistencyOne, false},
		{"QUORUM", ConsistencyQuorum, false},
		{"ALL", ConsistencyAll, false},
		{"INVALID", ConsistencyLocalOne, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseConsistencyLevel(tt.input)
			if tt.wantError && err == nil {
				t.Error("Expected error, got nil")
			}
			if !tt.wantError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if !tt.wantError && got != tt.want {
				t.Errorf("ParseConsistencyLevel() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewTransaction(t *testing.T) {
	txn := NewTransaction(123)

	if txn.ID != 123 {
		t.Errorf("Expected ID 123, got %d", txn.ID)
	}

	if !txn.IsInProgress() {
		t.Error("New transaction should be in progress")
	}

	if txn.StatementCount() != 0 {
		t.Errorf("New transaction should have 0 statements, got %d", txn.StatementCount())
	}

	if txn.WriteConsistency != ConsistencyQuorum {
		t.Errorf("Default write consistency should be QUORUM, got %v", txn.WriteConsistency)
	}
}

func TestTransaction_AddStatement(t *testing.T) {
	txn := NewTransaction(1)

	stmt := Statement{
		SQL:       "INSERT INTO users VALUES (1, 'Alice')",
		Type:      StatementInsert,
		TableName: "users",
	}

	err := txn.AddStatement(stmt)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if txn.StatementCount() != 1 {
		t.Errorf("Expected 1 statement, got %d", txn.StatementCount())
	}

	// Try adding after commit
	_ = txn.Commit()
	err = txn.AddStatement(stmt)
	if err == nil {
		t.Error("Should not be able to add statement after commit")
	}
}

func TestTransaction_Commit(t *testing.T) {
	txn := NewTransaction(1)

	err := txn.Commit()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if txn.IsInProgress() {
		t.Error("Transaction should not be in progress after commit")
	}

	// Try committing again
	err = txn.Commit()
	if err == nil {
		t.Error("Should not be able to commit twice")
	}
}

func TestTransaction_Rollback(t *testing.T) {
	txn := NewTransaction(1)

	stmt := Statement{
		SQL:  "INSERT INTO users VALUES (1, 'Alice')",
		Type: StatementInsert,
	}
	_ = txn.AddStatement(stmt)

	err := txn.Rollback()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if txn.IsInProgress() {
		t.Error("Transaction should not be in progress after rollback")
	}

	if txn.StatementCount() != 0 {
		t.Error("Statements should be cleared after rollback")
	}
}

func TestTransaction_SetConsistency(t *testing.T) {
	txn := NewTransaction(1)

	txn.SetWriteConsistency(ConsistencyAll)
	if txn.WriteConsistency != ConsistencyAll {
		t.Errorf("Write consistency not set correctly")
	}

	txn.SetReadConsistency(ConsistencyQuorum)
	if txn.ReadConsistency != ConsistencyQuorum {
		t.Errorf("Read consistency not set correctly")
	}
}

func TestTransaction_SetTimestamp(t *testing.T) {
	txn := NewTransaction(1)

	clock := hlc.NewClock(1)
	ts := clock.Now()

	txn.SetTimestamp(ts)

	if !hlc.Equal(txn.Timestamp, ts) {
		t.Error("Timestamp not set correctly")
	}
}

func TestTransaction_HasWrites(t *testing.T) {
	txn := NewTransaction(1)

	if txn.HasWrites() {
		t.Error("Empty transaction should not have writes")
	}

	// Add SELECT statement
	_ = txn.AddStatement(Statement{
		SQL:  "SELECT * FROM users",
		Type: StatementSelect,
	})

	if txn.HasWrites() {
		t.Error("Transaction with only SELECT should not have writes")
	}

	// Add INSERT statement
	_ = txn.AddStatement(Statement{
		SQL:  "INSERT INTO users VALUES (1, 'Alice')",
		Type: StatementInsert,
	})

	if !txn.HasWrites() {
		t.Error("Transaction with INSERT should have writes")
	}
}

func TestTransaction_GetStatements(t *testing.T) {
	txn := NewTransaction(1)

	stmt1 := Statement{SQL: "INSERT INTO users VALUES (1, 'Alice')", Type: StatementInsert}
	stmt2 := Statement{SQL: "UPDATE users SET name = 'Bob'", Type: StatementUpdate}

	_ = txn.AddStatement(stmt1)
	_ = txn.AddStatement(stmt2)

	stmts := txn.GetStatements()

	if len(stmts) != 2 {
		t.Errorf("Expected 2 statements, got %d", len(stmts))
	}

	// Verify it's a copy (modifying shouldn't affect original)
	stmts[0].SQL = "MODIFIED"
	originalStmts := txn.GetStatements()
	if originalStmts[0].SQL == "MODIFIED" {
		t.Error("GetStatements should return a copy, not the original")
	}
}

func TestTransaction_ConcurrentAccess(t *testing.T) {
	txn := NewTransaction(1)
	done := make(chan bool)

	// Spawn goroutines adding statements
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 100; j++ {
				stmt := Statement{
					SQL:  "INSERT INTO users VALUES (?, ?)",
					Type: StatementInsert,
				}
				_ = txn.AddStatement(stmt)
			}
			done <- true
		}(i)
	}

	// Wait for all
	for i := 0; i < 10; i++ {
		<-done
	}

	// Should have 1000 statements
	if txn.StatementCount() != 1000 {
		t.Errorf("Expected 1000 statements, got %d", txn.StatementCount())
	}
}

func BenchmarkTransaction_AddStatement(b *testing.B) {
	txn := NewTransaction(1)
	stmt := Statement{
		SQL:  "INSERT INTO users VALUES (1, 'Alice')",
		Type: StatementInsert,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = txn.AddStatement(stmt)
	}
}

func BenchmarkTransaction_GetStatements(b *testing.B) {
	txn := NewTransaction(1)

	// Pre-populate
	for i := 0; i < 100; i++ {
		_ = txn.AddStatement(Statement{
			SQL:  "INSERT INTO users VALUES (?, ?)",
			Type: StatementInsert,
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn.GetStatements()
	}
}
