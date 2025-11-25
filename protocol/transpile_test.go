package protocol

import (
	"testing"
)

func TestTranspileMySQLToSQLite(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// INSERT statements
		{
			name:     "INSERT IGNORE",
			input:    "INSERT IGNORE INTO users (id, name) VALUES (1, 'test')",
			expected: "INSERT OR IGNORE INTO users (id, name) VALUES (1, 'test')",
		},
		{
			name:     "REPLACE INTO",
			input:    "REPLACE INTO users (id, name) VALUES (1, 'test')",
			expected: "INSERT OR REPLACE INTO users (id, name) VALUES (1, 'test')",
		},

		// Index hints
		{
			name:     "FORCE INDEX",
			input:    "SELECT * FROM users FORCE INDEX(PRIMARY) WHERE id = 1",
			expected: "SELECT * FROM users WHERE id = 1",
		},
		{
			name:     "USE INDEX",
			input:    "SELECT * FROM users USE INDEX(idx_name) WHERE name = 'test'",
			expected: "SELECT * FROM users WHERE name = 'test'",
		},
		{
			name:     "IGNORE INDEX",
			input:    "SELECT * FROM users IGNORE INDEX(idx_name) WHERE id > 10",
			expected: "SELECT * FROM users WHERE id > 10",
		},
		{
			name:     "FORCE INDEX with backticks",
			input:    "SELECT * FROM users FORCE INDEX(`PRIMARY`) WHERE id = 1",
			expected: "SELECT * FROM users WHERE id = 1",
		},

		// SELECT modifiers
		{
			name:     "STRAIGHT_JOIN",
			input:    "SELECT STRAIGHT_JOIN * FROM users u JOIN orders o ON u.id = o.user_id",
			expected: "SELECT JOIN * FROM users u JOIN orders o ON u.id = o.user_id",
		},
		{
			name:     "SQL_CALC_FOUND_ROWS",
			input:    "SELECT SQL_CALC_FOUND_ROWS * FROM users LIMIT 10",
			expected: "SELECT * FROM users LIMIT 10",
		},
		{
			name:     "SQL_CACHE",
			input:    "SELECT SQL_CACHE * FROM users WHERE id = 1",
			expected: "SELECT * FROM users WHERE id = 1",
		},
		{
			name:     "SQL_NO_CACHE",
			input:    "SELECT SQL_NO_CACHE * FROM users WHERE id = 1",
			expected: "SELECT * FROM users WHERE id = 1",
		},
		{
			name:     "SQL_SMALL_RESULT",
			input:    "SELECT SQL_SMALL_RESULT * FROM users GROUP BY status",
			expected: "SELECT * FROM users GROUP BY status",
		},
		{
			name:     "HIGH_PRIORITY",
			input:    "SELECT HIGH_PRIORITY * FROM users WHERE id = 1",
			expected: "SELECT * FROM users WHERE id = 1",
		},

		// Locking clauses
		{
			name:     "FOR UPDATE",
			input:    "SELECT * FROM users WHERE id = 1 FOR UPDATE",
			expected: "SELECT * FROM users WHERE id = 1",
		},
		{
			name:     "LOCK IN SHARE MODE",
			input:    "SELECT * FROM users WHERE id = 1 LOCK IN SHARE MODE",
			expected: "SELECT * FROM users WHERE id = 1",
		},

		// LIMIT clause
		{
			name:     "LIMIT offset, count",
			input:    "SELECT * FROM users LIMIT 10, 20",
			expected: "SELECT * FROM users LIMIT 20 OFFSET 10",
		},
		{
			name:     "LIMIT with OFFSET (already correct)",
			input:    "SELECT * FROM users LIMIT 20 OFFSET 10",
			expected: "SELECT * FROM users LIMIT 20 OFFSET 10",
		},

		// Complex combinations
		{
			name:     "Multiple hints and modifiers",
			input:    "SELECT SQL_CALC_FOUND_ROWS * FROM users FORCE INDEX(PRIMARY) WHERE id > 10 FOR UPDATE LIMIT 5, 10",
			expected: "SELECT * FROM users WHERE id > 10 LIMIT 10 OFFSET 5",
		},
		{
			name:     "UPDATE with index hint",
			input:    "UPDATE users FORCE INDEX(PRIMARY) SET name = 'test' WHERE id = 1",
			expected: "UPDATE users SET name = 'test' WHERE id = 1",
		},
		{
			name:     "DELETE with index hint",
			input:    "DELETE FROM users FORCE INDEX(PRIMARY) WHERE id = 1",
			expected: "DELETE FROM users WHERE id = 1",
		},

		// Case insensitivity
		{
			name:     "Mixed case INSERT IGNORE",
			input:    "insert ignore into users (id) values (1)",
			expected: "INSERT OR IGNORE INTO users (id) values (1)",
		},
		{
			name:     "Mixed case REPLACE",
			input:    "Replace Into users (id) values (1)",
			expected: "INSERT OR REPLACE INTO users (id) values (1)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := transpileMySQLToSQLite(tt.input)
			// Normalize whitespace for comparison
			if result != tt.expected {
				t.Errorf("transpileMySQLToSQLite() = %q, want %q", result, tt.expected)
			}
		})
	}
}
