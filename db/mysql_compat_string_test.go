package db

import (
	"database/sql"
	"testing"
)

func setupTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(SQLiteDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	return db
}

func TestMySQLConcatWS(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected interface{}
	}{
		{
			name:     "basic concat",
			query:    "SELECT CONCAT_WS(',', 'a', 'b', 'c')",
			expected: "a,b,c",
		},
		{
			name:     "with null",
			query:    "SELECT CONCAT_WS(',', 'a', NULL, 'c')",
			expected: "a,c",
		},
		{
			name:     "empty separator",
			query:    "SELECT CONCAT_WS('', 'a', 'b')",
			expected: "ab",
		},
		{
			name:     "multi-char separator",
			query:    "SELECT CONCAT_WS(' - ', 'apple', 'banana', 'cherry')",
			expected: "apple - banana - cherry",
		},
		{
			name:     "all nulls",
			query:    "SELECT CONCAT_WS(',', NULL, NULL)",
			expected: "",
		},
		{
			name:     "single value",
			query:    "SELECT CONCAT_WS(',', 'alone')",
			expected: "alone",
		},
		{
			name:     "with numbers",
			query:    "SELECT CONCAT_WS('-', 'test', 123)",
			expected: "test-123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result sql.NullString
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if tt.expected == nil {
				if result.Valid {
					t.Errorf("Expected NULL, got %v", result.String)
				}
			} else {
				if !result.Valid {
					t.Errorf("Expected %v, got NULL", tt.expected)
				} else if result.String != tt.expected {
					t.Errorf("Expected %v, got %v", tt.expected, result.String)
				}
			}
		})
	}
}

func TestMySQLLeft(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected interface{}
	}{
		{
			name:     "basic left",
			query:    "SELECT LEFT('hello', 3)",
			expected: "hel",
		},
		{
			name:     "length exceeds string",
			query:    "SELECT LEFT('hi', 10)",
			expected: "hi",
		},
		{
			name:     "zero length",
			query:    "SELECT LEFT('hello', 0)",
			expected: "",
		},
		{
			name:     "negative length",
			query:    "SELECT LEFT('hello', -1)",
			expected: "",
		},
		{
			name:     "unicode characters",
			query:    "SELECT LEFT('你好世界', 2)",
			expected: "你好",
		},
		{
			name:     "empty string",
			query:    "SELECT LEFT('', 5)",
			expected: "",
		},
		{
			name:     "null input",
			query:    "SELECT LEFT(NULL, 3)",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result sql.NullString
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if tt.expected == nil {
				if result.Valid {
					t.Errorf("Expected NULL, got %v", result.String)
				}
			} else {
				if !result.Valid {
					t.Errorf("Expected %v, got NULL", tt.expected)
				} else if result.String != tt.expected {
					t.Errorf("Expected %v, got %v", tt.expected, result.String)
				}
			}
		})
	}
}

func TestMySQLRight(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected interface{}
	}{
		{
			name:     "basic right",
			query:    "SELECT RIGHT('hello', 3)",
			expected: "llo",
		},
		{
			name:     "length exceeds string",
			query:    "SELECT RIGHT('hi', 10)",
			expected: "hi",
		},
		{
			name:     "zero length",
			query:    "SELECT RIGHT('hello', 0)",
			expected: "",
		},
		{
			name:     "negative length",
			query:    "SELECT RIGHT('hello', -1)",
			expected: "",
		},
		{
			name:     "unicode characters",
			query:    "SELECT RIGHT('你好世界', 2)",
			expected: "世界",
		},
		{
			name:     "empty string",
			query:    "SELECT RIGHT('', 5)",
			expected: "",
		},
		{
			name:     "null input",
			query:    "SELECT RIGHT(NULL, 3)",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result sql.NullString
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if tt.expected == nil {
				if result.Valid {
					t.Errorf("Expected NULL, got %v", result.String)
				}
			} else {
				if !result.Valid {
					t.Errorf("Expected %v, got NULL", tt.expected)
				} else if result.String != tt.expected {
					t.Errorf("Expected %v, got %v", tt.expected, result.String)
				}
			}
		})
	}
}

func TestMySQLReverse(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected interface{}
	}{
		{
			name:     "basic reverse",
			query:    "SELECT REVERSE('hello')",
			expected: "olleh",
		},
		{
			name:     "single char",
			query:    "SELECT REVERSE('a')",
			expected: "a",
		},
		{
			name:     "empty string",
			query:    "SELECT REVERSE('')",
			expected: "",
		},
		{
			name:     "unicode characters",
			query:    "SELECT REVERSE('你好世界')",
			expected: "界世好你",
		},
		{
			name:     "palindrome",
			query:    "SELECT REVERSE('racecar')",
			expected: "racecar",
		},
		{
			name:     "null input",
			query:    "SELECT REVERSE(NULL)",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result sql.NullString
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if tt.expected == nil {
				if result.Valid {
					t.Errorf("Expected NULL, got %v", result.String)
				}
			} else {
				if !result.Valid {
					t.Errorf("Expected %v, got NULL", tt.expected)
				} else if result.String != tt.expected {
					t.Errorf("Expected %v, got %v", tt.expected, result.String)
				}
			}
		})
	}
}

func TestMySQLLPad(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected interface{}
	}{
		{
			name:     "basic lpad",
			query:    "SELECT LPAD('hi', 5, '0')",
			expected: "000hi",
		},
		{
			name:     "exact length",
			query:    "SELECT LPAD('hello', 5, '0')",
			expected: "hello",
		},
		{
			name:     "truncate",
			query:    "SELECT LPAD('hello', 3, '0')",
			expected: "hel",
		},
		{
			name:     "multi-char pad",
			query:    "SELECT LPAD('x', 5, 'ab')",
			expected: "ababx",
		},
		{
			name:     "zero length",
			query:    "SELECT LPAD('hello', 0, '0')",
			expected: "",
		},
		{
			name:     "unicode pad",
			query:    "SELECT LPAD('test', 6, '😀')",
			expected: "😀😀test",
		},
		{
			name:     "null input",
			query:    "SELECT LPAD(NULL, 5, '0')",
			expected: nil,
		},
		{
			name:     "null pad",
			query:    "SELECT LPAD('hi', 5, NULL)",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result sql.NullString
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if tt.expected == nil {
				if result.Valid {
					t.Errorf("Expected NULL, got %v", result.String)
				}
			} else {
				if !result.Valid {
					t.Errorf("Expected %v, got NULL", tt.expected)
				} else if result.String != tt.expected {
					t.Errorf("Expected %v, got %v", tt.expected, result.String)
				}
			}
		})
	}
}

func TestMySQLRPad(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected interface{}
	}{
		{
			name:     "basic rpad",
			query:    "SELECT RPAD('hi', 5, '0')",
			expected: "hi000",
		},
		{
			name:     "exact length",
			query:    "SELECT RPAD('hello', 5, '0')",
			expected: "hello",
		},
		{
			name:     "truncate",
			query:    "SELECT RPAD('hello', 3, '0')",
			expected: "hel",
		},
		{
			name:     "multi-char pad",
			query:    "SELECT RPAD('x', 5, 'ab')",
			expected: "xabab",
		},
		{
			name:     "zero length",
			query:    "SELECT RPAD('hello', 0, '0')",
			expected: "",
		},
		{
			name:     "unicode pad",
			query:    "SELECT RPAD('test', 6, '😀')",
			expected: "test😀😀",
		},
		{
			name:     "null input",
			query:    "SELECT RPAD(NULL, 5, '0')",
			expected: nil,
		},
		{
			name:     "null pad",
			query:    "SELECT RPAD('hi', 5, NULL)",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result sql.NullString
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if tt.expected == nil {
				if result.Valid {
					t.Errorf("Expected NULL, got %v", result.String)
				}
			} else {
				if !result.Valid {
					t.Errorf("Expected %v, got NULL", tt.expected)
				} else if result.String != tt.expected {
					t.Errorf("Expected %v, got %v", tt.expected, result.String)
				}
			}
		})
	}
}

func TestMySQLRepeat(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected interface{}
	}{
		{
			name:     "basic repeat",
			query:    "SELECT REPEAT('ab', 3)",
			expected: "ababab",
		},
		{
			name:     "zero count",
			query:    "SELECT REPEAT('test', 0)",
			expected: "",
		},
		{
			name:     "negative count",
			query:    "SELECT REPEAT('test', -1)",
			expected: "",
		},
		{
			name:     "single repeat",
			query:    "SELECT REPEAT('x', 1)",
			expected: "x",
		},
		{
			name:     "unicode",
			query:    "SELECT REPEAT('😀', 3)",
			expected: "😀😀😀",
		},
		{
			name:     "empty string",
			query:    "SELECT REPEAT('', 5)",
			expected: "",
		},
		{
			name:     "null input",
			query:    "SELECT REPEAT(NULL, 3)",
			expected: nil,
		},
		{
			name:     "null count",
			query:    "SELECT REPEAT('test', NULL)",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result sql.NullString
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if tt.expected == nil {
				if result.Valid {
					t.Errorf("Expected NULL, got %v", result.String)
				}
			} else {
				if !result.Valid {
					t.Errorf("Expected %v, got NULL", tt.expected)
				} else if result.String != tt.expected {
					t.Errorf("Expected %v, got %v", tt.expected, result.String)
				}
			}
		})
	}
}

func TestMySQLFindInSet(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected int64
	}{
		{
			name:     "found at position 2",
			query:    "SELECT FIND_IN_SET('b', 'a,b,c')",
			expected: 2,
		},
		{
			name:     "found at position 1",
			query:    "SELECT FIND_IN_SET('a', 'a,b,c')",
			expected: 1,
		},
		{
			name:     "found at position 3",
			query:    "SELECT FIND_IN_SET('c', 'a,b,c')",
			expected: 3,
		},
		{
			name:     "not found",
			query:    "SELECT FIND_IN_SET('d', 'a,b,c')",
			expected: 0,
		},
		{
			name:     "empty list",
			query:    "SELECT FIND_IN_SET('a', '')",
			expected: 0,
		},
		{
			name:     "single item match",
			query:    "SELECT FIND_IN_SET('x', 'x')",
			expected: 1,
		},
		{
			name:     "single item no match",
			query:    "SELECT FIND_IN_SET('y', 'x')",
			expected: 0,
		},
		{
			name:     "empty string in list",
			query:    "SELECT FIND_IN_SET('', 'a,,c')",
			expected: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result sql.NullInt64
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if !result.Valid {
				t.Errorf("Expected %v, got NULL", tt.expected)
			} else if result.Int64 != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result.Int64)
			}
		})
	}
}

func TestMySQLField(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected int64
	}{
		{
			name:     "found at position 2",
			query:    "SELECT FIELD('b', 'a', 'b', 'c')",
			expected: 2,
		},
		{
			name:     "found at position 1",
			query:    "SELECT FIELD('a', 'a', 'b', 'c')",
			expected: 1,
		},
		{
			name:     "not found",
			query:    "SELECT FIELD('d', 'a', 'b', 'c')",
			expected: 0,
		},
		{
			name:     "numeric value",
			query:    "SELECT FIELD(2, 1, 2, 3)",
			expected: 2,
		},
		{
			name:     "single item match",
			query:    "SELECT FIELD('x', 'x')",
			expected: 1,
		},
		{
			name:     "single item no match",
			query:    "SELECT FIELD('y', 'x')",
			expected: 0,
		},
		{
			name:     "null needle",
			query:    "SELECT FIELD(NULL, 'a', 'b')",
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result int64
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestMySQLElt(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected interface{}
	}{
		{
			name:     "first element",
			query:    "SELECT ELT(1, 'a', 'b', 'c')",
			expected: "a",
		},
		{
			name:     "second element",
			query:    "SELECT ELT(2, 'a', 'b', 'c')",
			expected: "b",
		},
		{
			name:     "third element",
			query:    "SELECT ELT(3, 'a', 'b', 'c')",
			expected: "c",
		},
		{
			name:     "out of range",
			query:    "SELECT ELT(4, 'a', 'b', 'c')",
			expected: nil,
		},
		{
			name:     "zero index",
			query:    "SELECT ELT(0, 'a', 'b', 'c')",
			expected: nil,
		},
		{
			name:     "negative index",
			query:    "SELECT ELT(-1, 'a', 'b', 'c')",
			expected: nil,
		},
		{
			name:     "null index",
			query:    "SELECT ELT(NULL, 'a', 'b', 'c')",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result sql.NullString
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if tt.expected == nil {
				if result.Valid {
					t.Errorf("Expected NULL, got %v", result.String)
				}
			} else {
				if !result.Valid {
					t.Errorf("Expected %v, got NULL", tt.expected)
				} else if result.String != tt.expected {
					t.Errorf("Expected %v, got %v", tt.expected, result.String)
				}
			}
		})
	}
}

func TestMySQLSubstringIndex(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected interface{}
	}{
		{
			name:     "positive count 2",
			query:    "SELECT SUBSTRING_INDEX('a.b.c', '.', 2)",
			expected: "a.b",
		},
		{
			name:     "positive count 1",
			query:    "SELECT SUBSTRING_INDEX('a.b.c', '.', 1)",
			expected: "a",
		},
		{
			name:     "negative count -1",
			query:    "SELECT SUBSTRING_INDEX('a.b.c', '.', -1)",
			expected: "c",
		},
		{
			name:     "negative count -2",
			query:    "SELECT SUBSTRING_INDEX('a.b.c', '.', -2)",
			expected: "b.c",
		},
		{
			name:     "count exceeds occurrences",
			query:    "SELECT SUBSTRING_INDEX('a.b.c', '.', 10)",
			expected: "a.b.c",
		},
		{
			name:     "negative count exceeds",
			query:    "SELECT SUBSTRING_INDEX('a.b.c', '.', -10)",
			expected: "a.b.c",
		},
		{
			name:     "zero count",
			query:    "SELECT SUBSTRING_INDEX('a.b.c', '.', 0)",
			expected: "",
		},
		{
			name:     "delimiter not found",
			query:    "SELECT SUBSTRING_INDEX('abc', '.', 1)",
			expected: "abc",
		},
		{
			name:     "multi-char delimiter",
			query:    "SELECT SUBSTRING_INDEX('one::two::three', '::', 2)",
			expected: "one::two",
		},
		{
			name:     "null input",
			query:    "SELECT SUBSTRING_INDEX(NULL, '.', 1)",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result sql.NullString
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if tt.expected == nil {
				if result.Valid {
					t.Errorf("Expected NULL, got %v", result.String)
				}
			} else {
				if !result.Valid {
					t.Errorf("Expected %v, got NULL", tt.expected)
				} else if result.String != tt.expected {
					t.Errorf("Expected %v, got %v", tt.expected, result.String)
				}
			}
		})
	}
}

func TestMySQLIf(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "true condition",
			query:    "SELECT IF(1, 'yes', 'no')",
			expected: "yes",
		},
		{
			name:     "false condition",
			query:    "SELECT IF(0, 'yes', 'no')",
			expected: "no",
		},
		{
			name:     "comparison true",
			query:    "SELECT IF(5 > 3, 'greater', 'less')",
			expected: "greater",
		},
		{
			name:     "comparison false",
			query:    "SELECT IF(3 > 5, 'greater', 'less')",
			expected: "less",
		},
		{
			name:     "null condition",
			query:    "SELECT IF(NULL, 'yes', 'no')",
			expected: "no",
		},
		{
			name:     "empty string condition",
			query:    "SELECT IF('', 'yes', 'no')",
			expected: "no",
		},
		{
			name:     "non-empty string condition",
			query:    "SELECT IF('text', 'yes', 'no')",
			expected: "yes",
		},
		{
			name:     "negative number",
			query:    "SELECT IF(-1, 'yes', 'no')",
			expected: "yes",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result string
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestMySQLIfNull(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Note: IFNULL is natively supported by SQLite and is MySQL-compatible
	// We don't need to implement it - this test verifies it works correctly

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "null value",
			query:    "SELECT IFNULL(NULL, 'default')",
			expected: "default",
		},
		{
			name:     "non-null value",
			query:    "SELECT IFNULL('value', 'default')",
			expected: "value",
		},
		{
			name:     "empty string is not null",
			query:    "SELECT IFNULL('', 'default')",
			expected: "",
		},
		{
			name:     "zero is not null",
			query:    "SELECT IFNULL(0, 999)",
			expected: "0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result string
			err := db.QueryRow(tt.query).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestMySQLStringFunctionsIntegration(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	_, err := db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			first_name TEXT,
			last_name TEXT,
			email TEXT,
			tags TEXT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	testData := []struct {
		id        int
		firstName string
		lastName  string
		email     string
		tags      string
	}{
		{1, "Alice", "Smith", "alice@example.com", "admin,user,verified"},
		{2, "Bob", "Jones", "bob@test.org", "user,pending"},
		{3, "Charlie", "Brown", "charlie@example.net", "user,verified,premium"},
	}

	for _, data := range testData {
		_, err = db.Exec(
			"INSERT INTO users (id, first_name, last_name, email, tags) VALUES (?, ?, ?, ?, ?)",
			data.id, data.firstName, data.lastName, data.email, data.tags,
		)
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	t.Run("concat_ws for full names", func(t *testing.T) {
		rows, err := db.Query("SELECT CONCAT_WS(' ', first_name, last_name) as full_name FROM users ORDER BY id")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		expected := []string{"Alice Smith", "Bob Jones", "Charlie Brown"}
		i := 0
		for rows.Next() {
			var fullName string
			if err := rows.Scan(&fullName); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			if fullName != expected[i] {
				t.Errorf("Row %d: expected %s, got %s", i, expected[i], fullName)
			}
			i++
		}
	})

	t.Run("find_in_set for tag filtering", func(t *testing.T) {
		var count int
		err := db.QueryRow("SELECT COUNT(*) FROM users WHERE FIND_IN_SET('verified', tags) > 0").Scan(&count)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if count != 2 {
			t.Errorf("Expected 2 verified users, got %d", count)
		}
	})

	t.Run("substring_index for domain extraction", func(t *testing.T) {
		var domain string
		err := db.QueryRow(
			"SELECT SUBSTRING_INDEX(email, '@', -1) FROM users WHERE id = 1",
		).Scan(&domain)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		if domain != "example.com" {
			t.Errorf("Expected example.com, got %s", domain)
		}
	})

	t.Run("left for initials", func(t *testing.T) {
		rows, err := db.Query(
			"SELECT CONCAT_WS('', LEFT(first_name, 1), LEFT(last_name, 1)) as initials FROM users ORDER BY id",
		)
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		defer rows.Close()

		expected := []string{"AS", "BJ", "CB"}
		i := 0
		for rows.Next() {
			var initials string
			if err := rows.Scan(&initials); err != nil {
				t.Fatalf("Scan failed: %v", err)
			}
			if initials != expected[i] {
				t.Errorf("Row %d: expected %s, got %s", i, expected[i], initials)
			}
			i++
		}
	})
}
