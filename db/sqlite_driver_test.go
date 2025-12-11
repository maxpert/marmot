package db

import (
	"database/sql"
	"testing"
)

// TestRegexpBasicMatch tests basic REGEXP functionality
func TestRegexpBasicMatch(t *testing.T) {
	db, err := sql.Open(SQLiteDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		text     string
		pattern  string
		expected bool
	}{
		{
			name:     "simple prefix match",
			text:     "hello",
			pattern:  "^h",
			expected: true,
		},
		{
			name:     "prefix no match",
			text:     "hello",
			pattern:  "^a",
			expected: false,
		},
		{
			name:     "suffix match",
			text:     "world",
			pattern:  "ld$",
			expected: true,
		},
		{
			name:     "contains match",
			text:     "hello world",
			pattern:  "lo wo",
			expected: true,
		},
		{
			name:     "contains no match",
			text:     "hello world",
			pattern:  "xyz",
			expected: false,
		},
		{
			name:     "digit pattern match",
			text:     "user123",
			pattern:  "[0-9]+",
			expected: true,
		},
		{
			name:     "digit pattern no match",
			text:     "username",
			pattern:  "[0-9]+",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result bool
			query := "SELECT ? REGEXP ?"
			err := db.QueryRow(query, tt.text, tt.pattern).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected REGEXP result %v for text='%s' pattern='%s', got %v",
					tt.expected, tt.text, tt.pattern, result)
			}
		})
	}
}

// TestRegexpCaseSensitivity tests case sensitivity behavior
func TestRegexpCaseSensitivity(t *testing.T) {
	db, err := sql.Open(SQLiteDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		text     string
		pattern  string
		expected bool
	}{
		{
			name:     "exact case match",
			text:     "Hello",
			pattern:  "Hello",
			expected: true,
		},
		{
			name:     "different case no match",
			text:     "Hello",
			pattern:  "hello",
			expected: false,
		},
		{
			name:     "case insensitive pattern",
			text:     "Hello",
			pattern:  "(?i)hello",
			expected: true,
		},
		{
			name:     "uppercase pattern on lowercase text",
			text:     "hello",
			pattern:  "HELLO",
			expected: false,
		},
		{
			name:     "case insensitive with anchors",
			text:     "WORLD",
			pattern:  "(?i)^world$",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result bool
			query := "SELECT ? REGEXP ?"
			err := db.QueryRow(query, tt.text, tt.pattern).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected REGEXP result %v for text='%s' pattern='%s', got %v",
					tt.expected, tt.text, tt.pattern, result)
			}
		})
	}
}

// TestRegexpWithTable tests REGEXP in WHERE clause with table data
func TestRegexpWithTable(t *testing.T) {
	db, err := sql.Open(SQLiteDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			email TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	testData := []struct {
		id    int
		email string
	}{
		{1, "alice@example.com"},
		{2, "bob@test.org"},
		{3, "charlie@example.net"},
		{4, "dave@example.com"},
	}

	for _, data := range testData {
		_, err = db.Exec("INSERT INTO users (id, email) VALUES (?, ?)", data.id, data.email)
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	// Test: Find all emails from example.com domain
	rows, err := db.Query("SELECT id, email FROM users WHERE email REGEXP ?", "@example\\.com$")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	var count int
	var foundIDs []int
	for rows.Next() {
		var id int
		var email string
		if err := rows.Scan(&id, &email); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		count++
		foundIDs = append(foundIDs, id)
	}

	// Should find alice and dave (ids 1 and 4)
	if count != 2 {
		t.Errorf("Expected 2 matches, got %d", count)
	}

	expectedIDs := map[int]bool{1: true, 4: true}
	for _, id := range foundIDs {
		if !expectedIDs[id] {
			t.Errorf("Unexpected ID %d in results", id)
		}
	}

	t.Logf("Found %d users with @example.com emails (IDs: %v)", count, foundIDs)
}

// TestRegexpComplexPatterns tests more complex regex patterns
func TestRegexpComplexPatterns(t *testing.T) {
	db, err := sql.Open(SQLiteDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		text     string
		pattern  string
		expected bool
	}{
		{
			name:     "email pattern match",
			text:     "user@example.com",
			pattern:  "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$",
			expected: true,
		},
		{
			name:     "invalid email",
			text:     "notanemail",
			pattern:  "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$",
			expected: false,
		},
		{
			name:     "phone pattern match",
			text:     "123-456-7890",
			pattern:  "^\\d{3}-\\d{3}-\\d{4}$",
			expected: true,
		},
		{
			name:     "invalid phone",
			text:     "123-45-6789",
			pattern:  "^\\d{3}-\\d{3}-\\d{4}$",
			expected: false,
		},
		{
			name:     "word boundary",
			text:     "hello world",
			pattern:  "\\bhello\\b",
			expected: true,
		},
		{
			name:     "alternation match",
			text:     "cat",
			pattern:  "^(cat|dog|bird)$",
			expected: true,
		},
		{
			name:     "alternation no match",
			text:     "fish",
			pattern:  "^(cat|dog|bird)$",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result bool
			query := "SELECT ? REGEXP ?"
			err := db.QueryRow(query, tt.text, tt.pattern).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected REGEXP result %v for text='%s' pattern='%s', got %v",
					tt.expected, tt.text, tt.pattern, result)
			}
		})
	}
}

// TestRegexpErrorHandling tests behavior with invalid patterns
func TestRegexpErrorHandling(t *testing.T) {
	db, err := sql.Open(SQLiteDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Invalid regex patterns should return error
	invalidPatterns := []string{
		"[",    // Unclosed bracket
		"(?P<", // Invalid named group
		"*",    // Invalid quantifier
		"(?",   // Incomplete group
	}

	for _, pattern := range invalidPatterns {
		var result bool
		query := "SELECT ? REGEXP ?"
		err := db.QueryRow(query, "test", pattern).Scan(&result)
		if err == nil {
			t.Errorf("Expected error for invalid pattern '%s', but got none", pattern)
		}
		t.Logf("Invalid pattern '%s' correctly returned error: %v", pattern, err)
	}
}
