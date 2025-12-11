package protocol

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// setupTestDB creates an in-memory SQLite database with test schema
func setupTestDB(t *testing.T) *sql.DB {
	db := openTestDB(t, ":memory:")

	// Create comprehensive test schema
	schema := `
		CREATE TABLE users (
			id INTEGER PRIMARY KEY, 
			name TEXT, 
			email TEXT, 
			status TEXT,
			age INTEGER,
			created_at TEXT
		);
		CREATE TABLE orders (
			id INTEGER PRIMARY KEY, 
			user_id INTEGER, 
			total REAL, 
			product_id INTEGER,
			created_at TEXT
		);
		CREATE TABLE products (
			id INTEGER PRIMARY KEY, 
			name TEXT, 
			price REAL, 
			category TEXT,
			stock INTEGER
		);
		CREATE TABLE items (
			id INTEGER PRIMARY KEY, 
			order_id INTEGER, 
			product_id INTEGER, 
			quantity INTEGER,
			price REAL
		);
		CREATE TABLE logs (
			id INTEGER PRIMARY KEY,
			message TEXT,
			level TEXT,
			created_at TEXT
		);
		CREATE TABLE metrics (
			id INTEGER PRIMARY KEY,
			name TEXT,
			value REAL,
			timestamp TEXT
		);
		CREATE TABLE sessions (
			id INTEGER PRIMARY KEY,
			user_id INTEGER,
			token TEXT,
			expires_at TEXT
		);
	`

	_, err := db.Exec(schema)
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	return db
}

// TestParser_SQLiteExecution_BatchInserts validates batch INSERT operations
func TestParser_SQLiteExecution_BatchInserts(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	tests := []struct {
		name       string
		mysqlQuery string
		verifyFunc func(*testing.T, *sql.DB)
	}{
		{
			name:       "Batch INSERT 10 rows",
			mysqlQuery: "INSERT INTO users (id, name, email, status) VALUES (1, 'alice', 'a@ex.com', 'active'), (2, 'bob', 'b@ex.com', 'active'), (3, 'charlie', 'c@ex.com', 'inactive'), (4, 'dave', 'd@ex.com', 'active'), (5, 'eve', 'e@ex.com', 'active'), (6, 'frank', 'f@ex.com', 'inactive'), (7, 'grace', 'g@ex.com', 'active'), (8, 'henry', 'h@ex.com', 'active'), (9, 'iris', 'i@ex.com', 'inactive'), (10, 'jack', 'j@ex.com', 'active')",
			verifyFunc: func(t *testing.T, db *sql.DB) {
				var count int
				err := db.QueryRow("SELECT COUNT(*) FROM users").Scan(&count)
				if err != nil || count != 10 {
					t.Errorf("Expected 10 users, got %d", count)
				}
			},
		},
		{
			name:       "Batch INSERT with qualified name",
			mysqlQuery: "INSERT INTO testdb.products (id, name, price, category) VALUES (1, 'Laptop', 999.99, 'electronics'), (2, 'Mouse', 29.99, 'electronics'), (3, 'Desk', 299.99, 'furniture')",
			verifyFunc: func(t *testing.T, db *sql.DB) {
				var count int
				err := db.QueryRow("SELECT COUNT(*) FROM products WHERE category = 'electronics'").Scan(&count)
				if err != nil || count != 2 {
					t.Errorf("Expected 2 electronics, got %d", count)
				}
			},
		},
		{
			name:       "Batch INSERT with consistency hint",
			mysqlQuery: "/*+ CONSISTENCY(QUORUM) */ INSERT INTO orders (id, user_id, total, product_id) VALUES (1, 1, 999.99, 1), (2, 2, 29.99, 2), (3, 1, 299.99, 3)",
			verifyFunc: func(t *testing.T, db *sql.DB) {
				var sum float64
				err := db.QueryRow("SELECT SUM(total) FROM orders").Scan(&sum)
				if err != nil || sum < 1329.97 {
					t.Errorf("Expected sum >= 1329.97, got %f", sum)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.mysqlQuery)

			// Execute parsed SQL in SQLite
			_, err := db.Exec(stmt.SQL)
			if err != nil {
				t.Fatalf("Failed to execute: %v\nSQL: %s", err, stmt.SQL)
			}

			// Verify results
			if tt.verifyFunc != nil {
				tt.verifyFunc(t, db)
			}
		})
	}
}

// TestParser_SQLiteExecution_Updates validates UPDATE operations
func TestParser_SQLiteExecution_Updates(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Insert test data
	_, _ = db.Exec("INSERT INTO users (id, name, email, status) VALUES (1, 'alice', 'a@ex.com', 'active'), (2, 'bob', 'b@ex.com', 'inactive')")

	tests := []struct {
		name       string
		mysqlQuery string
		verifyFunc func(*testing.T, *sql.DB)
	}{
		{
			name:       "Simple UPDATE",
			mysqlQuery: "UPDATE users SET status = 'inactive' WHERE id = 1",
			verifyFunc: func(t *testing.T, db *sql.DB) {
				var status string
				_ = db.QueryRow("SELECT status FROM users WHERE id = 1").Scan(&status)
				if status != "inactive" {
					t.Errorf("Expected status='inactive', got '%s'", status)
				}
			},
		},
		{
			name:       "UPDATE with qualified name",
			mysqlQuery: "UPDATE mydb.users SET email = 'newemail@ex.com' WHERE id = 2",
			verifyFunc: func(t *testing.T, db *sql.DB) {
				var email string
				_ = db.QueryRow("SELECT email FROM users WHERE id = 2").Scan(&email)
				if email != "newemail@ex.com" {
					t.Errorf("Expected new email, got '%s'", email)
				}
			},
		},
		{
			name:       "UPDATE with consistency hint",
			mysqlQuery: "/*+ CONSISTENCY(ALL) */ UPDATE users SET status = 'active' WHERE status = 'inactive'",
			verifyFunc: func(t *testing.T, db *sql.DB) {
				var count int
				_ = db.QueryRow("SELECT COUNT(*) FROM users WHERE status = 'active'").Scan(&count)
				if count != 2 {
					t.Errorf("Expected 2 active users, got %d", count)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt := ParseStatement(tt.mysqlQuery)

			_, err := db.Exec(stmt.SQL)
			if err != nil {
				t.Fatalf("Failed to execute: %v\nSQL: %s", err, stmt.SQL)
			}

			if tt.verifyFunc != nil {
				tt.verifyFunc(t, db)
			}
		})
	}
}

// Continue in next file due to size...
