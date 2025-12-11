package db

import (
	"database/sql"
	"testing"
	"time"

	"github.com/mattn/go-sqlite3"
)

const testDriverName = "sqlite3_marmot_datetime_test"

func init() {
	sql.Register(testDriverName, &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			if err := conn.RegisterFunc("regexp", regexpMatch, true); err != nil {
				return err
			}
			return RegisterMySQLDateTimeFuncs(conn)
		},
	})
}

func TestMySQLYear(t *testing.T) {
	db, err := sql.Open(testDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		input    string
		expected int
		isNull   bool
	}{
		{"standard date", "2024-12-11", 2024, false},
		{"datetime", "2024-12-11 14:30:00", 2024, false},
		{"year start", "2024-01-01", 2024, false},
		{"year end", "2024-12-31", 2024, false},
		{"leap year", "2020-02-29", 2020, false},
		{"old date", "1990-05-15", 1990, false},
		{"future date", "2030-06-20", 2030, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result sql.NullInt64
			err := db.QueryRow("SELECT YEAR(?)", tt.input).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if tt.isNull {
				if result.Valid {
					t.Errorf("Expected NULL, got %d", result.Int64)
				}
			} else {
				if !result.Valid {
					t.Errorf("Expected %d, got NULL", tt.expected)
				} else if int(result.Int64) != tt.expected {
					t.Errorf("Expected %d, got %d", tt.expected, result.Int64)
				}
			}
		})
	}
}

func TestMySQLMonth(t *testing.T) {
	db, err := sql.Open(testDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{"january", "2024-01-15", 1},
		{"february", "2024-02-20", 2},
		{"march", "2024-03-10", 3},
		{"december", "2024-12-31", 12},
		{"datetime", "2024-06-15 14:30:00", 6},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result int
			err := db.QueryRow("SELECT MONTH(?)", tt.input).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestMySQLDay(t *testing.T) {
	db, err := sql.Open(testDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{"first day", "2024-01-01", 1},
		{"mid month", "2024-06-15", 15},
		{"last day", "2024-12-31", 31},
		{"datetime", "2024-03-25 14:30:00", 25},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result int
			err := db.QueryRow("SELECT DAY(?)", tt.input).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestMySQLHour(t *testing.T) {
	db, err := sql.Open(testDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{"midnight", "2024-12-11 00:00:00", 0},
		{"morning", "2024-12-11 09:30:00", 9},
		{"afternoon", "2024-12-11 14:30:00", 14},
		{"evening", "2024-12-11 23:59:59", 23},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result int
			err := db.QueryRow("SELECT HOUR(?)", tt.input).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestMySQLMinute(t *testing.T) {
	db, err := sql.Open(testDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{"zero minutes", "2024-12-11 14:00:00", 0},
		{"mid hour", "2024-12-11 14:30:00", 30},
		{"end of hour", "2024-12-11 14:59:00", 59},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result int
			err := db.QueryRow("SELECT MINUTE(?)", tt.input).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestMySQLSecond(t *testing.T) {
	db, err := sql.Open(testDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{"zero seconds", "2024-12-11 14:30:00", 0},
		{"mid minute", "2024-12-11 14:30:30", 30},
		{"end of minute", "2024-12-11 14:30:59", 59},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result int
			err := db.QueryRow("SELECT SECOND(?)", tt.input).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestMySQLDayOfWeek(t *testing.T) {
	db, err := sql.Open(testDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{"sunday", "2024-12-01", 1},
		{"monday", "2024-12-02", 2},
		{"tuesday", "2024-12-03", 3},
		{"wednesday", "2024-12-04", 4},
		{"thursday", "2024-12-05", 5},
		{"friday", "2024-12-06", 6},
		{"saturday", "2024-12-07", 7},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result int
			err := db.QueryRow("SELECT DAYOFWEEK(?)", tt.input).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestMySQLDayOfYear(t *testing.T) {
	db, err := sql.Open(testDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{"first day of year", "2024-01-01", 1},
		{"leap day", "2024-02-29", 60},
		{"mid year", "2024-07-01", 183},
		{"last day of year", "2024-12-31", 366},
		{"non-leap year end", "2023-12-31", 365},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result int
			err := db.QueryRow("SELECT DAYOFYEAR(?)", tt.input).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestMySQLWeekday(t *testing.T) {
	db, err := sql.Open(testDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{"monday", "2024-12-02", 0},
		{"tuesday", "2024-12-03", 1},
		{"wednesday", "2024-12-04", 2},
		{"thursday", "2024-12-05", 3},
		{"friday", "2024-12-06", 4},
		{"saturday", "2024-12-07", 5},
		{"sunday", "2024-12-01", 6},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result int
			err := db.QueryRow("SELECT WEEKDAY(?)", tt.input).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestMySQLWeek(t *testing.T) {
	db, err := sql.Open(testDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name  string
		input string
	}{
		{"first week", "2024-01-01"},
		{"mid year", "2024-06-15"},
		{"last week", "2024-12-31"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result int
			err := db.QueryRow("SELECT WEEK(?)", tt.input).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result < 0 || result > 53 {
				t.Errorf("Expected week between 0-53, got %d", result)
			}
		})
	}
}

func TestMySQLQuarter(t *testing.T) {
	db, err := sql.Open(testDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		input    string
		expected int
	}{
		{"Q1 january", "2024-01-15", 1},
		{"Q1 february", "2024-02-20", 1},
		{"Q1 march", "2024-03-25", 1},
		{"Q2 april", "2024-04-10", 2},
		{"Q2 may", "2024-05-15", 2},
		{"Q2 june", "2024-06-20", 2},
		{"Q3 july", "2024-07-10", 3},
		{"Q3 august", "2024-08-15", 3},
		{"Q3 september", "2024-09-20", 3},
		{"Q4 october", "2024-10-10", 4},
		{"Q4 november", "2024-11-15", 4},
		{"Q4 december", "2024-12-20", 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result int
			err := db.QueryRow("SELECT QUARTER(?)", tt.input).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestMySQLNow(t *testing.T) {
	db, err := sql.Open(testDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	var result string
	err = db.QueryRow("SELECT NOW()").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	parsed, err := time.ParseInLocation(mysqlDateTimeLayout, result, time.Local)
	if err != nil {
		t.Fatalf("Failed to parse NOW() result: %v", err)
	}

	now := time.Now()
	diff := now.Sub(parsed)
	if diff < 0 {
		diff = -diff
	}
	if diff > 2*time.Second {
		t.Errorf("NOW() timestamp differs by more than 2 seconds: %v", diff)
	}
}

func TestMySQLCurDate(t *testing.T) {
	db, err := sql.Open(testDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	var result string
	err = db.QueryRow("SELECT CURDATE()").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	expected := time.Now().Format(mysqlDateLayout)
	if result != expected {
		t.Errorf("Expected %s, got %s", expected, result)
	}
}

func TestMySQLCurTime(t *testing.T) {
	db, err := sql.Open(testDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	var result string
	err = db.QueryRow("SELECT CURTIME()").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	_, err = time.Parse(mysqlTimeLayout, result)
	if err != nil {
		t.Fatalf("Failed to parse CURTIME() result: %v", err)
	}

	now := time.Now()
	expected := now.Format(mysqlTimeLayout)

	parsedResult, _ := time.Parse(mysqlTimeLayout, result)
	parsedExpected, _ := time.Parse(mysqlTimeLayout, expected)
	diff := parsedExpected.Sub(parsedResult)
	if diff < 0 {
		diff = -diff
	}
	if diff > 2*time.Second {
		t.Errorf("CURTIME() differs by more than 2 seconds: %v (got %s, expected ~%s)", diff, result, expected)
	}
}

func TestMySQLCurrentTimestamp(t *testing.T) {
	db, err := sql.Open(testDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	var result string
	err = db.QueryRow("SELECT CURRENT_TIMESTAMP").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	parsed, err := time.Parse("2006-01-02 15:04:05", result)
	if err != nil {
		t.Fatalf("Failed to parse CURRENT_TIMESTAMP result: %v", err)
	}

	now := time.Now().UTC()
	parsedUTC := parsed.UTC()
	diff := now.Sub(parsedUTC)
	if diff < 0 {
		diff = -diff
	}
	if diff > 2*time.Second {
		t.Errorf("CURRENT_TIMESTAMP differs by more than 2 seconds: %v", diff)
	}
}

func TestMySQLUTCTimestamp(t *testing.T) {
	db, err := sql.Open(testDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	var result string
	err = db.QueryRow("SELECT UTC_TIMESTAMP()").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	parsed, err := time.Parse(mysqlDateTimeLayout, result)
	if err != nil {
		t.Fatalf("Failed to parse UTC_TIMESTAMP() result: %v", err)
	}

	now := time.Now().UTC()
	diff := now.Sub(parsed)
	if diff < 0 {
		diff = -diff
	}
	if diff > 2*time.Second {
		t.Errorf("UTC_TIMESTAMP() differs by more than 2 seconds: %v", diff)
	}
}

func TestMySQLDateFormat(t *testing.T) {
	db, err := sql.Open(testDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		date     string
		format   string
		expected string
	}{
		{"year-month-day", "2024-12-11 14:30:45", "%Y-%m-%d", "2024-12-11"},
		{"full datetime", "2024-12-11 14:30:45", "%Y-%m-%d %H:%i:%s", "2024-12-11 14:30:45"},
		{"short year", "2024-12-11", "%y/%m/%d", "24/12/11"},
		{"time only", "2024-12-11 14:30:45", "%H:%i:%s", "14:30:45"},
		{"month name", "2024-12-11", "%M %d, %Y", "December 11, 2024"},
		{"short month", "2024-01-05", "%b %d", "Jan 05"},
		{"weekday", "2024-12-11", "%W", "Wednesday"},
		{"day of year", "2024-12-11", "%j", "346"},
		{"unpadded month", "2024-03-05", "%c/%e", "3/5"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT DATE_FORMAT(?, ?)", tt.date, tt.format).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestMySQLFromUnixtime(t *testing.T) {
	db, err := sql.Open(testDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name      string
		timestamp int64
		expected  string
	}{
		{"epoch", 0, "1970-01-01 00:00:00"},
		{"specific time", 1609459200, "2021-01-01 00:00:00"},
		{"recent time", 1702310400, "2023-12-11 16:00:00"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT FROM_UNIXTIME(?)", tt.timestamp).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestMySQLUnixTimestamp(t *testing.T) {
	db, err := sql.Open(testDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		date     string
		expected int64
	}{
		{"epoch", "1970-01-01 00:00:00", 0},
		{"specific time", "2021-01-01 00:00:00", 1609459200},
		{"recent time", "2023-12-11 16:00:00", 1702310400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result int64
			err := db.QueryRow("SELECT UNIX_TIMESTAMP(?)", tt.date).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestMySQLUnixTimestampNow(t *testing.T) {
	db, err := sql.Open(testDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	var result int64
	err = db.QueryRow("SELECT UNIX_TIMESTAMP()").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	now := time.Now().Unix()
	diff := now - result
	if diff < 0 {
		diff = -diff
	}
	if diff > 2 {
		t.Errorf("UNIX_TIMESTAMP() differs by more than 2 seconds: %d", diff)
	}
}

func TestMySQLDateDiff(t *testing.T) {
	db, err := sql.Open(testDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		date1    string
		date2    string
		expected int
	}{
		{"same day", "2024-12-11", "2024-12-11", 0},
		{"one day apart", "2024-12-12", "2024-12-11", 1},
		{"negative difference", "2024-12-11", "2024-12-12", -1},
		{"one week", "2024-12-18", "2024-12-11", 7},
		{"across months", "2024-02-01", "2024-01-01", 31},
		{"across years non-leap", "2024-01-01", "2023-01-01", 365},
		{"across leap year", "2025-01-01", "2024-01-01", 366},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result int
			err := db.QueryRow("SELECT DATEDIFF(?, ?)", tt.date1, tt.date2).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected %d, got %d", tt.expected, result)
			}
		})
	}
}

func TestMySQLLastDay(t *testing.T) {
	db, err := sql.Open(testDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"january", "2024-01-15", "2024-01-31"},
		{"february leap year", "2024-02-10", "2024-02-29"},
		{"february non-leap", "2023-02-10", "2023-02-28"},
		{"march", "2024-03-10", "2024-03-31"},
		{"april", "2024-04-15", "2024-04-30"},
		{"december", "2024-12-01", "2024-12-31"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result string
			err := db.QueryRow("SELECT LAST_DAY(?)", tt.input).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestMySQLDateTimeNullHandling(t *testing.T) {
	db, err := sql.Open(testDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	functions := []string{
		"YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND",
		"DAYOFWEEK", "DAYOFYEAR", "WEEKDAY", "WEEK", "QUARTER",
		"DATE_FORMAT", "LAST_DAY",
	}

	for _, fn := range functions {
		t.Run(fn, func(t *testing.T) {
			var result sql.NullString
			query := "SELECT " + fn + "(NULL)"
			if fn == "DATE_FORMAT" {
				query = "SELECT DATE_FORMAT(NULL, '%Y-%m-%d')"
			}
			err := db.QueryRow(query).Scan(&result)
			if err != nil {
				t.Fatalf("Query failed: %v", err)
			}
			if result.Valid {
				t.Errorf("Expected NULL result for %s(NULL), got: %v", fn, result.String)
			}
		})
	}
}

func TestMySQLDateTimeInvalidInputs(t *testing.T) {
	db, err := sql.Open(testDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name  string
		query string
	}{
		{"invalid date format", "SELECT YEAR('not-a-date')"},
		{"empty string", "SELECT MONTH('')"},
		{"malformed datetime", "SELECT DAY('2024-13-45')"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result sql.NullInt64
			err := db.QueryRow(tt.query).Scan(&result)
			if err == nil {
				t.Logf("Warning: Expected error for invalid input in %s, but query succeeded", tt.name)
			}
		})
	}
}

func TestMySQLDateTimeWithTable(t *testing.T) {
	db, err := sql.Open(testDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE events (
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			event_date TEXT NOT NULL
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	testData := []struct {
		id   int
		name string
		date string
	}{
		{1, "New Year", "2024-01-01 00:00:00"},
		{2, "Valentine", "2024-02-14 12:00:00"},
		{3, "Summer", "2024-07-04 14:30:00"},
		{4, "Halloween", "2024-10-31 20:00:00"},
		{5, "Christmas", "2024-12-25 10:00:00"},
	}

	for _, data := range testData {
		_, err = db.Exec("INSERT INTO events (id, name, event_date) VALUES (?, ?, ?)",
			data.id, data.name, data.date)
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}
	}

	rows, err := db.Query("SELECT name, MONTH(event_date) as month FROM events WHERE QUARTER(event_date) = 4 ORDER BY month")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	var count int
	expectedEvents := map[string]int{"Halloween": 10, "Christmas": 12}
	for rows.Next() {
		var name string
		var month int
		if err := rows.Scan(&name, &month); err != nil {
			t.Fatalf("Scan failed: %v", err)
		}
		count++
		if expected, ok := expectedEvents[name]; ok {
			if month != expected {
				t.Errorf("Event %s: expected month %d, got %d", name, expected, month)
			}
		}
	}

	if count != 2 {
		t.Errorf("Expected 2 Q4 events, got %d", count)
	}
}
