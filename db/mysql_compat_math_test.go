package db

import (
	"database/sql"
	"math"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func setupMathTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open(SQLiteDriverName, ":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}

	return db
}

func TestMySQLRand(t *testing.T) {
	db := setupMathTestDB(t)
	defer db.Close()

	var r1, r2 float64
	err := db.QueryRow("SELECT RAND()").Scan(&r1)
	if err != nil {
		t.Fatalf("Failed to query RAND(): %v", err)
	}

	err = db.QueryRow("SELECT RAND()").Scan(&r2)
	if err != nil {
		t.Fatalf("Failed to query RAND(): %v", err)
	}

	if r1 < 0 || r1 >= 1 {
		t.Errorf("RAND() = %f, want value in [0, 1)", r1)
	}

	if r2 < 0 || r2 >= 1 {
		t.Errorf("RAND() = %f, want value in [0, 1)", r2)
	}

	var s1, s2 float64
	err = db.QueryRow("SELECT RAND(42)").Scan(&s1)
	if err != nil {
		t.Fatalf("Failed to query RAND(42): %v", err)
	}

	err = db.QueryRow("SELECT RAND(42)").Scan(&s2)
	if err != nil {
		t.Fatalf("Failed to query RAND(42): %v", err)
	}

	if s1 != s2 {
		t.Errorf("RAND(42) should be deterministic, got %f and %f", s1, s2)
	}

	var s3 float64
	err = db.QueryRow("SELECT RAND(100)").Scan(&s3)
	if err != nil {
		t.Fatalf("Failed to query RAND(100): %v", err)
	}

	if s3 == s1 {
		t.Error("RAND(42) and RAND(100) should return different values")
	}
}

func TestMySQLSign(t *testing.T) {
	db := setupMathTestDB(t)
	defer db.Close()

	tests := []struct {
		input    float64
		expected int
	}{
		{-5.5, -1},
		{-0.1, -1},
		{0, 0},
		{0.1, 1},
		{5.5, 1},
		{-100, -1},
		{100, 1},
	}

	for _, tt := range tests {
		var result int
		err := db.QueryRow("SELECT SIGN(?)", tt.input).Scan(&result)
		if err != nil {
			t.Fatalf("Failed to query SIGN(%f): %v", tt.input, err)
		}

		if result != tt.expected {
			t.Errorf("SIGN(%f) = %d, want %d", tt.input, result, tt.expected)
		}
	}
}

func TestMySQLTruncate(t *testing.T) {
	db := setupMathTestDB(t)
	defer db.Close()

	tests := []struct {
		x        float64
		d        int64
		expected float64
	}{
		{1.999, 1, 1.9},
		{1.999, 0, 1.0},
		{1.999, 2, 1.99},
		{-1.999, 1, -1.9},
		{122.9, -2, 100.0},
		{10.28, 1, 10.2},
		{10.28, 0, 10.0},
	}

	for _, tt := range tests {
		var result float64
		err := db.QueryRow("SELECT TRUNCATE(?, ?)", tt.x, tt.d).Scan(&result)
		if err != nil {
			t.Fatalf("Failed to query TRUNCATE(%f, %d): %v", tt.x, tt.d, err)
		}

		if math.Abs(result-tt.expected) > 0.0001 {
			t.Errorf("TRUNCATE(%f, %d) = %f, want %f", tt.x, tt.d, result, tt.expected)
		}
	}
}

func TestMySQLDegrees(t *testing.T) {
	db := setupMathTestDB(t)
	defer db.Close()

	tests := []struct {
		radians  float64
		expected float64
	}{
		{math.Pi, 180.0},
		{math.Pi / 2, 90.0},
		{0, 0.0},
		{2 * math.Pi, 360.0},
	}

	for _, tt := range tests {
		var result float64
		err := db.QueryRow("SELECT DEGREES(?)", tt.radians).Scan(&result)
		if err != nil {
			t.Fatalf("Failed to query DEGREES(%f): %v", tt.radians, err)
		}

		if math.Abs(result-tt.expected) > 0.0001 {
			t.Errorf("DEGREES(%f) = %f, want %f", tt.radians, result, tt.expected)
		}
	}
}

func TestMySQLRadians(t *testing.T) {
	db := setupMathTestDB(t)
	defer db.Close()

	tests := []struct {
		degrees  float64
		expected float64
	}{
		{180.0, math.Pi},
		{90.0, math.Pi / 2},
		{0, 0.0},
		{360.0, 2 * math.Pi},
	}

	for _, tt := range tests {
		var result float64
		err := db.QueryRow("SELECT RADIANS(?)", tt.degrees).Scan(&result)
		if err != nil {
			t.Fatalf("Failed to query RADIANS(%f): %v", tt.degrees, err)
		}

		if math.Abs(result-tt.expected) > 0.0001 {
			t.Errorf("RADIANS(%f) = %f, want %f", tt.degrees, result, tt.expected)
		}
	}
}

func TestMySQLPi(t *testing.T) {
	db := setupMathTestDB(t)
	defer db.Close()

	var result float64
	err := db.QueryRow("SELECT PI()").Scan(&result)
	if err != nil {
		t.Fatalf("Failed to query PI(): %v", err)
	}

	if math.Abs(result-math.Pi) > 0.0001 {
		t.Errorf("PI() = %f, want %f", result, math.Pi)
	}
}

func TestMySQLCot(t *testing.T) {
	db := setupMathTestDB(t)
	defer db.Close()

	tests := []struct {
		input    float64
		expected float64
	}{
		{math.Pi / 4, 1.0},
		{math.Pi / 2, 0.0},
	}

	for _, tt := range tests {
		var result float64
		err := db.QueryRow("SELECT COT(?)", tt.input).Scan(&result)
		if err != nil {
			t.Fatalf("Failed to query COT(%f): %v", tt.input, err)
		}

		if math.Abs(result-tt.expected) > 0.0001 {
			t.Errorf("COT(%f) = %f, want %f", tt.input, result, tt.expected)
		}
	}
}

func TestMySQLLog(t *testing.T) {
	db := setupMathTestDB(t)
	defer db.Close()

	var result float64
	err := db.QueryRow("SELECT LOG(10)").Scan(&result)
	if err != nil {
		t.Fatalf("Failed to query LOG(10): %v", err)
	}

	expected := math.Log(10)
	if math.Abs(result-expected) > 0.0001 {
		t.Errorf("LOG(10) = %f, want %f", result, expected)
	}

	var result2 float64
	err = db.QueryRow("SELECT LOG(10, 100)").Scan(&result2)
	if err != nil {
		t.Fatalf("Failed to query LOG(10, 100): %v", err)
	}

	if math.Abs(result2-2.0) > 0.0001 {
		t.Errorf("LOG(10, 100) = %f, want 2.0", result2)
	}

	var nullResult sql.NullFloat64
	err = db.QueryRow("SELECT LOG(-1)").Scan(&nullResult)
	if err != nil {
		t.Fatalf("Failed to query LOG(-1): %v", err)
	}

	if nullResult.Valid {
		t.Error("LOG(-1) should return NULL")
	}
}

func TestMySQLLog2(t *testing.T) {
	db := setupMathTestDB(t)
	defer db.Close()

	tests := []struct {
		input    float64
		expected float64
	}{
		{8, 3.0},
		{16, 4.0},
		{2, 1.0},
		{1, 0.0},
	}

	for _, tt := range tests {
		var result float64
		err := db.QueryRow("SELECT LOG2(?)", tt.input).Scan(&result)
		if err != nil {
			t.Fatalf("Failed to query LOG2(%f): %v", tt.input, err)
		}

		if math.Abs(result-tt.expected) > 0.0001 {
			t.Errorf("LOG2(%f) = %f, want %f", tt.input, result, tt.expected)
		}
	}

	var nullResult sql.NullFloat64
	err := db.QueryRow("SELECT LOG2(-1)").Scan(&nullResult)
	if err != nil {
		t.Fatalf("Failed to query LOG2(-1): %v", err)
	}

	if nullResult.Valid {
		t.Error("LOG2(-1) should return NULL")
	}
}

func TestMySQLPow(t *testing.T) {
	db := setupMathTestDB(t)
	defer db.Close()

	tests := []struct {
		x        float64
		y        float64
		expected float64
	}{
		{2, 3, 8.0},
		{10, 2, 100.0},
		{5, 0, 1.0},
		{2, -1, 0.5},
	}

	for _, tt := range tests {
		var result float64
		err := db.QueryRow("SELECT POW(?, ?)", tt.x, tt.y).Scan(&result)
		if err != nil {
			t.Fatalf("Failed to query POW(%f, %f): %v", tt.x, tt.y, err)
		}

		if math.Abs(result-tt.expected) > 0.0001 {
			t.Errorf("POW(%f, %f) = %f, want %f", tt.x, tt.y, result, tt.expected)
		}
	}

	var result float64
	err := db.QueryRow("SELECT POWER(2, 3)").Scan(&result)
	if err != nil {
		t.Fatalf("Failed to query POWER(2, 3): %v", err)
	}

	if math.Abs(result-8.0) > 0.0001 {
		t.Errorf("POWER(2, 3) = %f, want 8.0", result)
	}
}

func TestMySQLMD5(t *testing.T) {
	db := setupMathTestDB(t)
	defer db.Close()

	tests := []struct {
		input    string
		expected string
	}{
		{"hello", "5d41402abc4b2a76b9719d911017c592"},
		{"", "d41d8cd98f00b204e9800998ecf8427e"},
		{"Hello World", "b10a8db164e0754105b7a99be72e3fe5"},
		{"test", "098f6bcd4621d373cade4e832627b4f6"},
	}

	for _, tt := range tests {
		var result string
		err := db.QueryRow("SELECT MD5(?)", tt.input).Scan(&result)
		if err != nil {
			t.Fatalf("Failed to query MD5(%q): %v", tt.input, err)
		}

		if result != tt.expected {
			t.Errorf("MD5(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}
}

func TestMySQLSHA1(t *testing.T) {
	db := setupMathTestDB(t)
	defer db.Close()

	tests := []struct {
		input    string
		expected string
	}{
		{"hello", "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d"},
		{"", "da39a3ee5e6b4b0d3255bfef95601890afd80709"},
		{"test", "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"},
	}

	for _, tt := range tests {
		var result string
		err := db.QueryRow("SELECT SHA1(?)", tt.input).Scan(&result)
		if err != nil {
			t.Fatalf("Failed to query SHA1(%q): %v", tt.input, err)
		}

		if result != tt.expected {
			t.Errorf("SHA1(%q) = %q, want %q", tt.input, result, tt.expected)
		}
	}

	var result string
	err := db.QueryRow("SELECT SHA(?)", "hello").Scan(&result)
	if err != nil {
		t.Fatalf("Failed to query SHA('hello'): %v", err)
	}

	if result != "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d" {
		t.Errorf("SHA('hello') = %q, want 'aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d'", result)
	}
}

func TestMySQLSHA2(t *testing.T) {
	db := setupMathTestDB(t)
	defer db.Close()

	tests := []struct {
		input    string
		bits     int64
		expected string
	}{
		{"hello", 224, "ea09ae9cc6768c50fcee903ed054556e5bfc8347907f12598aa24193"},
		{"hello", 256, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"},
		{"hello", 384, "59e1748777448c69de6b800d7a33bbfb9ff1b463e44354c3553bcdb9c666fa90125a3c79f90397bdf5f6a13de828684f"},
		{"hello", 512, "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043"},
	}

	for _, tt := range tests {
		var result string
		err := db.QueryRow("SELECT SHA2(?, ?)", tt.input, tt.bits).Scan(&result)
		if err != nil {
			t.Fatalf("Failed to query SHA2(%q, %d): %v", tt.input, tt.bits, err)
		}

		if result != tt.expected {
			t.Errorf("SHA2(%q, %d) = %q, want %q", tt.input, tt.bits, result, tt.expected)
		}
	}

	var nullResult sql.NullString
	err := db.QueryRow("SELECT SHA2('hello', 999)").Scan(&nullResult)
	if err != nil {
		t.Fatalf("Failed to query SHA2('hello', 999): %v", err)
	}

	if nullResult.Valid {
		t.Error("SHA2('hello', 999) should return NULL for invalid bit length")
	}
}
