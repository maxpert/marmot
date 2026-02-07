package query

import "testing"

func TestConvertANSIQuotes_BackslashEscapes(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple identifier",
			input:    `SELECT "col" FROM "tbl"`,
			expected: "SELECT `col` FROM `tbl`",
		},
		{
			name:     "string with escaped quote",
			input:    `SELECT "col" FROM "tbl" WHERE name = 'it\'s working'`,
			expected: "SELECT `col` FROM `tbl` WHERE name = 'it\\'s working'",
		},
		{
			name:     "COMMENT with escaped quote",
			input:    `CREATE TABLE "batch" ("token" VARCHAR(64) COMMENT 'the user\'s token')`,
			expected: "CREATE TABLE `batch` (`token` VARCHAR(64) COMMENT 'the user\\'s token')",
		},
		{
			name:     "multiple escaped chars",
			input:    `INSERT INTO "t" VALUES ('line1\nline2', 'path\\to\\file')`,
			expected: "INSERT INTO `t` VALUES ('line1\\nline2', 'path\\\\to\\\\file')",
		},
		{
			name:     "SQL escaped quote",
			input:    `SELECT "col" FROM "tbl" WHERE name = 'it''s fine'`,
			expected: "SELECT `col` FROM `tbl` WHERE name = 'it''s fine'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertANSIQuotesToBackticks(tt.input)
			if result != tt.expected {
				t.Errorf("\nInput:    %s\nExpected: %s\nGot:      %s", tt.input, tt.expected, result)
			}
		})
	}
}
