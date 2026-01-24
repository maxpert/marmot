package protocol

import "testing"

func TestParseMarmotSetCommand(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantVarType string
		wantValue   string
		wantMatched bool
	}{
		{
			name:        "SET transpilation ON",
			sql:         "SET marmot_transpilation = ON",
			wantVarType: "transpilation",
			wantValue:   "ON",
			wantMatched: true,
		},
		{
			name:        "SET transpilation OFF",
			sql:         "SET marmot_transpilation = OFF",
			wantVarType: "transpilation",
			wantValue:   "OFF",
			wantMatched: true,
		},
		{
			name:        "case insensitive",
			sql:         "set MARMOT_TRANSPILATION = on",
			wantVarType: "transpilation",
			wantValue:   "ON",
			wantMatched: true,
		},
		{
			name:        "with semicolon",
			sql:         "SET marmot_transpilation = OFF;",
			wantVarType: "transpilation",
			wantValue:   "OFF",
			wantMatched: true,
		},
		{
			name:        "with whitespace",
			sql:         "  SET   marmot_transpilation   =   ON  ",
			wantVarType: "transpilation",
			wantValue:   "ON",
			wantMatched: true,
		},
		{
			name:        "not a marmot command",
			sql:         "SET autocommit = 1",
			wantVarType: "",
			wantValue:   "",
			wantMatched: false,
		},
		{
			name:        "invalid value",
			sql:         "SET marmot_transpilation = MAYBE",
			wantVarType: "",
			wantValue:   "",
			wantMatched: false,
		},
		{
			name:        "regular SELECT",
			sql:         "SELECT * FROM users",
			wantVarType: "",
			wantValue:   "",
			wantMatched: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVarType, gotValue, gotMatched := ParseMarmotSetCommand(tt.sql)
			if gotVarType != tt.wantVarType {
				t.Errorf("ParseMarmotSetCommand() gotVarType = %v, want %v", gotVarType, tt.wantVarType)
			}
			if gotValue != tt.wantValue {
				t.Errorf("ParseMarmotSetCommand() gotValue = %v, want %v", gotValue, tt.wantValue)
			}
			if gotMatched != tt.wantMatched {
				t.Errorf("ParseMarmotSetCommand() gotMatched = %v, want %v", gotMatched, tt.wantMatched)
			}
		})
	}
}

func TestParseLoadExtensionCommand(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantExtName string
		wantMatched bool
	}{
		{
			name:        "LOAD EXTENSION simple",
			sql:         "LOAD EXTENSION sqlite-vector",
			wantExtName: "sqlite-vector",
			wantMatched: true,
		},
		{
			name:        "case insensitive",
			sql:         "load extension MyExtension",
			wantExtName: "MyExtension",
			wantMatched: true,
		},
		{
			name:        "with semicolon",
			sql:         "LOAD EXTENSION vec0;",
			wantExtName: "vec0",
			wantMatched: true,
		},
		{
			name:        "with whitespace",
			sql:         "  LOAD   EXTENSION   myext  ",
			wantExtName: "myext",
			wantMatched: true,
		},
		{
			name:        "not a load extension",
			sql:         "SELECT * FROM extensions",
			wantExtName: "",
			wantMatched: false,
		},
		{
			name:        "load data infile",
			sql:         "LOAD DATA INFILE 'file.csv'",
			wantExtName: "",
			wantMatched: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotExtName, gotMatched := ParseLoadExtensionCommand(tt.sql)
			if gotExtName != tt.wantExtName {
				t.Errorf("ParseLoadExtensionCommand() gotExtName = %v, want %v", gotExtName, tt.wantExtName)
			}
			if gotMatched != tt.wantMatched {
				t.Errorf("ParseLoadExtensionCommand() gotMatched = %v, want %v", gotMatched, tt.wantMatched)
			}
		})
	}
}

func TestIsMarmotSessionCommand(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want bool
	}{
		{
			name: "SET marmot_transpilation",
			sql:  "SET marmot_transpilation = ON",
			want: true,
		},
		{
			name: "SET marmot_transpilation lowercase",
			sql:  "set marmot_transpilation = off",
			want: true,
		},
		{
			name: "LOAD EXTENSION",
			sql:  "LOAD EXTENSION vec0",
			want: true,
		},
		{
			name: "load extension lowercase",
			sql:  "load extension myext",
			want: true,
		},
		{
			name: "regular SET",
			sql:  "SET autocommit = 1",
			want: false,
		},
		{
			name: "regular SELECT",
			sql:  "SELECT * FROM users",
			want: false,
		},
		{
			name: "LOAD DATA",
			sql:  "LOAD DATA INFILE 'file.csv'",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsMarmotSessionCommand(tt.sql); got != tt.want {
				t.Errorf("IsMarmotSessionCommand() = %v, want %v", got, tt.want)
			}
		})
	}
}
