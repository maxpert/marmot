package vecindex

import (
	"strings"
	"testing"
)

func TestValidateIndexName(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		in      string
		wantErr bool
	}{
		{"simple", "embeddings", false},
		{"with_underscore", "docs_embed", false},
		{"with_digits", "idx42", false},
		{"empty", "", true},
		{"leading_digit", "1idx", true},
		{"leading_underscore", "_idx", true},
		{"hyphen", "my-idx", true},
		{"space", "my idx", true},
		{"dot", "my.idx", true},
		{"reserved_marmot", "marmot_foo", true},
		{"reserved_marmot_case", "MarmotFoo", true},
		{"too_long", strings.Repeat("a", MaxIndexNameLen+1), true},
		{"max_len", "a" + strings.Repeat("b", MaxIndexNameLen-1), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateIndexName(tc.in)
			if tc.wantErr && err == nil {
				t.Fatalf("ValidateIndexName(%q) = nil, want error", tc.in)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("ValidateIndexName(%q) = %v, want nil", tc.in, err)
			}
		})
	}
}

func TestIsVecLocalTable(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"members", "__marmot_vec_emb_members", true},
		{"staging", "__marmot_vec_emb_members_next", true},
		{"trigger", "__marmot_vec_emb_ai", true},
		{"centroids_replicated", "_marmot_vec_emb_centroids", false},
		{"user_table", "docs", false},
		{"empty", "", false},
		{"partial_prefix", "__marmot_ve", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := IsVecLocalTable(tc.in)
			if got != tc.want {
				t.Errorf("IsVecLocalTable(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}
