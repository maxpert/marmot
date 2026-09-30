//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package coordinator_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkAutoIncrementInsert measures single-row autocommit INSERTs through
// the coordinator handler into an INT AUTO_INCREMENT table (narrow ids from
// claimed ranges) and into a BIGINT one (wide generated ids), in one session.
func BenchmarkAutoIncrementInsert(b *testing.B) {
	for _, tc := range []struct{ name, ddl string }{
		{"narrow_int", "CREATE TABLE bench (id INT AUTO_INCREMENT PRIMARY KEY, v TEXT)"},
		{"wide_bigint", "CREATE TABLE bench (id BIGINT AUTO_INCREMENT PRIMARY KEY, v TEXT)"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			s := setupNoopDML(b)
			_, err := s.handler.HandleQuery(s.session, tc.ddl, nil)
			require.NoError(b, err)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := s.handler.HandleQuery(s.session, "INSERT INTO bench (v) VALUES ('x')", nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
