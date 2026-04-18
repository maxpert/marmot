//go:build sqlite_preupdate_hook
// +build sqlite_preupdate_hook

package coordinator_test

import (
	"sync/atomic"
	"testing"

	"github.com/maxpert/marmot/protocol"
)

func BenchmarkSharedScanV1_OverlapVsDisjoint(b *testing.B) {
	opts := sharedScanOptions{
		dim:        32,
		rows:       4096,
		clusters:   128,
		nlist:      128,
		nprobe:     8,
		cacheBytes: 32 << 20,
	}

	cases := []struct {
		name     string
		mode     sharedScanWorkloadMode
		useCache bool
		parallel bool
		want     int
	}{
		{name: "Cache/Overlap", mode: sharedScanWorkloadOverlap, useCache: true, want: 5},
		{name: "Cache/Disjoint", mode: sharedScanWorkloadDisjoint, useCache: true, want: 32},
		{name: "IndependentNoCache/Overlap", mode: sharedScanWorkloadOverlap, useCache: false, want: 5},
		{name: "IndependentNoCache/Disjoint", mode: sharedScanWorkloadDisjoint, useCache: false, want: 32},
		{name: "CacheParallel/Overlap", mode: sharedScanWorkloadOverlap, useCache: true, parallel: true, want: 5},
		{name: "CacheParallel/Disjoint", mode: sharedScanWorkloadDisjoint, useCache: true, parallel: true, want: 32},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			s := setupSharedScanFixture(b, opts)
			workload := buildSharedScanWorkload(b, s, tc.mode, tc.want)
			stmt := sharedScanStatement()

			if tc.parallel {
				var cursor atomic.Uint64
				b.ReportAllocs()
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					session := newSharedScanSession(tc.useCache)
					for pb.Next() {
						idx := int(cursor.Add(1)-1) % len(workload)
						queryBlob := workload[idx].queryBlob
						params := []interface{}{queryBlob, queryBlob}
						info, args, err := s.handler.MaybeRewriteVectorSelect(stmt, params, session)
						if err != nil {
							b.Fatalf("MaybeRewriteVectorSelect: %v", err)
						}
						if info == nil {
							b.Fatal("expected rewrite info")
						}
						rs, err := s.handler.ExecuteVectorPlan(stmt, info, args, protocol.ConsistencyLocalOne)
						if err != nil {
							b.Fatalf("ExecuteVectorPlan: %v", err)
						}
						if rs == nil || len(rs.Rows) == 0 {
							b.Fatal("empty result set")
						}
					}
				})
				return
			}

			session := newSharedScanSession(tc.useCache)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				queryBlob := workload[i%len(workload)].queryBlob
				params := []interface{}{queryBlob, queryBlob}
				info, args, err := s.handler.MaybeRewriteVectorSelect(stmt, params, session)
				if err != nil {
					b.Fatalf("MaybeRewriteVectorSelect: %v", err)
				}
				if info == nil {
					b.Fatal("expected rewrite info")
				}
				rs, err := s.handler.ExecuteVectorPlan(stmt, info, args, protocol.ConsistencyLocalOne)
				if err != nil {
					b.Fatalf("ExecuteVectorPlan: %v", err)
				}
				if rs == nil || len(rs.Rows) == 0 {
					b.Fatal("empty result set")
				}
			}
		})
	}
}
