package cfg

import (
	"testing"
	"time"
)

// TestValidate_AutoIncIntervals: an unset AUTO_INCREMENT merge or base sync
// interval takes its default, a set one is kept, and a negative one is
// refused.
func TestValidate_AutoIncIntervals(t *testing.T) {
	tests := []struct {
		name                string
		merge, sync         int
		wantMerge, wantSync time.Duration
		wantErr             bool
	}{
		{name: "unset takes the defaults", wantMerge: 2 * time.Second, wantSync: 10 * time.Second},
		{name: "set values are kept", merge: 100, sync: 500, wantMerge: 100 * time.Millisecond, wantSync: 500 * time.Millisecond},
		{name: "negative merge is refused", merge: -1, wantErr: true},
		{name: "negative sync is refused", sync: -1, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := Config
			defer func() { Config = original }()
			conf := *original
			conf.Cluster.AutoIncMergeIntervalMS = tt.merge
			conf.Cluster.AutoIncBaseSyncIntervalMS = tt.sync
			Config = &conf

			err := Validate()
			if tt.wantErr {
				if err == nil {
					t.Fatal("Validate accepted a negative interval")
				}
				return
			}
			if err != nil {
				t.Fatalf("Validate: %v", err)
			}
			if got := Config.Cluster.GetAutoIncMergeInterval(); got != tt.wantMerge {
				t.Fatalf("merge interval = %s, want %s", got, tt.wantMerge)
			}
			if got := Config.Cluster.GetAutoIncBaseSyncInterval(); got != tt.wantSync {
				t.Fatalf("base sync interval = %s, want %s", got, tt.wantSync)
			}
		})
	}
}
