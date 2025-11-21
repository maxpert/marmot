package coordinator

import (
	"testing"

	"github.com/maxpert/marmot/hlc"
)

func TestNewConflictResolver(t *testing.T) {
	cr := NewConflictResolver(LastWriteWins)
	if cr == nil {
		t.Fatal("Expected non-nil ConflictResolver")
	}

	if cr.strategy != LastWriteWins {
		t.Errorf("Expected strategy %v, got %v", LastWriteWins, cr.strategy)
	}
}

func TestResolveTimestamps_LastWriteWins(t *testing.T) {
	cr := NewConflictResolver(LastWriteWins)

	tests := []struct {
		name string
		ts1  hlc.Timestamp
		ts2  hlc.Timestamp
		want bool // true if ts1 wins
	}{
		{
			name: "ts1 has higher wall time",
			ts1:  hlc.Timestamp{WallTime: 200, Logical: 0, NodeID: 1},
			ts2:  hlc.Timestamp{WallTime: 100, Logical: 0, NodeID: 2},
			want: true,
		},
		{
			name: "ts2 has higher wall time",
			ts1:  hlc.Timestamp{WallTime: 100, Logical: 0, NodeID: 1},
			ts2:  hlc.Timestamp{WallTime: 200, Logical: 0, NodeID: 2},
			want: false,
		},
		{
			name: "Same wall time, ts1 has higher logical",
			ts1:  hlc.Timestamp{WallTime: 100, Logical: 5, NodeID: 1},
			ts2:  hlc.Timestamp{WallTime: 100, Logical: 3, NodeID: 2},
			want: true,
		},
		{
			name: "Same wall time, ts2 has higher logical",
			ts1:  hlc.Timestamp{WallTime: 100, Logical: 3, NodeID: 1},
			ts2:  hlc.Timestamp{WallTime: 100, Logical: 5, NodeID: 2},
			want: false,
		},
		{
			name: "Same wall and logical, ts1 has higher node ID",
			ts1:  hlc.Timestamp{WallTime: 100, Logical: 5, NodeID: 2},
			ts2:  hlc.Timestamp{WallTime: 100, Logical: 5, NodeID: 1},
			want: true,
		},
		{
			name: "Identical timestamps",
			ts1:  hlc.Timestamp{WallTime: 100, Logical: 5, NodeID: 1},
			ts2:  hlc.Timestamp{WallTime: 100, Logical: 5, NodeID: 1},
			want: true, // ts1 wins on equality
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cr.ResolveTimestamps(tt.ts1, tt.ts2)
			if got != tt.want {
				t.Errorf("ResolveTimestamps() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestResolveTimestamps_FirstWriteWins(t *testing.T) {
	cr := NewConflictResolver(FirstWriteWins)

	tests := []struct {
		name string
		ts1  hlc.Timestamp
		ts2  hlc.Timestamp
		want bool // true if ts1 wins
	}{
		{
			name: "ts1 has lower wall time (wins)",
			ts1:  hlc.Timestamp{WallTime: 100, Logical: 0, NodeID: 1},
			ts2:  hlc.Timestamp{WallTime: 200, Logical: 0, NodeID: 2},
			want: true,
		},
		{
			name: "ts2 has lower wall time (wins)",
			ts1:  hlc.Timestamp{WallTime: 200, Logical: 0, NodeID: 1},
			ts2:  hlc.Timestamp{WallTime: 100, Logical: 0, NodeID: 2},
			want: false,
		},
		{
			name: "Same wall time, ts1 has lower logical (wins)",
			ts1:  hlc.Timestamp{WallTime: 100, Logical: 3, NodeID: 1},
			ts2:  hlc.Timestamp{WallTime: 100, Logical: 5, NodeID: 2},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cr.ResolveTimestamps(tt.ts1, tt.ts2)
			if got != tt.want {
				t.Errorf("ResolveTimestamps() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSelectWinner(t *testing.T) {
	cr := NewConflictResolver(LastWriteWins)

	tests := []struct {
		name       string
		timestamps []hlc.Timestamp
		wantIdx    int
	}{
		{
			name:       "Empty timestamps",
			timestamps: []hlc.Timestamp{},
			wantIdx:    -1,
		},
		{
			name: "Single timestamp",
			timestamps: []hlc.Timestamp{
				{WallTime: 100, Logical: 0, NodeID: 1},
			},
			wantIdx: 0,
		},
		{
			name: "Winner is first",
			timestamps: []hlc.Timestamp{
				{WallTime: 300, Logical: 0, NodeID: 1},
				{WallTime: 100, Logical: 0, NodeID: 2},
				{WallTime: 200, Logical: 0, NodeID: 3},
			},
			wantIdx: 0,
		},
		{
			name: "Winner is middle",
			timestamps: []hlc.Timestamp{
				{WallTime: 100, Logical: 0, NodeID: 1},
				{WallTime: 300, Logical: 0, NodeID: 2},
				{WallTime: 200, Logical: 0, NodeID: 3},
			},
			wantIdx: 1,
		},
		{
			name: "Winner is last",
			timestamps: []hlc.Timestamp{
				{WallTime: 100, Logical: 0, NodeID: 1},
				{WallTime: 200, Logical: 0, NodeID: 2},
				{WallTime: 300, Logical: 0, NodeID: 3},
			},
			wantIdx: 2,
		},
		{
			name: "Same wall time, different logical",
			timestamps: []hlc.Timestamp{
				{WallTime: 100, Logical: 1, NodeID: 1},
				{WallTime: 100, Logical: 5, NodeID: 2},
				{WallTime: 100, Logical: 3, NodeID: 3},
			},
			wantIdx: 1, // Highest logical clock
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cr.SelectWinner(tt.timestamps)
			if got != tt.wantIdx {
				t.Errorf("SelectWinner() = %d, want %d", got, tt.wantIdx)
			}
		})
	}
}

func TestSelectLatestVersion(t *testing.T) {
	cr := NewConflictResolver(LastWriteWins)

	tests := []struct {
		name     string
		versions []Version
		wantData interface{}
		wantNil  bool
	}{
		{
			name:     "Empty versions",
			versions: []Version{},
			wantNil:  true,
		},
		{
			name: "Single version",
			versions: []Version{
				{
					Timestamp: hlc.Timestamp{WallTime: 100, Logical: 0, NodeID: 1},
					Data:      "data1",
				},
			},
			wantData: "data1",
			wantNil:  false,
		},
		{
			name: "Multiple versions - latest is first",
			versions: []Version{
				{
					Timestamp: hlc.Timestamp{WallTime: 300, Logical: 0, NodeID: 1},
					Data:      "data3",
				},
				{
					Timestamp: hlc.Timestamp{WallTime: 100, Logical: 0, NodeID: 2},
					Data:      "data1",
				},
				{
					Timestamp: hlc.Timestamp{WallTime: 200, Logical: 0, NodeID: 3},
					Data:      "data2",
				},
			},
			wantData: "data3",
			wantNil:  false,
		},
		{
			name: "Multiple versions - latest is last",
			versions: []Version{
				{
					Timestamp: hlc.Timestamp{WallTime: 100, Logical: 0, NodeID: 1},
					Data:      "data1",
				},
				{
					Timestamp: hlc.Timestamp{WallTime: 200, Logical: 0, NodeID: 2},
					Data:      "data2",
				},
				{
					Timestamp: hlc.Timestamp{WallTime: 300, Logical: 0, NodeID: 3},
					Data:      "data3",
				},
			},
			wantData: "data3",
			wantNil:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cr.SelectLatestVersion(tt.versions)

			if tt.wantNil {
				if got != nil {
					t.Errorf("SelectLatestVersion() = %v, want nil", got)
				}
				return
			}

			if got == nil {
				t.Fatal("SelectLatestVersion() returned nil, want non-nil")
			}

			if got.Data != tt.wantData {
				t.Errorf("SelectLatestVersion().Data = %v, want %v", got.Data, tt.wantData)
			}
		})
	}
}

func TestIsConflict(t *testing.T) {
	tests := []struct {
		name      string
		ts1       hlc.Timestamp
		ts2       hlc.Timestamp
		threshold int64
		want      bool
	}{
		{
			name:      "Identical timestamps - no conflict",
			ts1:       hlc.Timestamp{WallTime: 100, Logical: 5, NodeID: 1},
			ts2:       hlc.Timestamp{WallTime: 100, Logical: 5, NodeID: 1},
			threshold: 1000,
			want:      false,
		},
		{
			name:      "Different nodes, within threshold - conflict",
			ts1:       hlc.Timestamp{WallTime: 1000, Logical: 0, NodeID: 1},
			ts2:       hlc.Timestamp{WallTime: 1500, Logical: 0, NodeID: 2},
			threshold: 1000, // 500 diff is within 1000 threshold
			want:      true,
		},
		{
			name:      "Different nodes, outside threshold - no conflict",
			ts1:       hlc.Timestamp{WallTime: 1000, Logical: 0, NodeID: 1},
			ts2:       hlc.Timestamp{WallTime: 3000, Logical: 0, NodeID: 2},
			threshold: 1000, // 2000 diff is outside 1000 threshold
			want:      false,
		},
		{
			name:      "Different nodes, exactly at threshold - conflict",
			ts1:       hlc.Timestamp{WallTime: 1000, Logical: 0, NodeID: 1},
			ts2:       hlc.Timestamp{WallTime: 2000, Logical: 0, NodeID: 2},
			threshold: 1000, // 1000 diff equals threshold
			want:      true,
		},
		{
			name:      "ts2 before ts1, within threshold - conflict",
			ts1:       hlc.Timestamp{WallTime: 2000, Logical: 0, NodeID: 1},
			ts2:       hlc.Timestamp{WallTime: 1500, Logical: 0, NodeID: 2},
			threshold: 1000, // 500 diff is within 1000 threshold
			want:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsConflict(tt.ts1, tt.ts2, tt.threshold)
			if got != tt.want {
				t.Errorf("IsConflict() = %v, want %v", got, tt.want)
			}
		})
	}
}

func BenchmarkResolveTimestamps(b *testing.B) {
	cr := NewConflictResolver(LastWriteWins)
	ts1 := hlc.Timestamp{WallTime: 200, Logical: 5, NodeID: 1}
	ts2 := hlc.Timestamp{WallTime: 100, Logical: 3, NodeID: 2}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cr.ResolveTimestamps(ts1, ts2)
	}
}

func BenchmarkSelectWinner(b *testing.B) {
	cr := NewConflictResolver(LastWriteWins)
	timestamps := []hlc.Timestamp{
		{WallTime: 100, Logical: 0, NodeID: 1},
		{WallTime: 300, Logical: 0, NodeID: 2},
		{WallTime: 200, Logical: 0, NodeID: 3},
		{WallTime: 150, Logical: 0, NodeID: 4},
		{WallTime: 250, Logical: 0, NodeID: 5},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cr.SelectWinner(timestamps)
	}
}
