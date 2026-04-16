package coordinator

import (
	"testing"

	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/protocol"
)

func makeTestSession(vars vecindex.VecSessionVars) *protocol.ConnectionSession {
	s := &protocol.ConnectionSession{}
	s.VecVars = vars
	return s
}

func TestConnQuerySession_Nprobe_UsesSessionWhenSet(t *testing.T) {
	vars := vecindex.DefaultVecSessionVars()
	vars.Nprobe = 64
	qs := NewQuerySession(makeTestSession(vars))
	if got := qs.Nprobe(16); got != 64 {
		t.Errorf("Nprobe(indexDefault=16) = %d, want 64", got)
	}
}

func TestConnQuerySession_Nprobe_FallsBackToIndexDefault(t *testing.T) {
	vars := vecindex.DefaultVecSessionVars() // Nprobe==0
	qs := NewQuerySession(makeTestSession(vars))
	if got := qs.Nprobe(32); got != 32 {
		t.Errorf("Nprobe(indexDefault=32) = %d, want 32 (fallback)", got)
	}
}

func TestConnQuerySession_ForcePlan(t *testing.T) {
	tests := []struct {
		plan vecindex.ForcePlan
		want string
	}{
		{vecindex.ForcePlanAuto, "auto"},
		{vecindex.ForcePlanPre, "pre"},
		{vecindex.ForcePlanPost, "post"},
	}
	for _, tt := range tests {
		vars := vecindex.DefaultVecSessionVars()
		vars.ForcePlan = tt.plan
		qs := NewQuerySession(makeTestSession(vars))
		if got := qs.ForcePlan(); got != tt.want {
			t.Errorf("ForcePlan() = %q, want %q", got, tt.want)
		}
	}
}

func TestConnQuerySession_PrefilterCap(t *testing.T) {
	vars := vecindex.DefaultVecSessionVars()
	vars.PrefilterCap = 3000
	qs := NewQuerySession(makeTestSession(vars))
	if got := qs.PrefilterCap(); got != 3000 {
		t.Errorf("PrefilterCap() = %d, want 3000", got)
	}
}

func TestConnQuerySession_Fallback(t *testing.T) {
	vars := vecindex.DefaultVecSessionVars()
	vars.Fallback = true
	qs := NewQuerySession(makeTestSession(vars))
	if qs.Fallback() != "on" {
		t.Errorf("Fallback() = %q, want %q", qs.Fallback(), "on")
	}
	vars.Fallback = false
	qs2 := NewQuerySession(makeTestSession(vars))
	if qs2.Fallback() != "off" {
		t.Errorf("Fallback() = %q, want %q", qs2.Fallback(), "off")
	}
}

// TestConnQuerySession_LiveMutation verifies that SET @@marmot_vec_nprobe after
// session creation is reflected immediately (pointer semantics).
func TestConnQuerySession_LiveMutation(t *testing.T) {
	sess := makeTestSession(vecindex.DefaultVecSessionVars())
	qs := NewQuerySession(sess)

	if got := qs.Nprobe(8); got != 8 { // unset → fallback
		t.Fatalf("before SET: Nprobe = %d, want 8", got)
	}

	// Simulate SET @@marmot_vec_nprobe = 48
	if err := sess.VecVars.Apply("marmot_vec_nprobe", "48"); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if got := qs.Nprobe(8); got != 48 { // now session value wins
		t.Errorf("after SET: Nprobe = %d, want 48", got)
	}
}
