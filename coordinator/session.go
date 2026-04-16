package coordinator

import (
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/protocol"
)

// connQuerySession implements QuerySession backed by a protocol.ConnectionSession.
// It satisfies the interface consumed by RewriteVectorQuery (P2-A) and the
// coordinator handler fallback path (§7.5).
type connQuerySession struct {
	vars *vecindex.VecSessionVars
}

// NewQuerySession wraps a ConnectionSession's VecVars to satisfy QuerySession.
// The returned value holds a pointer into session.VecVars so mutations via
// SET @@marmot_vec_* are visible immediately.
func NewQuerySession(session *protocol.ConnectionSession) QuerySession {
	return &connQuerySession{vars: &session.VecVars}
}

func (s *connQuerySession) Nprobe(indexDefault int) int {
	if s.vars.Nprobe > 0 {
		return s.vars.Nprobe
	}
	return indexDefault
}

func (s *connQuerySession) ForcePlan() string {
	return s.vars.ForcePlan.String()
}

func (s *connQuerySession) PrefilterCap() int64 {
	return int64(s.vars.PrefilterCap)
}

func (s *connQuerySession) Fallback() string {
	if s.vars.Fallback {
		return "on"
	}
	return "off"
}

func (s *connQuerySession) UseGoRank() bool { return s.vars.UseGoRank }

func (s *connQuerySession) UseCache() bool { return s.vars.UseCache }
