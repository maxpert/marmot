package coordinator

import (
	"testing"
	"time"

	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/require"
)

// The commit timestamp must follow every participant's PREPARE-time clock:
// a participant that granted a row's intent had already committed that
// row's previous writer, so its clock is past that writer's commit.
func TestDecideCommitTSFollowsEveryPreparedParticipantsClock(t *testing.T) {
	wc := NewWriteCoordinator(1, nil, nil, nil, time.Second, hlc.NewClock(1))
	ahead := hlc.Timestamp{WallTime: time.Now().Add(time.Hour).UnixNano(), Logical: 7, NodeID: 3}

	got := wc.decideCommitTS(map[uint64]*ReplicationResponse{
		2: {Success: true},
		3: {Success: true, AppliedAt: ahead},
	})

	require.True(t, hlc.After(got, ahead), "commit %v must follow participant clock %v", got, ahead)
	require.Equal(t, uint64(1), got.NodeID)
}
