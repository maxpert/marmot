package db

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/maxpert/marmot/hlc"
	"github.com/stretchr/testify/require"
)

// loadDataBenchRows is how many rows each benchmarked LOAD DATA carries.
const loadDataBenchRows = 10_000

// BenchmarkReplayLoadData replays one LOAD DATA of loadDataBenchRows new
// rows per iteration.
func BenchmarkReplayLoadData(b *testing.B) {
	dbMgr, err := NewDatabaseManager(b.TempDir(), 1, hlc.NewClock(1))
	require.NoError(b, err)
	defer dbMgr.Close()
	require.NoError(b, dbMgr.CreateDatabase("app"))
	mdb, err := dbMgr.GetDatabase("app")
	require.NoError(b, err)
	_, err = mdb.GetWriteDB().Exec(`CREATE TABLE docs (id INTEGER PRIMARY KEY, title TEXT)`)
	require.NoError(b, err)
	require.NoError(b, mdb.ReloadSchema())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		var payload strings.Builder
		for r := 0; r < loadDataBenchRows; r++ {
			fmt.Fprintf(&payload, "%d,title-%d\n", i*loadDataBenchRows+r, r)
		}
		row := &EncodedCapturedRow{
			Table: "docs", Op: uint8(OpTypeLoadData),
			LoadSQL:  "LOAD DATA LOCAL INFILE 'x.csv' INTO TABLE docs FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' (id, title)",
			LoadData: []byte(payload.String()),
		}
		txn := &ReplayTxn{TxnID: uint64(i + 1), OriginNodeID: 2, CommitTS: hlc.Timestamp{WallTime: int64(i + 1), NodeID: 2}, Rows: []*EncodedCapturedRow{row}}
		b.StartTimer()
		_, err := mdb.ApplyReplayedTxn(context.Background(), txn, false)
		require.NoError(b, err)
	}
}
