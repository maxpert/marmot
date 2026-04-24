package db

import (
	"context"
	"database/sql"
	"runtime"
	"runtime/debug"
)

func releaseVectorBuildMemory() {
	runtime.GC()
	debug.FreeOSMemory()
}

func releaseVectorBuildResources(ctx context.Context, conn *sql.DB) {
	if conn != nil {
		_, _ = conn.ExecContext(ctx, "PRAGMA shrink_memory")
	}
	releaseVectorBuildMemory()
}
