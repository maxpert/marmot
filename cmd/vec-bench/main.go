// vec-bench — standalone benchmark tool for the Marmot v5.4 vector index.
//
// Drives the online CREATE-index-first + insert + query path against an
// arbitrary ANN benchmark dataset in fvecs/ivecs format. Persists its SQLite
// database so subsequent runs can skip the build and exercise just the query
// hot path.
//
// Usage:
//
//	vec-bench \
//	   --data-dir /tmp/marmot/benchdata/dbpedia-openai-1536 \
//	   --db-dir   /tmp/marmot/vec-bench \
//	   --index    embed_idx \
//	   --table    docs \
//	   --column   embed \
//	   --n-queries 10000 \
//	   --warmup    1000
//
// Flags:
//
//	--data-dir     Directory with train.fvecs, test.fvecs, groundtruth.ivecs, metadata.json.
//	--db-dir       Persistent DB root. Default /tmp/marmot/vec-bench.
//	--db-name      Marmot database name. Default "bench".
//	--index        Vector-index name. Default "embed_idx".
//	--table        Base table name. Default "docs".
//	--column       Embedding column name. Default "embed".
//	--metric       cosine | l2 | dot. Default: from metadata.json ("angular"→cosine).
//	--nlist        IVF clusters. 0 = auto-tune (~4√n, capped at 2048).
//	--nprobe       Probed clusters per query. 0 = auto-tune (√nlist, min 8).
//	--force-build  Drop existing index + table and rebuild from scratch.
//	--skip-insert  Assume docs already populated; just (re)use the index.
//	--bootstrap-rows Initial rows inserted before the first REINDEX.
//	                 Default -1 = auto (~50%, floored at nlist). Use 0 to
//	                 keep the run delta-only end-to-end.
//	--warmup       Warmup query count before measurement. Default 1000.
//	--n-queries    Measurement query count. Default len(test).
//	--k            Top-K returned per query. Default 10.
//	--settle-delta-timeout Wait up to this long for background delta flush
//	                       to drain cluster_id=0 rows before read
//	                       measurement. Default 0 (don't wait).
//	--profile-dir  pprof output dir. Default /tmp/marmot/vec-bench/prof.
//	--use-go-rank  Use the Go-side ranking path (default true).
//	--use-cache    Use the legacy in-memory vector cache (default false).
//	--cache-bytes  Per-index cache budget for --use-cache.
//	               Default 0 = auto (256 MiB).
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	appcfg "github.com/maxpert/marmot/cfg"
	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/db"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/modules/vecindex"
	"github.com/maxpert/marmot/modules/vecindex/pkg/benchutil"
	"github.com/maxpert/marmot/modules/vecindex/pkg/kmeans"
	"github.com/maxpert/marmot/protocol"
)

type config struct {
	dataDir            string
	dbDir              string
	dbName             string
	indexName          string
	tableName          string
	columnName         string
	metric             string
	nlist              int
	nprobe             int
	forceBuild         bool
	skipInsert         bool
	bootstrapRows      int
	warmup             int
	nQueries           int
	k                  int
	settleDeltaTimeout time.Duration
	profileDir         string
	profileCPU         bool
	useGoRank          bool
	useCache           bool
	cacheBytes         uint64
	insertTx           int
	insertN            int
	queryConc          int
	insertConc         int
	readPool           int
	deltaFlushInterval time.Duration
	deltaFlushMaxRows  int
	deltaFlushBatch    int
	sqliteCacheMB      int
	sharedScanWindow   time.Duration
	sharedScanMaxReq   int
	sharedScanMaxUnion int
}

func parseFlags() *config {
	c := &config{}
	defaultQueryConc := defaultQueryConcurrency()
	// Use a local FlagSet — importing the `db` package pulls in
	// `cfg.DataDirFlag` et al on the global flag namespace, which would
	// collide with the --data-dir we want for this tool.
	fs := flag.NewFlagSet("vec-bench", flag.ExitOnError)
	fs.StringVar(&c.dataDir, "data-dir", "", "Directory with train.fvecs, test.fvecs, groundtruth.ivecs, metadata.json (required)")
	fs.StringVar(&c.dbDir, "db-dir", "/tmp/marmot/vec-bench", "Persistent DB root")
	fs.StringVar(&c.dbName, "db-name", "bench", "Marmot database name")
	fs.StringVar(&c.indexName, "index", "embed_idx", "Vector-index name")
	fs.StringVar(&c.tableName, "table", "docs", "Base table name")
	fs.StringVar(&c.columnName, "column", "embed", "Embedding column name")
	fs.StringVar(&c.metric, "metric", "", `Distance metric: cosine | l2 | dot (default: metadata.json, "angular"→cosine)`)
	fs.IntVar(&c.nlist, "nlist", 0, "IVF clusters (0 = auto-tune)")
	fs.IntVar(&c.nprobe, "nprobe", 0, "Probed clusters (0 = auto-tune)")
	fs.BoolVar(&c.forceBuild, "force-build", false, "Drop existing index + table and rebuild")
	fs.BoolVar(&c.skipInsert, "skip-insert", false, "Assume docs already populated; just use the existing index")
	fs.IntVar(&c.bootstrapRows, "bootstrap-rows", -1, "Initial rows inserted before first REINDEX (-1 = auto, 0 = delta-only)")
	fs.IntVar(&c.warmup, "warmup", 1000, "Warmup query count before measurement")
	fs.IntVar(&c.nQueries, "n-queries", 0, "Measurement query count (0 = full test set)")
	fs.IntVar(&c.k, "k", 10, "Top-K per query")
	fs.DurationVar(&c.settleDeltaTimeout, "settle-delta-timeout", 0, "Wait for background delta flush before read measurement (0 = disabled)")
	fs.StringVar(&c.profileDir, "profile-dir", "", "pprof output dir (default db-dir/prof)")
	fs.BoolVar(&c.profileCPU, "profile-cpu", true, "Write CPU profiles for warmup/measurement phases")
	fs.BoolVar(&c.useGoRank, "use-go-rank", true, "Use Go-side ranking path")
	fs.BoolVar(&c.useCache, "use-cache", false, "Use legacy in-memory vector cache")
	fs.Uint64Var(&c.cacheBytes, "cache-bytes", 0, "Per-index cache budget for --use-cache (0 = auto 256 MiB)")
	fs.IntVar(&c.insertTx, "insert-tx", 20000, "Rows per insert transaction")
	fs.IntVar(&c.insertN, "insert-n", 0, "Cap inserted rows (0 = all train vectors). Useful for insert-throughput benches.")
	fs.IntVar(&c.queryConc, "query-concurrency", defaultQueryConc, "Concurrent query goroutines (parallel measurement; default = GOMAXPROCS)")
	fs.IntVar(&c.insertConc, "insert-concurrency", 1, "Concurrent insert goroutines (parallel insert phase)")
	fs.IntVar(&c.readPool, "read-pool", 0, "Override readDB max-open-conns (0 = match query-concurrency)")
	fs.DurationVar(&c.sharedScanWindow, "shared-scan-window", 100*time.Microsecond, "Shared-scan microbatch window")
	fs.IntVar(&c.sharedScanMaxReq, "shared-scan-max-requests", 8, "Shared-scan max requests per batch")
	fs.IntVar(&c.sharedScanMaxUnion, "shared-scan-max-union", 64, "Shared-scan max distinct clusters per batch")
	fs.DurationVar(&c.deltaFlushInterval, "delta-flush-interval", 0, "Override delta flush interval (0 = product default)")
	fs.IntVar(&c.deltaFlushMaxRows, "delta-flush-max-rows", 0, "Override delta flush max rows per cycle (0 = product default)")
	fs.IntVar(&c.deltaFlushBatch, "delta-flush-batch", 0, "Override delta flush commit batch size (0 = product default)")
	fs.IntVar(&c.sqliteCacheMB, "sqlite-cache-mb", 64, "SQLite page-cache budget in MiB for vec-bench connections")
	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}

	if c.dataDir == "" {
		fmt.Fprintln(os.Stderr, "--data-dir is required")
		fs.Usage()
		os.Exit(2)
	}
	if c.profileDir == "" {
		c.profileDir = filepath.Join(c.dbDir, "prof")
	}
	return c
}

func defaultQueryConcurrency() int {
	if p := runtime.GOMAXPROCS(0); p > 0 {
		return p
	}
	return 1
}

type harness struct {
	cfg     *config
	dbMgr   *db.DatabaseManager
	vecMgr  *db.VectorIndexManager
	engine  *vecindex.Engine
	handler *coordinator.CoordinatorHandler
	conn    *sql.DB
	readDB  *sql.DB
	meta    benchutil.DatasetMetadata
	train   *benchutil.MMapFvecs
	test    *benchutil.MMapFvecs
	gt      *benchutil.MMapIvecs
}

type queryRunStats struct {
	lats          []time.Duration
	recall10      float64
	recall10in100 float64
	workerQueries []int64
}

// plog writes a timestamped line-terminated message to stderr, prefixed so
// bench output can be grep'd out of the surrounding zerolog noise.
func plog(format string, args ...interface{}) {
	fmt.Fprint(os.Stderr, "[vec-bench] ")
	fmt.Fprintf(os.Stderr, format, args...)
	fmt.Fprintln(os.Stderr)
}

// fatal logs a fatal message and exits 1.
func fatal(format string, args ...interface{}) {
	fmt.Fprint(os.Stderr, "[vec-bench] FATAL ")
	fmt.Fprintf(os.Stderr, format, args...)
	fmt.Fprintln(os.Stderr)
	os.Exit(1)
}

func processRSSBytes() (uint64, error) {
	out, err := exec.Command("ps", "-o", "rss=", "-p", fmt.Sprintf("%d", os.Getpid())).Output()
	if err != nil {
		return 0, err
	}
	var rssKiB uint64
	if _, err := fmt.Sscanf(strings.TrimSpace(string(out)), "%d", &rssKiB); err != nil {
		return 0, err
	}
	return rssKiB * 1024, nil
}

func logMemorySnapshot(label string) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	rssBytes, err := processRSSBytes()
	if err != nil {
		plog("memory %s: alloc=%.0f MB inuse=%.0f MB sys=%.0f MB rss=n/a (%v)",
			label,
			float64(ms.HeapAlloc)/1e6,
			float64(ms.HeapInuse)/1e6,
			float64(ms.Sys)/1e6,
			err)
		return
	}
	plog("memory %s: alloc=%.0f MB inuse=%.0f MB sys=%.0f MB rss=%.0f MB",
		label,
		float64(ms.HeapAlloc)/1e6,
		float64(ms.HeapInuse)/1e6,
		float64(ms.Sys)/1e6,
		float64(rssBytes)/1e6)
}

func main() {
	cfg := parseFlags()
	plog("starting: data-dir=%s db-dir=%s db-name=%s index=%s",
		cfg.dataDir, cfg.dbDir, cfg.dbName, cfg.indexName)
	if err := os.MkdirAll(cfg.dbDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "[vec-bench] FATAL mkdir db-dir: %v\n", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(cfg.profileDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "[vec-bench] FATAL mkdir profile-dir: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintln(os.Stderr, "[vec-bench] opening harness ...")
	h, err := openHarness(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[vec-bench] FATAL openHarness: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintln(os.Stderr, "[vec-bench] harness opened")
	defer h.Close()

	if cfg.forceBuild {
		if err := h.dropExisting(); err != nil {
			fatal("dropExisting: %v", err)
		}
	}

	if err := h.loadDataset(); err != nil {
		fatal("loadDataset: %v", err)
	}

	if err := h.ensureTable(); err != nil {
		fatal("ensureTable: %v", err)
	}

	if err := h.ensureIndex(); err != nil {
		fatal("ensureIndex: %v", err)
	}

	if !cfg.skipInsert {
		if err := h.ensureTableAndInsert(); err != nil {
			fatal("ensureTableAndInsert: %v", err)
		}
	}

	if err := h.runQueryPhase(); err != nil {
		fatal("runQueryPhase: %v", err)
	}

	plog("done")
}

func (h *harness) Close() {
	if h.train != nil {
		_ = h.train.Close()
		h.train = nil
	}
	if h.test != nil {
		_ = h.test.Close()
		h.test = nil
	}
	if h.gt != nil {
		_ = h.gt.Close()
		h.gt = nil
	}
}

func (h *harness) releaseTrain() {
	if h.train == nil {
		return
	}
	if err := h.train.Close(); err != nil {
		plog("warning: close train mmap: %v", err)
	}
	h.train = nil
	logMemorySnapshot("after releasing train mmap")
}

func openHarness(cfg *config) (*harness, error) {
	clock := hlc.NewClock(1)
	if cfg.skipInsert {
		appcfg.Config.BatchCommit.Enabled = false
		plog("batch committer disabled for skip-insert run")
	}
	if cfg.useCache || cfg.cacheBytes > 0 {
		budget := cfg.cacheBytes
		if budget == 0 {
			budget = 256 << 20
		}
		appcfg.Config.VectorIndex.CacheBytes = budget
		plog("legacy vector cache budget: %.0f MB", float64(budget)/(1<<20))
	}
	dbMgr, err := db.NewDatabaseManager(cfg.dbDir, 1, clock)
	if err != nil {
		return nil, fmt.Errorf("new db manager: %w", err)
	}
	if !dbMgr.DatabaseExists(cfg.dbName) {
		if err := dbMgr.CreateDatabase(cfg.dbName); err != nil {
			return nil, fmt.Errorf("create database: %w", err)
		}
	}

	vecMgr := db.NewVectorIndexManager(dbMgr)
	dbMgr.SetVectorIndexManager(vecMgr)

	engine := vecindex.NewEngine()
	db.SetVectorUDFProvider(engine)

	conn, err := dbMgr.GetDatabaseConnection(cfg.dbName)
	if err != nil {
		return nil, fmt.Errorf("get write conn: %w", err)
	}
	engine.SetFlushDB(db.NewSQLDeltaFlushDB(dbMgr))
	flushCfg := vecindex.DefaultDeltaFlushConfig()
	if cfg.deltaFlushInterval > 0 {
		flushCfg.Interval = cfg.deltaFlushInterval
	}
	if cfg.deltaFlushMaxRows > 0 {
		flushCfg.MaxRows = cfg.deltaFlushMaxRows
	}
	if cfg.deltaFlushBatch > 0 {
		flushCfg.BatchSize = cfg.deltaFlushBatch
	}
	engine.SetFlushConfig(flushCfg)

	hook := db.NewEngineHook(engine, dbMgr)
	vecMgr.SetLifecycleHook(hook)
	vecMgr.SetEngineProvider(hook)
	vecMgr.SetReindexHook(hook)
	if err := vecMgr.Start(context.Background()); err != nil {
		return nil, fmt.Errorf("vecMgr start: %w", err)
	}

	readDB, err := dbMgr.GetDatabaseReadConnection(cfg.dbName)
	if err != nil {
		return nil, fmt.Errorf("get read conn: %w", err)
	}
	readPool := cfg.readPool
	if readPool <= 0 {
		readPool = cfg.queryConc
		if readPool < 1 {
			readPool = 1
		}
	}
	readDB.SetMaxOpenConns(readPool)
	readDB.SetMaxIdleConns(readPool)
	if cfg.readPool > 0 {
		plog("readDB pool size overridden: max-open-conns=%d", readPool)
	} else {
		plog("readDB pool size auto-set to query concurrency: max-open-conns=%d", readPool)
	}

	for _, pragma := range []string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA synchronous=NORMAL`,
		`PRAGMA temp_store=MEMORY`,
	} {
		if _, err := conn.Exec(pragma); err != nil {
			return nil, fmt.Errorf("pragma %q: %w", pragma, err)
		}
	}
	if cfg.sqliteCacheMB > 0 {
		cacheKB := sqliteCacheKB(cfg.sqliteCacheMB)
		pragma := fmt.Sprintf(`PRAGMA cache_size=-%d`, cacheKB)
		for _, dbh := range []*sql.DB{conn, readDB} {
			if _, err := dbh.Exec(pragma); err != nil {
				return nil, fmt.Errorf("pragma %q: %w", pragma, err)
			}
		}
		plog("sqlite page cache budget: %d MB", cfg.sqliteCacheMB)
	}

	localReader := db.NewLocalReader(dbMgr)
	nodeProvider := coordinator.NewBenchNodeProvider([]uint64{1})
	rc := coordinator.NewReadCoordinator(1, nodeProvider, localReader, 30*time.Second)
	handler := coordinator.NewBenchHandler(1, rc, dbMgr, clock)
	handler.SetVectorEngine(engine)

	return &harness{
		cfg:     cfg,
		dbMgr:   dbMgr,
		vecMgr:  vecMgr,
		engine:  engine,
		handler: handler,
		conn:    conn,
		readDB:  readDB,
	}, nil
}

func (h *harness) loadDataset() error {
	trainPath := filepath.Join(h.cfg.dataDir, "train.fvecs")
	testPath := filepath.Join(h.cfg.dataDir, "test.fvecs")
	gtPath := filepath.Join(h.cfg.dataDir, "groundtruth.ivecs")
	for _, p := range []string{trainPath, testPath, gtPath} {
		if _, err := os.Stat(p); err != nil {
			return fmt.Errorf("dataset missing: %s (%w)", p, err)
		}
	}
	meta, err := benchutil.LoadMetadata(h.cfg.dataDir)
	if err != nil {
		return fmt.Errorf("load metadata: %w", err)
	}

	loadStart := time.Now()
	plog("loading train.fvecs ...")
	train, err := benchutil.OpenMMapFvecs(trainPath)
	if err != nil {
		return fmt.Errorf("read train: %w", err)
	}
	plog("loading test.fvecs ...")
	test, err := benchutil.OpenMMapFvecs(testPath)
	if err != nil {
		_ = train.Close()
		return fmt.Errorf("read test: %w", err)
	}
	plog("loading groundtruth.ivecs ...")
	gt, err := benchutil.OpenMMapIvecs(gtPath)
	if err != nil {
		_ = train.Close()
		_ = test.Close()
		return fmt.Errorf("read groundtruth: %w", err)
	}
	plog("dataset: train=%d test=%d dim=%d metric=%s k=%d (%s)",
		train.Len(), test.Len(), meta.Dim, meta.Metric, meta.K, time.Since(loadStart))

	if h.cfg.metric == "" {
		switch meta.Metric {
		case "angular", "cosine":
			h.cfg.metric = "cosine"
		case "euclidean":
			h.cfg.metric = "l2"
		default:
			h.cfg.metric = "l2"
		}
	}
	h.meta = meta
	h.train = train
	h.test = test
	h.gt = gt
	logMemorySnapshot("after dataset mmap")
	return nil
}

func (h *harness) ensureTable() error {
	_, err := h.conn.Exec(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s" (
		id    INTEGER PRIMARY KEY,
		"%s"  BLOB
	)`, h.cfg.tableName, h.cfg.columnName))
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	return nil
}

func (h *harness) dropExisting() error {
	plog("force-build: dropping index %q and table %q", h.cfg.indexName, h.cfg.tableName)
	ctx := context.Background()
	_ = h.vecMgr.DropIndex(ctx, h.cfg.indexName, h.cfg.dbName)
	if _, err := h.conn.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, h.cfg.tableName)); err != nil {
		return fmt.Errorf("drop table: %w", err)
	}
	return nil
}

func (h *harness) ensureTableAndInsert() error {
	toInsert := targetInsertRows(h.train.Len(), h.cfg.insertN)

	var existing int64
	if err := h.conn.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM "%s"`, h.cfg.tableName)).Scan(&existing); err != nil {
		return fmt.Errorf("count rows: %w", err)
	}
	if existing == int64(toInsert) {
		plog("docs already populated (%d rows), skipping insert", existing)
		return nil
	}
	if existing > 0 {
		return fmt.Errorf("docs already contains %d rows, expected %d; rerun with --force-build for a clean online benchmark", existing, toInsert)
	}
	if toInsert == 0 {
		plog("insert skipped: target rows=0")
		return nil
	}

	bootstrapRows := resolveBootstrapRows(toInsert, h.cfg.bootstrapRows, h.cfg.nlist)
	plog("online load plan: total=%d bootstrap=%d online=%d",
		toInsert, bootstrapRows, toInsert-bootstrapRows)

	hasCentroids, err := h.indexHasCentroids()
	if err != nil {
		return fmt.Errorf("check centroid state: %w", err)
	}

	if bootstrapRows > 0 {
		if err := h.insertRange(0, bootstrapRows, "bootstrap"); err != nil {
			return err
		}
	}

	if !hasCentroids && bootstrapRows > 0 {
		if err := h.reindexIndex("bootstrap"); err != nil {
			return err
		}
		hasCentroids = true
	}

	if bootstrapRows < toInsert {
		if err := h.insertRange(bootstrapRows, toInsert, "online"); err != nil {
			return err
		}
	}

	if hasCentroids && h.cfg.settleDeltaTimeout > 0 {
		if err := h.waitForDeltaDrain(h.cfg.settleDeltaTimeout); err != nil {
			return err
		}
	}
	h.releaseTrain()
	return nil
}

func targetInsertRows(trainLen, insertCap int) int {
	if insertCap > 0 && insertCap < trainLen {
		return insertCap
	}
	return trainLen
}

func resolveBootstrapRows(totalRows, requested, nlist int) int {
	if totalRows <= 0 {
		return 0
	}
	if requested >= 0 {
		if requested > totalRows {
			return totalRows
		}
		return requested
	}
	auto := totalRows / 2
	if auto == 0 {
		auto = totalRows
	}
	if nlist > 0 && auto < nlist {
		auto = nlist
	}
	if auto > totalRows {
		auto = totalRows
	}
	return auto
}

func (h *harness) insertRange(lo, hi int, phase string) error {
	if hi <= lo {
		plog("%s insert skipped: empty range", phase)
		return nil
	}

	workers := h.cfg.insertConc
	if workers < 1 {
		workers = 1
	}
	rowsToInsert := hi - lo
	plog("%s insert: rows=%d tx-size=%d workers=%d range=[%d,%d)",
		phase, rowsToInsert, h.cfg.insertTx, workers, lo, hi)
	insertStart := time.Now()

	type chunk struct{ lo, hi int }
	chunks := make(chan chunk, 64)
	var workerErr atomic.Value
	var txCount atomic.Int64
	var rowCount atomic.Int64
	var txLatSum atomic.Int64
	var txLatMax atomic.Int64
	workerRows := make([]atomic.Int64, workers)
	workerTxs := make([]atomic.Int64, workers)
	var wg sync.WaitGroup

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for c := range chunks {
				if workerErr.Load() != nil {
					return
				}
				tStart := time.Now()
				tx, err := h.conn.Begin()
				if err != nil {
					workerErr.Store(fmt.Errorf("%s begin tx [%d,%d): %w", phase, c.lo, c.hi, err))
					return
				}
				stmt, err := tx.Prepare(fmt.Sprintf(`INSERT INTO "%s"(id, "%s") VALUES (?, ?)`,
					h.cfg.tableName, h.cfg.columnName))
				if err != nil {
					tx.Rollback()
					workerErr.Store(fmt.Errorf("%s prepare insert: %w", phase, err))
					return
				}
				for i := c.lo; i < c.hi; i++ {
					if _, err := stmt.Exec(int64(i+1), h.train.VectorBytes(i)); err != nil {
						stmt.Close()
						tx.Rollback()
						workerErr.Store(fmt.Errorf("%s insert id=%d: %w", phase, i+1, err))
						return
					}
				}
				stmt.Close()
				if err := tx.Commit(); err != nil {
					workerErr.Store(fmt.Errorf("%s commit tx [%d,%d): %w", phase, c.lo, c.hi, err))
					return
				}
				lat := time.Since(tStart)
				txLatSum.Add(int64(lat))
				for {
					cur := txLatMax.Load()
					if int64(lat) <= cur || txLatMax.CompareAndSwap(cur, int64(lat)) {
						break
					}
				}
				txCount.Add(1)
				rowCount.Add(int64(c.hi - c.lo))
				workerTxs[wid].Add(1)
				workerRows[wid].Add(int64(c.hi - c.lo))
			}
		}(w)
	}

	go func() {
		for inserted := lo; inserted < hi; {
			end := inserted + h.cfg.insertTx
			if end > hi {
				end = hi
			}
			chunks <- chunk{lo: inserted, hi: end}
			inserted = end
		}
		close(chunks)
	}()

	progressDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-progressDone:
				return
			case <-ticker.C:
				rc := rowCount.Load()
				if rc == 0 {
					continue
				}
				plog("  %s inserted %d/%d (%.1fs, %.0f rows/s)",
					phase, rc, rowsToInsert,
					time.Since(insertStart).Seconds(),
					float64(rc)/time.Since(insertStart).Seconds())
			}
		}
	}()

	wg.Wait()
	close(progressDone)
	if e := workerErr.Load(); e != nil {
		return e.(error)
	}

	totalElapsed := time.Since(insertStart)
	nTx := txCount.Load()
	avgTxMs := float64(0)
	if nTx > 0 {
		avgTxMs = float64(txLatSum.Load()) / float64(nTx) / 1e6
	}
	plog("%s insert complete in %s  (workers=%d rows/s=%.0f txns=%d avg-tx=%.1fms max-tx=%.1fms)",
		phase, totalElapsed, workers,
		float64(rowCount.Load())/totalElapsed.Seconds(),
		nTx, avgTxMs, float64(txLatMax.Load())/1e6)
	plog("  %s write throughput: rows/s=%.0f tx/s=%.1f",
		phase,
		float64(rowCount.Load())/totalElapsed.Seconds(),
		float64(nTx)/totalElapsed.Seconds())
	for w := 0; w < workers; w++ {
		plog("  %s write worker[%d]: rows=%d txns=%d rows/s=%.0f tx/s=%.1f",
			phase,
			w,
			workerRows[w].Load(),
			workerTxs[w].Load(),
			float64(workerRows[w].Load())/totalElapsed.Seconds(),
			float64(workerTxs[w].Load())/totalElapsed.Seconds())
	}
	return nil
}

func (h *harness) indexHasCentroids() (bool, error) {
	cs, err := h.loadCentroidSet(h.cfg.indexName)
	if err != nil {
		return false, err
	}
	return cs != nil, nil
}

func (h *harness) reindexIndex(reason string) error {
	plog("reindexing %q after %s phase ...", h.cfg.indexName, reason)
	start := time.Now()
	if err := h.vecMgr.ReindexIndex(context.Background(), h.cfg.indexName); err != nil {
		return fmt.Errorf("reindex %s: %w", reason, err)
	}
	plog("  reindex done in %s", time.Since(start))
	logMemorySnapshot("after reindex")
	return nil
}

func (h *harness) waitForDeltaDrain(timeout time.Duration) error {
	membersQ := fmt.Sprintf(`"%s"`, vecindex.MembersTable(h.cfg.indexName))
	deadline := time.Now().Add(timeout)
	for {
		var deltaRows int64
		if err := h.conn.QueryRow(
			fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE cluster_id = 0`, membersQ),
		).Scan(&deltaRows); err != nil {
			return fmt.Errorf("count delta rows: %w", err)
		}
		if deltaRows == 0 {
			plog("delta flush settled: cluster_id=0 rows drained")
			logMemorySnapshot("after delta settle")
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("delta flush did not settle within %s (remaining=%d)", timeout, deltaRows)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (h *harness) ensureIndex() error {
	ctx := context.Background()
	var status string
	err := h.conn.QueryRow(
		`SELECT status FROM __marmot_vector_indexes WHERE index_name = ?`,
		h.cfg.indexName,
	).Scan(&status)
	if err == nil && status == "ready" {
		ok, err := h.hasSidecarVecLayout(h.cfg.indexName)
		if err != nil {
			return fmt.Errorf("check sidecar layout: %w", err)
		}
		if !ok {
			plog("index %q uses legacy members layout; dropping and rebuilding sidecar storage", h.cfg.indexName)
			if err := h.vecMgr.DropIndex(ctx, h.cfg.indexName, h.cfg.dbName); err != nil {
				return fmt.Errorf("drop legacy index: %w", err)
			}
		} else {
			plog("index %q already exists (status=%s); rehydrating engine state",
				h.cfg.indexName, status)
			if err := h.rehydrateEngine(); err != nil {
				return err
			}
			logMemorySnapshot("after reopen")
			return nil
		}
	}
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("query existing index: %w", err)
	}

	cpuPath := filepath.Join(h.cfg.profileDir, "cpu-create.pb.gz")
	heapPath := filepath.Join(h.cfg.profileDir, "heap-create.pb.gz")
	cpuF, err := os.Create(cpuPath)
	if err != nil {
		return fmt.Errorf("create cpu profile: %w", err)
	}
	if err := pprof.StartCPUProfile(cpuF); err != nil {
		cpuF.Close()
		return fmt.Errorf("start cpu profile: %w", err)
	}

	plog("creating vector index %q (nlist=%d nprobe=%d metric=%s)",
		h.cfg.indexName, h.cfg.nlist, h.cfg.nprobe, h.cfg.metric)
	indexStart := time.Now()
	meta := common.VectorIndexMeta{
		IndexName:  h.cfg.indexName,
		TableName:  h.cfg.tableName,
		ColumnName: h.cfg.columnName,
		Database:   h.cfg.dbName,
		Metric:     h.cfg.metric,
		Dim:        h.meta.Dim,
		Nlist:      h.cfg.nlist,
		Nprobe:     h.cfg.nprobe,
		Status:     "building",
		CreatedAt:  time.Now().UnixNano(),
	}
	if err := h.vecMgr.CreateIndex(ctx, meta); err != nil {
		pprof.StopCPUProfile()
		cpuF.Close()
		return fmt.Errorf("create index: %w", err)
	}
	if err := waitIndexReady(h.conn, h.cfg.indexName, 2*time.Hour); err != nil {
		pprof.StopCPUProfile()
		cpuF.Close()
		return fmt.Errorf("wait index ready: %w", err)
	}
	pprof.StopCPUProfile()
	cpuF.Close()
	indexElapsed := time.Since(indexStart)
	plog("index built in %s  [cpu %s]", indexElapsed, cpuPath)

	heapF, err := os.Create(heapPath)
	if err != nil {
		return fmt.Errorf("create heap profile: %w", err)
	}
	runtime.GC()
	if err := pprof.WriteHeapProfile(heapF); err != nil {
		heapF.Close()
		return fmt.Errorf("write heap profile: %w", err)
	}
	heapF.Close()
	plog("heap profile: %s", heapPath)

	logMemorySnapshot("after build")

	if h.cfg.nlist == 0 || h.cfg.nprobe == 0 {
		row := h.conn.QueryRow(
			`SELECT nlist, nprobe FROM __marmot_vector_indexes WHERE index_name = ?`,
			h.cfg.indexName)
		if err := row.Scan(&h.cfg.nlist, &h.cfg.nprobe); err != nil {
			return fmt.Errorf("read auto-tuned params: %w", err)
		}
		plog("auto-tuned: nlist=%d nprobe=%d", h.cfg.nlist, h.cfg.nprobe)
	}
	return nil
}

// rehydrateEngine restores the in-memory CentroidSet for an index whose
// on-disk state is present but whose Engine hasn't been populated.
func (h *harness) rehydrateEngine() error {
	meta, err := h.readIndexMeta()
	if err != nil {
		return fmt.Errorf("read index meta: %w", err)
	}

	cs, err := h.loadCentroidSet(meta.IndexName)
	if err != nil {
		return fmt.Errorf("load centroid set: %w", err)
	}

	spec := vecindex.IVFSpec{
		ID:     meta.IndexName,
		Dim:    meta.Dim,
		Nlist:  meta.Nlist,
		Nprobe: meta.Nprobe,
		Metric: metricFromString(meta.Metric),
	}
	state := h.engine.RegisterWithCentroidSet(meta.IndexName, spec, cs)
	cache, err := db.BuildEmptyVectorCache(context.Background(), h.readDB, *meta, spec, state.ProbeVersion())
	if err != nil {
		return fmt.Errorf("build cache on reopen: %w", err)
	}
	if cache != nil {
		state.StoreCache(cache)
		plog("rehydrated legacy vector cache on reopen")
	}
	if err := db.BuildResidentDeltaOnReopen(context.Background(), h.readDB, state, *meta, spec); err != nil {
		return fmt.Errorf("build resident delta on reopen: %w", err)
	}
	if delta := state.LoadResidentDelta(); delta != nil {
		plog("rehydrated resident delta on reopen: rows=%d", delta.Len())
	}
	if cs == nil {
		plog("engine rehydrated without centroids; queries will scan the delta partition only until reindex")
	} else {
		if dbPath, pathErr := h.dbMgr.GetDatabasePath(meta.Database); pathErr == nil {
			plog("attempting packed partition store rebuild on reopen: %s", dbPath)
			if err := db.BuildPackedPartitionStoreOnReopen(context.Background(), h.conn, dbPath, state, *meta, spec); err != nil {
				return fmt.Errorf("build packed store on reopen: %w", err)
			}
			if store := state.LoadPackedStore(); store != nil {
				plog("rehydrated packed partition store: %s", store.Path())
			} else {
				plog("packed partition store unavailable on reopen")
			}
		}
		plog("loaded %d centroids (dim=%d) from disk", cs.Len(), meta.Dim)
		plog("engine rehydrated from centroids only; sidecar rows stream directly from SQLite")
	}

	h.cfg.nlist = meta.Nlist
	h.cfg.nprobe = meta.Nprobe
	return nil
}

func (h *harness) readIndexMeta() (*common.VectorIndexMeta, error) {
	row := h.conn.QueryRow(`
		SELECT index_name, table_name, column_name, database_name,
		       metric, dim, nlist, nprobe, auto_nlist, auto_nprobe,
		       target_partition_size, max_norm, status
		FROM __marmot_vector_indexes WHERE index_name = ?`, h.cfg.indexName)
	var (
		m          common.VectorIndexMeta
		autoNlist  int64
		autoNprobe int64
	)
	err := row.Scan(
		&m.IndexName, &m.TableName, &m.ColumnName, &m.Database,
		&m.Metric, &m.Dim, &m.Nlist, &m.Nprobe,
		&autoNlist, &autoNprobe, &m.TargetPartitionSize,
		&m.MaxNorm, &m.Status,
	)
	if err != nil {
		return nil, err
	}
	m.AutoTuneNlist = autoNlist != 0
	m.AutoTuneNprobe = autoNprobe != 0
	return &m, nil
}

func (h *harness) hasSidecarVecLayout(indexName string) (bool, error) {
	rows, err := h.conn.Query(fmt.Sprintf(`PRAGMA table_info("%s")`, vecindex.MembersTable(indexName)))
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid       int
			name      string
			ctype     string
			notnull   int
			dfltValue interface{}
			pk        int
		)
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dfltValue, &pk); err != nil {
			return false, err
		}
		if name == "vec" {
			return true, nil
		}
	}
	return false, rows.Err()
}

func (h *harness) loadCentroidSet(indexName string) (*kmeans.CentroidSet, error) {
	table := vecindex.CentroidsTable(indexName)
	row := h.conn.QueryRow(fmt.Sprintf(
		`SELECT version, compression, centroids FROM "%s" WHERE index_id = 1`, table))
	var version int64
	var compression string
	var blob []byte
	if err := row.Scan(&version, &compression, &blob); err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	switch compression {
	case "zstd":
		return vecindex.DecodeCentroidBlob(blob)
	case "none":
		return kmeans.DecodeCentroidSet(blob)
	default:
		return nil, fmt.Errorf("unknown compression %q", compression)
	}
}

func (h *harness) runQueryPhase() error {
	if h.cfg.nQueries == 0 || h.cfg.nQueries > h.test.Len() {
		h.cfg.nQueries = h.test.Len()
	}
	if h.cfg.warmup > h.test.Len() {
		h.cfg.warmup = h.test.Len()
	}

	sess := &protocol.ConnectionSession{
		CurrentDatabase: h.cfg.dbName,
		ConnID:          42,
		VecVars:         vecindex.DefaultVecSessionVars(),
	}
	sess.VecVars.UseGoRank = h.cfg.useGoRank
	sess.VecVars.UseCache = h.cfg.useCache
	sess.VecVars.Fallback = false
	h.handler.BenchConfigureSharedScan(h.cfg.sharedScanWindow, h.cfg.sharedScanMaxReq, h.cfg.sharedScanMaxUnion)

	// Vitess parses double-quoted literals as strings, not identifiers.
	// Use backticks (MySQL identifier) or unquoted names — matching the
	// existing coordinator bench which is known to round-trip through rewriting.
	querySQL := fmt.Sprintf(
		"SELECT id FROM `%s` WHERE vec_match(`%s`, ?, %d) ORDER BY vec_distance(`%s`, ?) LIMIT %d",
		h.cfg.tableName, h.cfg.columnName, h.cfg.k, h.cfg.columnName, h.cfg.k)

	if h.cfg.warmup > 0 {
		h.handler.BenchResetSharedScanStats()
		plog("warming %d queries ...", h.cfg.warmup)
		ws := time.Now()
		if _, err := h.runQueries(sess, querySQL, h.cfg.warmup, "warm"); err != nil {
			return fmt.Errorf("warmup: %w", err)
		}
		plog("  warmup done in %s", time.Since(ws))
		logSharedScanStats("warmup", h.handler.BenchSharedScanStats())
		logMemorySnapshot("after warmup")
	}

	h.handler.BenchResetSharedScanStats()
	plog("measurement: %d queries (concurrency=%d) ...", h.cfg.nQueries, h.cfg.queryConc)
	measureStart := time.Now()
	stats, err := h.runQueries(sess, querySQL, h.cfg.nQueries, "measure")
	if err != nil {
		return fmt.Errorf("measurement: %w", err)
	}
	wallElapsed := time.Since(measureStart)

	sort.Slice(stats.lats, func(i, j int) bool { return stats.lats[i] < stats.lats[j] })
	p := func(q float64) time.Duration { return stats.lats[int(float64(len(stats.lats)-1)*q)] }

	// Two QPS views:
	//   - aggregate (wall): throughput actually delivered — queries / wall,
	//     the number a caller sees. With concurrency>1 this reflects parallel
	//     execution.
	//   - per-worker (cpu-equivalent): sum-of-latencies / queries for a pure
	//     single-thread rate. Useful to detect whether extra workers scale
	//     linearly or saturate a bottleneck.
	aggregateQPS := float64(len(stats.lats)) / wallElapsed.Seconds()
	var latSum time.Duration
	for _, d := range stats.lats {
		latSum += d
	}
	perWorkerQPS := float64(len(stats.lats)) / latSum.Seconds()
	plog("=== results ===")
	plog("  config: nlist=%d nprobe=%d metric=%s dim=%d K=%d concurrency=%d",
		h.cfg.nlist, h.cfg.nprobe, h.cfg.metric, h.meta.Dim, h.cfg.k, h.cfg.queryConc)
	plog("  shared-scan: window=%s max-requests=%d max-union=%d",
		h.cfg.sharedScanWindow, h.cfg.sharedScanMaxReq, h.cfg.sharedScanMaxUnion)
	plog("  recall@%d      = %.4f  (top-%d vs truth top-%d)", h.cfg.k, stats.recall10, h.cfg.k, h.cfg.k)
	plog("  recall@%d-in-100 = %.4f  (top-%d vs truth top-100)", h.cfg.k, stats.recall10in100, h.cfg.k)
	plog("  latency: p50=%s p95=%s p99=%s p999=%s max=%s",
		p(0.50), p(0.95), p(0.99), p(0.999), stats.lats[len(stats.lats)-1])
	plog("  throughput: %.0f QPS (aggregate wall, concurrency=%d, n=%d, elapsed=%s)",
		aggregateQPS, h.cfg.queryConc, len(stats.lats), wallElapsed)
	plog("             %.0f QPS (per-worker, sum-of-latencies / n — extrapolated 1-thread rate)",
		perWorkerQPS)
	for wid, n := range stats.workerQueries {
		plog("  read worker[%d]: queries=%d qps=%.2f",
			wid, n, float64(n)/wallElapsed.Seconds())
	}
	logSharedScanStats("measure", h.handler.BenchSharedScanStats())
	logMemorySnapshot("after measurement")
	return nil
}

func logSharedScanStats(label string, stats coordinator.VecSharedScanStats) {
	if stats.ExecuteCalls == 0 && stats.ProbeRefreshFallbacks == 0 {
		return
	}
	avgBatch := ratio(stats.BatchRequestsTotal, stats.SharedBatches)
	avgUnion := ratio(stats.BatchUnionClustersTotal, stats.SharedBatches)
	clusterReuse := ratio(stats.BatchRequestedClustersTotal, stats.BatchUnionClustersTotal)
	sharedRate := ratio(stats.SharedRequests, stats.ExecuteCalls)
	plog("  shared-scan[%s]: calls=%d shared-req=%d (%.2f%%) shared-batches=%d singleton-fallbacks=%d oversized=%d probe-refresh=%d",
		label,
		stats.ExecuteCalls,
		stats.SharedRequests,
		100*sharedRate,
		stats.SharedBatches,
		stats.SingletonFallbacks,
		stats.OversizedRequestFallbacks,
		stats.ProbeRefreshFallbacks)
	plog("                  avg-batch=%.2f max-batch=%d avg-union=%.2f max-union=%d cluster-reuse=%.2fx scan-clusters=%d scan-rows=%d seals(timer=%d maxReq=%d maxUnion=%d)",
		avgBatch,
		stats.MaxBatchSize,
		avgUnion,
		stats.MaxUnionClusters,
		clusterReuse,
		stats.ScanClusters,
		stats.ScanRows,
		stats.SealByTimer,
		stats.SealByMaxRequests,
		stats.SealByMaxUnion)
}

func ratio(num, den int64) float64 {
	if den == 0 {
		return 0
	}
	return float64(num) / float64(den)
}

// runQueries executes nQueries queries from h.test, captures a CPU profile
// tagged by profileTag, and returns per-query latencies + recall metrics.
func (h *harness) runQueries(sess *protocol.ConnectionSession, querySQL string, nQueries int, profileTag string) (*queryRunStats, error) {
	parsedAST, err := protocol.ParseVitessAST(querySQL)
	if err != nil {
		return nil, fmt.Errorf("parse query AST: %w", err)
	}
	var cpuPath string
	var cpuF *os.File
	if h.cfg.profileCPU {
		cpuPath = filepath.Join(h.cfg.profileDir, fmt.Sprintf("cpu-query-%s.pb.gz", profileTag))
		cpuF, err = os.Create(cpuPath)
		if err != nil {
			return nil, fmt.Errorf("create cpu profile: %w", err)
		}
		if err := pprof.StartCPUProfile(cpuF); err != nil {
			cpuF.Close()
			return nil, fmt.Errorf("start cpu profile: %w", err)
		}
		defer func() {
			pprof.StopCPUProfile()
			cpuF.Close()
		}()
	}

	workers := h.cfg.queryConc
	if workers < 1 {
		workers = 1
	}

	// Pre-build truth sets so each query-side goroutine does only the
	// measurement work and isn't racing on shared map construction.
	type truthPair struct {
		top10  map[int64]bool
		top100 map[int64]bool
	}
	truths := make([]truthPair, nQueries)
	for q := 0; q < nQueries; q++ {
		gt := h.gt.Vector(q)
		t10 := make(map[int64]bool, h.cfg.k)
		t100 := make(map[int64]bool, len(gt))
		for i, id := range gt {
			mapped := int64(id) + 1
			t100[mapped] = true
			if i < h.cfg.k {
				t10[mapped] = true
			}
		}
		truths[q] = truthPair{top10: t10, top100: t100}
	}

	lats := make([]time.Duration, nQueries)
	hits10 := make([]int, workers)
	hits10in100 := make([]int, workers)
	workerQueries := make([]int64, workers)
	progress := time.Now()
	var done atomic.Int64
	baseStmt := protocol.Statement{
		SQL:       querySQL,
		Type:      protocol.StatementSelect,
		Database:  h.cfg.dbName,
		ParsedAST: parsedAST,
	}

	jobs := make(chan int, workers*2)
	var jobErr atomic.Value
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for q := range jobs {
				if jobErr.Load() != nil {
					return
				}
				qb := h.test.VectorBytes(q)
				params := []interface{}{qb, qb}
				stmt := baseStmt

				start := time.Now()
				info, args, err := h.handler.BenchMaybeRewriteVectorSelect(stmt, params, sess)
				if err != nil {
					jobErr.Store(fmt.Errorf("rewrite q=%d: %w", q, err))
					return
				}
				if info == nil {
					jobErr.Store(fmt.Errorf("nil rewrite info q=%d", q))
					return
				}
				rs, err := h.handler.BenchExecuteVectorPlan(stmt, info, args, protocol.ConsistencyLocalOne)
				if err != nil {
					jobErr.Store(fmt.Errorf("execute q=%d: %w", q, err))
					return
				}
				lats[q] = time.Since(start)
				workerQueries[wid]++

				t := truths[q]
				for _, row := range rs.Rows {
					if len(row) == 0 {
						continue
					}
					id := toInt64(row[0])
					if t.top10[id] {
						hits10[wid]++
					}
					if t.top100[id] {
						hits10in100[wid]++
					}
				}
				if d := done.Add(1); d%1000 == 0 || d == int64(nQueries) {
					plog("  queried %d/%d (%.1fs)", d, nQueries, time.Since(progress).Seconds())
				}
			}
		}(w)
	}

	for q := 0; q < nQueries; q++ {
		jobs <- q
	}
	close(jobs)
	wg.Wait()

	if e := jobErr.Load(); e != nil {
		return nil, e.(error)
	}

	totalHits10, totalHits100 := 0, 0
	for w := 0; w < workers; w++ {
		totalHits10 += hits10[w]
		totalHits100 += hits10in100[w]
	}

	recall10 := float64(totalHits10) / float64(nQueries*h.cfg.k)
	recall10in100 := float64(totalHits100) / float64(nQueries*h.cfg.k)
	if h.cfg.profileCPU {
		plog("  cpu profile: %s", cpuPath)
	}
	return &queryRunStats{
		lats:          lats,
		recall10:      recall10,
		recall10in100: recall10in100,
		workerQueries: workerQueries,
	}, nil
}

func sqliteCacheKB(mb int) int {
	if mb <= 0 {
		return 0
	}
	return mb * 1024
}

func waitIndexReady(conn *sql.DB, indexName string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var status string
		err := conn.QueryRow(
			`SELECT status FROM __marmot_vector_indexes WHERE index_name = ?`, indexName,
		).Scan(&status)
		if err == nil && status == "ready" {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("index %q did not become ready within %s", indexName, timeout)
}

func toInt64(v interface{}) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int32:
		return int64(x)
	case int:
		return int64(x)
	case []byte:
		// Some drivers return INTEGERs as bytes; try to parse.
		n, _ := parseInt64(string(x))
		return n
	case string:
		n, _ := parseInt64(x)
		return n
	}
	return 0
}

func parseInt64(s string) (int64, error) {
	var n int64
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}

func metricFromString(s string) vecindex.Metric {
	switch s {
	case "cosine":
		return vecindex.MetricCosine
	case "dot":
		return vecindex.MetricDot
	default:
		return vecindex.MetricL2
	}
}
