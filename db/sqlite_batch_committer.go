package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jizhuozhi/go-future"
	"github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
)

type pendingCommit struct {
	cdcEntries []*IntentEntry
	stmts      []protocol.Statement
	promise    *future.Promise[error]
	err        error
}

type SQLiteBatchCommitter struct {
	dbPath string
	db     *sql.DB

	mu      sync.Mutex
	pending map[uint64]*pendingCommit

	maxBatchSize int
	maxWaitTime  time.Duration

	stopCh  chan struct{}
	stopped atomic.Bool
	wg      sync.WaitGroup
}

func NewSQLiteBatchCommitter(dbPath string, maxBatchSize int, maxWaitTime time.Duration) *SQLiteBatchCommitter {
	return &SQLiteBatchCommitter{
		dbPath:       dbPath,
		pending:      make(map[uint64]*pendingCommit),
		maxBatchSize: maxBatchSize,
		maxWaitTime:  maxWaitTime,
		stopCh:       make(chan struct{}),
	}
}

func (bc *SQLiteBatchCommitter) Start() error {
	db, err := bc.openOptimizedConnection()
	if err != nil {
		return fmt.Errorf("failed to open batch committer connection: %w", err)
	}
	bc.db = db

	bc.wg.Add(1)
	go bc.flushLoop()
	return nil
}

func (bc *SQLiteBatchCommitter) openOptimizedConnection() (*sql.DB, error) {
	// Build DSN with batch-optimized settings
	// WAL mode for compatibility with other connections
	// _txlock=immediate to acquire write lock at BEGIN
	dsn := bc.dbPath
	if !strings.Contains(dsn, ":memory:") {
		if strings.Contains(dsn, "?") {
			dsn += "&_journal_mode=WAL&_txlock=immediate"
		} else {
			dsn += "?_journal_mode=WAL&_txlock=immediate"
		}
	}

	db, err := sql.Open(SQLiteDriverName, dsn)
	if err != nil {
		return nil, err
	}

	// Single connection for batch writes
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	// Apply batch-optimized PRAGMAs
	// See: https://www.powersync.com/blog/sqlite-optimizations-for-ultra-high-performance
	// See: https://sqlite.org/wal.html
	pragmas := []string{
		"PRAGMA synchronous = OFF",             // Skip fsync per-commit (batch amortizes crash risk)
		"PRAGMA cache_size = -64000",           // 64MB page cache
		"PRAGMA temp_store = MEMORY",           // Temp tables in RAM
		"PRAGMA mmap_size = 268435456",         // 256MB memory-mapped I/O
		"PRAGMA journal_mode = WAL",            // WAL mode for concurrent reads
		"PRAGMA wal_autocheckpoint = 10000",    // ~40MB WAL before checkpoint (batch more writes)
		"PRAGMA journal_size_limit = 67108864", // 64MB max WAL size after checkpoint
	}

	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to set %s: %w", pragma, err)
		}
	}

	return db, nil
}

func (bc *SQLiteBatchCommitter) Stop() {
	if !bc.stopped.CompareAndSwap(false, true) {
		return
	}
	close(bc.stopCh)
	bc.wg.Wait()

	if bc.db != nil {
		bc.db.Close()
	}
}

func (bc *SQLiteBatchCommitter) Enqueue(txnID uint64, commitTS hlc.Timestamp, cdcEntries []*IntentEntry, stmts []protocol.Statement) *future.Future[error] {
	p := future.NewPromise[error]()

	bc.mu.Lock()
	bc.pending[txnID] = &pendingCommit{
		cdcEntries: cdcEntries,
		stmts:      stmts,
		promise:    p,
	}
	bc.mu.Unlock()

	return p.Future()
}

func (bc *SQLiteBatchCommitter) flushLoop() {
	defer bc.wg.Done()

	ticker := time.NewTicker(bc.maxWaitTime)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			bc.tryFlush()
		case <-bc.stopCh:
			bc.tryFlush()
			return
		}
	}
}

func (bc *SQLiteBatchCommitter) tryFlush() {
	bc.mu.Lock()
	if len(bc.pending) == 0 {
		bc.mu.Unlock()
		return
	}
	batch := bc.pending
	bc.pending = make(map[uint64]*pendingCommit)
	bc.mu.Unlock()

	bc.flush(batch)
}

func (bc *SQLiteBatchCommitter) getSchemaCache(conn *sql.Conn) (*SchemaCache, error) {
	cache := NewSchemaCache()
	err := conn.Raw(func(driverConn interface{}) error {
		sqliteConn, ok := driverConn.(*sqlite3.SQLiteConn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type: %T", driverConn)
		}
		return cache.Reload(sqliteConn)
	})
	if err != nil {
		return nil, err
	}
	return cache, nil
}

func (bc *SQLiteBatchCommitter) flush(batch map[uint64]*pendingCommit) {
	conn, err := bc.db.Conn(context.Background())
	if err != nil {
		for _, pc := range batch {
			pc.promise.Set(nil, err)
		}
		return
	}
	defer conn.Close()

	tx, err := conn.BeginTx(context.Background(), nil)
	if err != nil {
		for _, pc := range batch {
			pc.promise.Set(nil, err)
		}
		return
	}

	// Load schema inside transaction (atomic with writes)
	schemaCache, err := bc.getSchemaCache(conn)
	if err != nil {
		tx.Rollback()
		for _, pc := range batch {
			pc.promise.Set(nil, err)
		}
		return
	}

	// Apply all CDC entries
	for _, pc := range batch {
		for _, entry := range pc.cdcEntries {
			if err := bc.writeCDCDataToTx(tx, schemaCache, entry); err != nil {
				pc.err = err
				break
			}
		}
	}

	// Commit (single fsync for entire batch)
	if err := tx.Commit(); err != nil {
		tx.Rollback()
		for _, pc := range batch {
			pc.promise.Set(nil, err)
		}
		return
	}

	// Resolve all promises
	for _, pc := range batch {
		pc.promise.Set(nil, pc.err)
	}
}

func (bc *SQLiteBatchCommitter) writeCDCDataToTx(tx *sql.Tx, schemaCache *SchemaCache, entry *IntentEntry) error {
	opType := OpType(entry.Operation)

	switch opType {
	case OpTypeInsert, OpTypeReplace:
		return bc.writeCDCInsertToTx(tx, entry.Table, entry.NewValues)
	case OpTypeUpdate:
		return bc.writeCDCUpdateToTx(tx, schemaCache, entry.Table, entry.OldValues, entry.NewValues)
	case OpTypeDelete:
		return bc.writeCDCDeleteToTx(tx, schemaCache, entry.Table, entry.OldValues)
	default:
		return fmt.Errorf("unsupported operation type for CDC: %v", opType)
	}
}

func (bc *SQLiteBatchCommitter) writeCDCInsertToTx(tx *sql.Tx, tableName string, newValues map[string][]byte) error {
	if len(newValues) == 0 {
		return fmt.Errorf("no values to insert")
	}

	columns := make([]string, 0, len(newValues))
	placeholders := make([]string, 0, len(newValues))
	values := make([]interface{}, 0, len(newValues))

	for col := range newValues {
		columns = append(columns, col)
		placeholders = append(placeholders, "?")

		var value interface{}
		if err := encoding.Unmarshal(newValues[col], &value); err != nil {
			return fmt.Errorf("failed to deserialize value for column %s: %w", col, err)
		}
		values = append(values, value)
	}

	sqlStmt := fmt.Sprintf("INSERT OR REPLACE INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	_, err := tx.Exec(sqlStmt, values...)
	return err
}

func (bc *SQLiteBatchCommitter) writeCDCUpdateToTx(tx *sql.Tx, schemaCache *SchemaCache, tableName string, oldValues, newValues map[string][]byte) error {
	if len(newValues) == 0 {
		return fmt.Errorf("no values to update")
	}

	schema, err := schemaCache.GetSchemaFor(tableName)
	if err != nil {
		return fmt.Errorf("schema not found for table %s: %w", tableName, err)
	}

	setClauses := make([]string, 0, len(newValues))
	values := make([]interface{}, 0, len(newValues)+len(schema.PrimaryKeys))

	for col, valBytes := range newValues {
		setClauses = append(setClauses, fmt.Sprintf("%s = ?", col))

		var value interface{}
		if err := encoding.Unmarshal(valBytes, &value); err != nil {
			return fmt.Errorf("failed to deserialize value for column %s: %w", col, err)
		}
		values = append(values, value)
	}

	whereClauses := make([]string, 0, len(schema.PrimaryKeys))

	for _, pkCol := range schema.PrimaryKeys {
		pkBytes, ok := oldValues[pkCol]
		if !ok {
			pkBytes, ok = newValues[pkCol]
			if !ok {
				return fmt.Errorf("primary key column %s not found in CDC data", pkCol)
			}
		}

		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", pkCol))

		var value interface{}
		if err := encoding.Unmarshal(pkBytes, &value); err != nil {
			return fmt.Errorf("failed to deserialize PK value for column %s: %w", pkCol, err)
		}
		values = append(values, value)
	}

	sqlStmt := fmt.Sprintf("UPDATE %s SET %s WHERE %s",
		tableName,
		strings.Join(setClauses, ", "),
		strings.Join(whereClauses, " AND "))

	_, err = tx.Exec(sqlStmt, values...)
	return err
}

func (bc *SQLiteBatchCommitter) writeCDCDeleteToTx(tx *sql.Tx, schemaCache *SchemaCache, tableName string, oldValues map[string][]byte) error {
	if len(oldValues) == 0 {
		return fmt.Errorf("empty old values for delete")
	}

	schema, err := schemaCache.GetSchemaFor(tableName)
	if err != nil {
		return fmt.Errorf("schema not found for table %s: %w", tableName, err)
	}

	whereClauses := make([]string, 0, len(schema.PrimaryKeys))
	values := make([]interface{}, 0, len(schema.PrimaryKeys))

	for _, pkCol := range schema.PrimaryKeys {
		pkValue, ok := oldValues[pkCol]
		if !ok {
			return fmt.Errorf("primary key column %s not found in CDC old values", pkCol)
		}

		whereClauses = append(whereClauses, fmt.Sprintf("%s = ?", pkCol))

		var value interface{}
		if err := encoding.Unmarshal(pkValue, &value); err != nil {
			return fmt.Errorf("failed to deserialize PK value for column %s: %w", pkCol, err)
		}
		values = append(values, value)
	}

	sqlStmt := fmt.Sprintf("DELETE FROM %s WHERE %s",
		tableName,
		strings.Join(whereClauses, " AND "))

	_, err = tx.Exec(sqlStmt, values...)
	return err
}
