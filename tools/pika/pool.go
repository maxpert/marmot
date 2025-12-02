package main

import (
	"database/sql"
	"fmt"
	"regexp"
	"sync/atomic"

	_ "github.com/go-sql-driver/mysql"
)

// validTableName validates table names to prevent SQL injection.
// Only allows alphanumeric characters and underscores, starting with a letter or underscore.
var validTableName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// Pool manages connections across multiple cluster nodes with round-robin distribution.
type Pool struct {
	dbs     []*sql.DB
	hosts   []string
	counter uint64
}

// NewPool creates a connection pool across the given hosts.
func NewPool(hosts []string, database string, maxConnsPerHost int) (*Pool, error) {
	if len(hosts) == 0 {
		return nil, fmt.Errorf("no hosts provided")
	}

	p := &Pool{
		dbs:   make([]*sql.DB, len(hosts)),
		hosts: hosts,
	}

	for i, host := range hosts {
		dsn := fmt.Sprintf("root@tcp(%s)/%s?interpolateParams=true", host, database)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			p.Close()
			return nil, fmt.Errorf("failed to open connection to %s: %w", host, err)
		}

		db.SetMaxOpenConns(maxConnsPerHost)
		db.SetMaxIdleConns(maxConnsPerHost)

		if err := db.Ping(); err != nil {
			p.Close()
			return nil, fmt.Errorf("failed to ping %s: %w", host, err)
		}

		p.dbs[i] = db
	}

	return p, nil
}

// Get returns a database connection using round-robin selection.
func (p *Pool) Get() *sql.DB {
	idx := atomic.AddUint64(&p.counter, 1) % uint64(len(p.dbs))
	return p.dbs[idx]
}

// GetByIndex returns a specific database connection by index.
func (p *Pool) GetByIndex(idx int) *sql.DB {
	return p.dbs[idx%len(p.dbs)]
}

// Size returns the number of hosts in the pool.
func (p *Pool) Size() int {
	return len(p.dbs)
}

// Hosts returns the list of hosts.
func (p *Pool) Hosts() []string {
	return p.hosts
}

// Close closes all connections.
func (p *Pool) Close() error {
	var lastErr error
	for _, db := range p.dbs {
		if db != nil {
			if err := db.Close(); err != nil {
				lastErr = err
			}
		}
	}
	return lastErr
}

// CreateTable creates the benchmark table using round-robin node selection.
// DDL replicates to other nodes via Marmot.
func (p *Pool) CreateTable(table string, dropExisting bool) error {
	if !validTableName.MatchString(table) {
		return fmt.Errorf("invalid table name: %s (must be alphanumeric with underscores, starting with letter or underscore)", table)
	}

	db := p.Get() // Use round-robin instead of always first node

	if dropExisting {
		if _, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table)); err != nil {
			return fmt.Errorf("failed to drop table: %w", err)
		}
	}

	createSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		id VARCHAR(64) PRIMARY KEY,
		field0 TEXT,
		field1 TEXT,
		field2 TEXT,
		field3 TEXT,
		field4 TEXT,
		field5 TEXT,
		field6 TEXT,
		field7 TEXT,
		field8 TEXT,
		field9 TEXT
	)`, table)

	if _, err := db.Exec(createSQL); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

// GetRowCount returns the number of rows in the table using round-robin.
func (p *Pool) GetRowCount(table string) (int64, error) {
	if !validTableName.MatchString(table) {
		return 0, fmt.Errorf("invalid table name: %s", table)
	}
	var count int64
	err := p.Get().QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
	return count, err
}
