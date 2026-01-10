package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// RowMismatch describes a mismatch for a specific row key across hosts.
type RowMismatch struct {
	Key      string
	HostData map[string]string // host -> checksum or "NOT FOUND"
}

// VerifyResult holds verification results.
type VerifyResult struct {
	RowCounts      map[string]int64 // host -> count
	CountsMatch    bool
	SampledRows    int
	MatchedRows    int
	MismatchedRows int
	Mismatches     []RowMismatch // first N mismatches with details
}

// Verifier checks cluster consistency using a connection pool.
type Verifier struct {
	pool    *Pool
	table   string
	samples int
	timeout time.Duration
}

// NewVerifier creates a new Verifier.
func NewVerifier(pool *Pool, table string, samples int, timeout time.Duration) *Verifier {
	return &Verifier{
		pool:    pool,
		table:   table,
		samples: samples,
		timeout: timeout,
	}
}

// Verify runs the consistency check.
func (v *Verifier) Verify(ctx context.Context) (*VerifyResult, error) {
	result := &VerifyResult{
		RowCounts:  make(map[string]int64),
		Mismatches: make([]RowMismatch, 0),
	}

	// Step 1: Get row counts from each host
	if err := v.getRowCounts(ctx, result); err != nil {
		return nil, fmt.Errorf("failed to get row counts: %w", err)
	}

	// Check if counts match
	result.CountsMatch = v.checkCountsMatch(result.RowCounts)

	// Step 2: Sample random keys from first host
	keys, err := v.sampleKeys(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to sample keys: %w", err)
	}

	result.SampledRows = len(keys)

	// Step 3: Verify each sampled key across all hosts
	if err := v.verifyKeys(ctx, keys, result); err != nil {
		return nil, fmt.Errorf("failed to verify keys: %w", err)
	}

	return result, nil
}

// getRowCounts queries row count from each host using the pool.
func (v *Verifier) getRowCounts(ctx context.Context, result *VerifyResult) error {
	hosts := v.pool.Hosts()
	for i, host := range hosts {
		db := v.pool.GetByIndex(i)

		queryCtx, cancel := context.WithTimeout(ctx, v.timeout)
		var count int64
		query := fmt.Sprintf("SELECT COUNT(*) FROM %s", v.table)
		err := db.QueryRowContext(queryCtx, query).Scan(&count)
		cancel()

		if err != nil {
			return fmt.Errorf("host %s: %w", host, err)
		}
		result.RowCounts[host] = count
	}
	return nil
}

// checkCountsMatch checks if all hosts have the same row count.
func (v *Verifier) checkCountsMatch(counts map[string]int64) bool {
	if len(counts) == 0 {
		return true
	}

	var firstCount int64
	first := true
	for _, count := range counts {
		if first {
			firstCount = count
			first = false
			continue
		}
		if count != firstCount {
			return false
		}
	}
	return true
}

// sampleKeys samples random keys from the first host.
func (v *Verifier) sampleKeys(ctx context.Context) ([]string, error) {
	db := v.pool.GetByIndex(0)

	queryCtx, cancel := context.WithTimeout(ctx, v.timeout)
	defer cancel()

	// Sample random keys using ORDER BY RANDOM() LIMIT N
	query := fmt.Sprintf("SELECT id FROM %s ORDER BY RANDOM() LIMIT ?", v.table)
	rows, err := db.QueryContext(queryCtx, query, v.samples)
	if err != nil {
		return nil, fmt.Errorf("failed to sample keys: %w", err)
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("failed to scan key: %w", err)
		}
		keys = append(keys, key)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	return keys, nil
}

// verifyKeys verifies each key across all hosts using the pool.
func (v *Verifier) verifyKeys(ctx context.Context, keys []string, result *VerifyResult) error {
	const maxMismatches = 10
	hosts := v.pool.Hosts()

	for _, key := range keys {
		checksums := make(map[string]string)

		// Query each host for this key
		for i, host := range hosts {
			db := v.pool.GetByIndex(i)
			checksum, err := v.getRowChecksum(ctx, db, key)
			if err != nil {
				return fmt.Errorf("host %s, key %s: %w", host, key, err)
			}
			checksums[host] = checksum
		}

		// Check if all checksums match
		if v.allMatch(checksums) {
			result.MatchedRows++
		} else {
			result.MismatchedRows++
			if len(result.Mismatches) < maxMismatches {
				result.Mismatches = append(result.Mismatches, RowMismatch{
					Key:      key,
					HostData: checksums,
				})
			}
		}
	}

	return nil
}

// getRowChecksum gets the checksum for a specific row.
func (v *Verifier) getRowChecksum(ctx context.Context, db *sql.DB, key string) (string, error) {
	queryCtx, cancel := context.WithTimeout(ctx, v.timeout)
	defer cancel()

	// Build checksum query: concatenate all fields and compute hex
	query := fmt.Sprintf(`SELECT HEX(
		COALESCE(field0, '') || COALESCE(field1, '') || COALESCE(field2, '') ||
		COALESCE(field3, '') || COALESCE(field4, '') || COALESCE(field5, '') ||
		COALESCE(field6, '') || COALESCE(field7, '') || COALESCE(field8, '') ||
		COALESCE(field9, '')
	) as checksum FROM %s WHERE id = ?`, v.table)

	var checksum sql.NullString
	err := db.QueryRowContext(queryCtx, query, key).Scan(&checksum)
	if err == sql.ErrNoRows {
		return "NOT FOUND", nil
	}
	if err != nil {
		return "", fmt.Errorf("query failed: %w", err)
	}

	if !checksum.Valid {
		return "NULL", nil
	}

	// Truncate checksum for display (first 16 chars)
	cs := checksum.String
	if len(cs) > 16 {
		cs = cs[:16] + "..."
	}
	return cs, nil
}

// allMatch checks if all values in the map are identical.
func (v *Verifier) allMatch(data map[string]string) bool {
	if len(data) == 0 {
		return true
	}

	var firstValue string
	first := true
	for _, value := range data {
		if first {
			firstValue = value
			first = false
			continue
		}
		if value != firstValue {
			return false
		}
	}
	return true
}

// Print outputs verification results.
func (r *VerifyResult) Print() {
	fmt.Println()
	fmt.Println("Row Counts:")
	for host, count := range r.RowCounts {
		fmt.Printf("  %s: %d\n", host, count)
	}
	if r.CountsMatch {
		fmt.Println("  Status: MATCH")
	} else {
		fmt.Println("  Status: MISMATCH")
	}

	fmt.Println()
	fmt.Println("Sampled Verification:")
	fmt.Printf("  Sampled rows: %d\n", r.SampledRows)
	fmt.Printf("  Matching:     %d\n", r.MatchedRows)
	fmt.Printf("  Mismatched:   %d\n", r.MismatchedRows)

	if len(r.Mismatches) > 0 {
		fmt.Println()
		fmt.Println("Mismatches:")
		for _, m := range r.Mismatches {
			fmt.Printf("  Key: %s\n", m.Key)
			for host, data := range m.HostData {
				fmt.Printf("    %s: %s\n", host, data)
			}
		}
	}
}

// HasMismatches returns true if there are any mismatches.
func (r *VerifyResult) HasMismatches() bool {
	return !r.CountsMatch || r.MismatchedRows > 0
}

// executeVerify runs the verification.
func executeVerify(ctx context.Context, cfg *Config) error {
	fmt.Println("╔══════════════════════════════════════════════════════╗")
	fmt.Println("║            Pika Cluster Verification                 ║")
	fmt.Println("╚══════════════════════════════════════════════════════╝")
	fmt.Println()

	fmt.Printf("Hosts:    %s\n", cfg.Hosts)
	fmt.Printf("Database: %s\n", cfg.Database)
	fmt.Printf("Table:    %s\n", cfg.Table)
	fmt.Printf("Samples:  %d\n", cfg.VerifySamples)
	fmt.Printf("Timeout:  %s\n", cfg.VerifyTimeout)

	hosts := cfg.HostList()
	if len(hosts) < 2 {
		return fmt.Errorf("verification requires at least 2 hosts")
	}

	// Validate table name
	if !validTableName.MatchString(cfg.Table) {
		return fmt.Errorf("invalid table name: %s", cfg.Table)
	}

	// Create pool with 1 connection per host for verification
	pool, err := NewPool(hosts, cfg.Database, 1)
	if err != nil {
		return fmt.Errorf("failed to create connection pool: %w", err)
	}
	defer pool.Close()

	verifier := NewVerifier(pool, cfg.Table, cfg.VerifySamples, cfg.VerifyTimeout)

	fmt.Println()
	fmt.Println("Verifying cluster consistency...")

	result, err := verifier.Verify(ctx)
	if err != nil {
		return fmt.Errorf("verification failed: %w", err)
	}

	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════")
	fmt.Println("              VERIFICATION RESULTS                     ")
	fmt.Println("═══════════════════════════════════════════════════════")
	result.Print()

	if result.HasMismatches() {
		return fmt.Errorf("cluster verification failed: found inconsistencies")
	}

	fmt.Println()
	fmt.Println("Cluster verification passed!")
	return nil
}
