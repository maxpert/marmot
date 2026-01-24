package telemetry

import (
	"sync"
	"time"
)

// StatsProvider interface for components that provide stats
type StatsProvider interface {
	GetRowLockStats() (activeLocks, activeTransactions, gcMarkers, tablesWithLocks int)
	IntentStats() (pendingIntents int, err error)
}

// DatabaseLister interface for listing databases
type DatabaseLister interface {
	ListDatabases() []string
	GetDatabase(name string) StatsProvider
}

// MetricsCollector periodically collects stats and updates telemetry gauges
type MetricsCollector struct {
	dbLister DatabaseLister
	interval time.Duration
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector(dbLister DatabaseLister, interval time.Duration) *MetricsCollector {
	return &MetricsCollector{
		dbLister: dbLister,
		interval: interval,
		stopCh:   make(chan struct{}),
	}
}

// Start begins the periodic collection
func (mc *MetricsCollector) Start() {
	mc.wg.Add(1)
	go mc.collectLoop()
}

// Stop stops the collector
func (mc *MetricsCollector) Stop() {
	close(mc.stopCh)
	mc.wg.Wait()
}

func (mc *MetricsCollector) collectLoop() {
	defer mc.wg.Done()

	ticker := time.NewTicker(mc.interval)
	defer ticker.Stop()

	mc.collect()

	for {
		select {
		case <-ticker.C:
			mc.collect()
		case <-mc.stopCh:
			return
		}
	}
}

func (mc *MetricsCollector) collect() {
	if mc.dbLister == nil {
		return
	}

	var totalLocks, totalTxns, totalGC, totalTables, totalIntents int

	for _, dbName := range mc.dbLister.ListDatabases() {
		provider := mc.dbLister.GetDatabase(dbName)
		if provider == nil {
			continue
		}

		locks, txns, gc, tables := provider.GetRowLockStats()
		totalLocks += locks
		totalTxns += txns
		totalGC += gc
		totalTables += tables

		if intents, err := provider.IntentStats(); err == nil {
			totalIntents += intents
		}
	}

	UpdateRowLockStats(totalLocks, totalTxns, totalGC, totalTables)
	PendingIntents.Set(float64(totalIntents))
}
