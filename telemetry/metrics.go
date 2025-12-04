package telemetry

// Histogram bucket definitions for different latency profiles
var (
	// WriteTxnBuckets for distributed write transactions (network + consensus)
	WriteTxnBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

	// ReadTxnBuckets for local SQLite reads
	ReadTxnBuckets = []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25}

	// TwoPCBuckets for 2PC phase latencies
	TwoPCBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5}

	// SyncBuckets for anti-entropy and background sync operations
	SyncBuckets = []float64{0.1, 0.5, 1, 2.5, 5, 10, 30, 60}

	// QuorumAckBuckets for number of quorum acknowledgments
	QuorumAckBuckets = []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
)

// Cluster Health Metrics
var (
	// ClusterNodes tracks node count by status (ALIVE, SUSPECT, DEAD, JOINING, REMOVED)
	ClusterNodes GaugeVec = noopGaugeVec{}

	// ClusterQuorumAvailable indicates if quorum is achievable (1=yes, 0=no)
	ClusterQuorumAvailable Gauge = NoopStat{}

	// GossipRoundsTotal counts total gossip rounds executed
	GossipRoundsTotal Counter = NoopStat{}

	// GossipMessagesTotal counts gossip messages by direction (sent, received)
	GossipMessagesTotal CounterVec = noopCounterVec{}

	// GossipFailuresTotal counts failed gossip send attempts
	GossipFailuresTotal Counter = NoopStat{}

	// NodeStateTransitionsTotal counts state transitions (from -> to)
	NodeStateTransitionsTotal CounterVec = noopCounterVec{}

	// ClusterJoinTotal counts cluster join attempts by result (success, failed)
	ClusterJoinTotal CounterVec = noopCounterVec{}
)

// 2PC/Replication Metrics
var (
	// TxnTotal counts transactions by type (write, read) and result (success, failed, conflict)
	TxnTotal CounterVec = noopCounterVec{}

	// TxnDurationSeconds measures transaction latency by type
	TxnDurationSeconds HistogramVec = noopHistogramVec{}

	// TwoPhasePrepareSeconds measures 2PC prepare phase latency
	TwoPhasePrepareSeconds Histogram = NoopStat{}

	// TwoPhaseCommitSeconds measures 2PC commit phase latency
	TwoPhaseCommitSeconds Histogram = NoopStat{}

	// TwoPhaseQuorumAcks measures number of acks received per phase
	TwoPhaseQuorumAcks HistogramVec = noopHistogramVec{}

	// WriteConflictsTotal counts write conflicts by type (mvcc, intent) and path (fast, slow)
	WriteConflictsTotal CounterVec = noopCounterVec{}

	// IntentFilterChecks counts intent filter checks by result (fast_path, slow_path_miss, slow_path_conflict)
	IntentFilterChecks CounterVec = noopCounterVec{}

	// IntentFilterSize tracks current number of entries in the intent filter
	IntentFilterSize Gauge = NoopStat{}

	// IntentFilterFalsePositives counts false positives (slow path found no conflict)
	IntentFilterFalsePositives Counter = NoopStat{}

	// IntentFilterTxnCount tracks number of transactions with active intents
	IntentFilterTxnCount Gauge = NoopStat{}

	// ReplicationRequestsTotal counts replication requests by phase and result
	ReplicationRequestsTotal CounterVec = noopCounterVec{}

	// ActiveTransactions tracks currently active transactions
	ActiveTransactions Gauge = NoopStat{}
)

// Query Processing Metrics
var (
	// QueriesTotal counts queries by type (select, insert, update, delete, ddl) and result
	QueriesTotal CounterVec = noopCounterVec{}

	// QueryDurationSeconds measures query latency by type
	QueryDurationSeconds HistogramVec = noopHistogramVec{}

	// RowsAffected measures rows affected per write query
	RowsAffected Histogram = NoopStat{}

	// RowsReturned measures rows returned per read query
	RowsReturned Histogram = NoopStat{}

	// MySQLConnections tracks active MySQL protocol connections
	MySQLConnections Gauge = NoopStat{}

	// DDLOperationsTotal counts DDL operations by result
	DDLOperationsTotal CounterVec = noopCounterVec{}

	// DDLLockWaitSeconds measures time waiting for DDL lock
	DDLLockWaitSeconds Histogram = NoopStat{}
)

// Anti-Entropy Metrics
var (
	// AntiEntropyRoundsTotal counts anti-entropy rounds executed
	AntiEntropyRoundsTotal Counter = NoopStat{}

	// AntiEntropySyncsTotal counts syncs by type (delta, snapshot) and result
	AntiEntropySyncsTotal CounterVec = noopCounterVec{}

	// AntiEntropyDurationSeconds measures anti-entropy round duration
	AntiEntropyDurationSeconds Histogram = NoopStat{}

	// ReplicationLagTxns tracks transaction lag per peer
	ReplicationLagTxns GaugeVec = noopGaugeVec{}

	// DeltaSyncTxnsTotal counts transactions applied via delta sync
	DeltaSyncTxnsTotal Counter = NoopStat{}
)

// InitMetrics initializes all Prometheus metrics.
// Must be called after InitializeTelemetry().
func InitMetrics() {
	// Cluster Health Metrics
	ClusterNodes = NewGaugeVec(
		"cluster_nodes",
		"Number of nodes in cluster by status",
		[]string{"status"},
	)
	ClusterQuorumAvailable = NewGauge(
		"cluster_quorum_available",
		"Whether quorum is achievable (1=yes, 0=no)",
	)
	GossipRoundsTotal = NewCounter(
		"gossip_rounds_total",
		"Total number of gossip rounds executed",
	)
	GossipMessagesTotal = NewCounterVec(
		"gossip_messages_total",
		"Total gossip messages by direction",
		[]string{"direction"},
	)
	GossipFailuresTotal = NewCounter(
		"gossip_failures_total",
		"Total failed gossip send attempts",
	)
	NodeStateTransitionsTotal = NewCounterVec(
		"node_state_transitions_total",
		"Node state transitions",
		[]string{"from", "to"},
	)
	ClusterJoinTotal = NewCounterVec(
		"cluster_join_total",
		"Cluster join attempts by result",
		[]string{"result"},
	)

	// 2PC/Replication Metrics
	TxnTotal = NewCounterVec(
		"txn_total",
		"Total transactions by type and result",
		[]string{"type", "result"},
	)
	TxnDurationSeconds = NewHistogramVec(
		"txn_duration_seconds",
		"Transaction duration in seconds",
		[]string{"type"},
		WriteTxnBuckets,
	)
	TwoPhasePrepareSeconds = NewHistogramWithBuckets(
		"twophase_prepare_seconds",
		"2PC prepare phase duration in seconds",
		TwoPCBuckets,
	)
	TwoPhaseCommitSeconds = NewHistogramWithBuckets(
		"twophase_commit_seconds",
		"2PC commit phase duration in seconds",
		TwoPCBuckets,
	)
	TwoPhaseQuorumAcks = NewHistogramVec(
		"twophase_quorum_acks",
		"Number of quorum acknowledgments received",
		[]string{"phase"},
		QuorumAckBuckets,
	)
	WriteConflictsTotal = NewCounterVec(
		"write_conflicts_total",
		"Write conflicts by type and path",
		[]string{"type", "path"},
	)
	IntentFilterChecks = NewCounterVec(
		"intent_filter_checks_total",
		"Intent filter checks by result",
		[]string{"result"},
	)
	IntentFilterSize = NewGauge(
		"intent_filter_size",
		"Current number of entries in intent filter",
	)
	IntentFilterFalsePositives = NewCounter(
		"intent_filter_false_positives_total",
		"Intent filter false positives (slow path found no conflict)",
	)
	IntentFilterTxnCount = NewGauge(
		"intent_filter_txn_count",
		"Number of transactions with active intents in filter",
	)
	ReplicationRequestsTotal = NewCounterVec(
		"replication_requests_total",
		"Replication requests by phase and result",
		[]string{"phase", "result"},
	)
	ActiveTransactions = NewGauge(
		"active_transactions",
		"Number of currently active transactions",
	)

	// Query Processing Metrics
	QueriesTotal = NewCounterVec(
		"queries_total",
		"Total queries by type and result",
		[]string{"type", "result"},
	)
	QueryDurationSeconds = NewHistogramVec(
		"query_duration_seconds",
		"Query duration in seconds",
		[]string{"type"},
		WriteTxnBuckets,
	)
	RowsAffected = NewHistogram(
		"rows_affected",
		"Number of rows affected per write query",
	)
	RowsReturned = NewHistogram(
		"rows_returned",
		"Number of rows returned per read query",
	)
	MySQLConnections = NewGauge(
		"mysql_connections",
		"Number of active MySQL protocol connections",
	)
	DDLOperationsTotal = NewCounterVec(
		"ddl_operations_total",
		"DDL operations by result",
		[]string{"result"},
	)
	DDLLockWaitSeconds = NewHistogramWithBuckets(
		"ddl_lock_wait_seconds",
		"Time waiting for DDL lock in seconds",
		TwoPCBuckets,
	)

	// Anti-Entropy Metrics
	AntiEntropyRoundsTotal = NewCounter(
		"antientropy_rounds_total",
		"Total anti-entropy rounds executed",
	)
	AntiEntropySyncsTotal = NewCounterVec(
		"antientropy_syncs_total",
		"Anti-entropy syncs by type and result",
		[]string{"type", "result"},
	)
	AntiEntropyDurationSeconds = NewHistogramWithBuckets(
		"antientropy_duration_seconds",
		"Anti-entropy round duration in seconds",
		SyncBuckets,
	)
	ReplicationLagTxns = NewGaugeVec(
		"replication_lag_txns",
		"Transaction lag behind peer",
		[]string{"peer"},
	)
	DeltaSyncTxnsTotal = NewCounter(
		"delta_sync_txns_total",
		"Total transactions applied via delta sync",
	)
}
