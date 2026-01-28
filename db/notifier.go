package db

// CDCSignal represents notification that CDC data is available.
// Lightweight signal (24 bytes) - actual data pulled from MetaStore.
type CDCSignal struct {
	Database string
	TxnID    uint64
}

// CDCFilter specifies which signals a subscriber wants.
type CDCFilter struct {
	Databases []string // nil or empty = all databases
}

// CDCNotifier is called when CDC data is committed.
// Used by TransactionManager to signal subscribers.
type CDCNotifier interface {
	Signal(database string, txnID uint64)
}

// CDCSubscriber allows subscribing to CDC signals.
// Used by gRPC server to receive change notifications.
type CDCSubscriber interface {
	Subscribe(filter CDCFilter) (signals <-chan CDCSignal, cancel func())
}

// CDCHub combines both interfaces - the full notification system.
type CDCHub interface {
	CDCNotifier
	CDCSubscriber
}
