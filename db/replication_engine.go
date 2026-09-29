package db

import (
	"context"
	"errors"
	"fmt"

	"github.com/maxpert/marmot/coordinator"
	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/protocol/filter"
	"github.com/maxpert/marmot/protocol/mysqlcode"
	"github.com/rs/zerolog/log"
)

// ReplicationEngine encapsulates common replication logic for prepare/commit/abort phases.
// Extracted from LocalReplicator and ReplicationHandler to eliminate code duplication.
type ReplicationEngine struct {
	nodeID uint64
	dbMgr  DatabaseProvider
	clock  *hlc.Clock
}

// PrepareRequest contains parameters for the prepare phase
type PrepareRequest struct {
	TxnID      uint64
	NodeID     uint64
	StartTS    hlc.Timestamp
	Database   string
	Statements []protocol.Statement
}

// PrepareResult contains the result of the prepare phase
type PrepareResult struct {
	Success          bool
	Error            string
	ConflictDetected bool
	ConflictDetails  string
	// Rejected marks a deterministic refusal of the statement itself, such as DDL
	// SQLite cannot apply. Infrastructure failures (timeouts, storage errors) leave
	// it false so the coordinator keeps treating them as a missing ACK.
	Rejected bool
	// AutoIDStoredBase is this participant's own base (the largest of its claim
	// row's floors) for the table a rejected AUTO_INCREMENT range claim named,
	// so the claimant can retry above it rather than spin. Zero on every
	// response that is not such a rejection.
	AutoIDStoredBase uint64
	// ErrorCode is the MySQL server error code this rejection must reach the
	// client with, when the underlying error named one (a
	// transform.CodedError, for example the width-ceiling refusal's
	// ER_WARN_DATA_OUT_OF_RANGE). Zero means "not supplied": the coordinator
	// then falls back to classifying the message, as it did before. It exists
	// because Error is a string - the typed error cannot survive the hop to
	// the coordinator, local or remote, and without the code every
	// deterministic refusal reaches the client as 1105 HY000.
	ErrorCode uint16
}

// CommitRequest contains parameters for the commit phase
type CommitRequest struct {
	TxnID      uint64
	Database   string
	Statements []protocol.Statement // decision metadata; DML row images are durable from PREPARE
	// CommitTS is the commit timestamp the coordinator decided, identical on
	// every node; zero from a coordinator too old to send one.
	CommitTS hlc.Timestamp
}

// CommitResult contains the result of the commit phase
type CommitResult struct {
	Success bool
	Error   string
	// ClaimNotApplicable reports a COMMIT refused because this node could not
	// apply the AUTO_INCREMENT claim it carries (ErrAutoIncClaimNotApplicable).
	ClaimNotApplicable bool
}

// AbortRequest contains parameters for the abort phase
type AbortRequest struct {
	TxnID    uint64
	Database string
}

// AbortResult contains the result of the abort phase
type AbortResult struct {
	Success bool
	Error   string
}

// NewReplicationEngine creates a new replication engine
func NewReplicationEngine(nodeID uint64, dbMgr DatabaseProvider, clock *hlc.Clock) *ReplicationEngine {
	return &ReplicationEngine{
		nodeID: nodeID,
		dbMgr:  dbMgr,
		clock:  clock,
	}
}

// Prepare handles the prepare phase of 2PC replication
func (re *ReplicationEngine) Prepare(ctx context.Context, req *PrepareRequest) *PrepareResult {
	re.clock.Update(req.StartTS)

	if re.isDatabaseOperation(req.Statements) {
		return re.prepareDatabaseOperation(req)
	}

	return re.prepareRegularTransaction(ctx, req)
}

// ddlStatementsToValidate collects the DDL SQL that COMMIT will execute, in
// request order. Vector index control and LOAD DATA reuse the DDL intent type but
// are applied through their own paths, so they are excluded.
func ddlStatementsToValidate(statements []protocol.Statement) []string {
	var ddlSQL []string
	for _, stmt := range statements {
		if stmt.Type != protocol.StatementDDL || stmt.SQL == "" {
			continue
		}
		ddlSQL = append(ddlSQL, stmt.SQL)
	}
	return ddlSQL
}

// isDatabaseOperation checks if the request contains a CREATE/DROP DATABASE statement
func (re *ReplicationEngine) isDatabaseOperation(statements []protocol.Statement) bool {
	if len(statements) != 1 {
		return false
	}
	stmt := statements[0]
	return stmt.Type == protocol.StatementCreateDatabase || stmt.Type == protocol.StatementDropDatabase
}

// statementsCarryAutoIDClaim reports whether a transaction carries an
// AUTO_INCREMENT range claim. It reads the flag only, never the payload: the
// COMMIT message is decision metadata, and the claim's values are read from the
// intent this node wrote at PREPARE.
func statementsCarryAutoIDClaim(statements []protocol.Statement) bool {
	for _, stmt := range statements {
		if stmt.AutoIDClaim {
			return true
		}
	}
	return false
}

// prepareDatabaseOperation handles CREATE/DROP DATABASE operations using system database
func (re *ReplicationEngine) prepareDatabaseOperation(req *PrepareRequest) *PrepareResult {
	stmt := req.Statements[0]

	dbOp := DatabaseOpCreate
	if stmt.Type == protocol.StatementDropDatabase {
		dbOp = DatabaseOpDrop
	}

	// Fail fast: resolve and fence the op's registry key before touching the
	// system database's transaction manager, so a stale coordinator's PREPARE
	// is refused without leaving a transaction behind to abort.
	opKey, rejected := re.resolveDatabaseOpKey(stmt, dbOp)
	if rejected != nil {
		return rejected
	}

	systemDB, err := re.dbMgr.GetDatabase(SystemDatabaseName)
	if err != nil {
		return &PrepareResult{Success: false, Error: fmt.Sprintf("system database not found: %v", err)}
	}

	txnMgr := systemDB.GetTransactionManager()
	txn, err := txnMgr.BeginTransactionWithID(req.TxnID, req.NodeID, req.StartTS)
	if err != nil {
		return &PrepareResult{Success: false, Error: fmt.Sprintf("failed to begin transaction: %v", err)}
	}

	if err := txnMgr.AddStatement(txn, stmt); err != nil {
		_ = txnMgr.AbortTransaction(txn)
		return &PrepareResult{Success: false, Error: fmt.Sprintf("failed to add statement: %v", err)}
	}

	dbIntentKey := filter.EncodeDBOpIntentKey(stmt.Database)

	snapshotData := DatabaseOperationSnapshot{
		Type:         int(stmt.Type),
		Timestamp:    req.StartTS.WallTime,
		DatabaseName: stmt.Database,
		Operation:    dbOp,
		Generation:   opKey.Generation,
	}
	dataSnapshot, err := SerializeData(snapshotData)
	if err != nil {
		return &PrepareResult{Success: false, Error: fmt.Sprintf("failed to serialize data: %v", err)}
	}

	err = txnMgr.WriteIntent(txn, IntentTypeDatabaseOp, "", string(dbIntentKey), stmt, dataSnapshot)
	if err != nil {
		_ = txnMgr.AbortTransaction(txn)
		return &PrepareResult{Success: false, Error: fmt.Sprintf("write conflict: %v", err)}
	}

	log.Info().
		Str("database", stmt.Database).
		Str("operation", dbOp.String()).
		Uint64("generation", opKey.Generation).
		Uint64("node_id", re.nodeID).
		Uint64("txn_id", req.TxnID).
		Msg("Database operation prepared (intent created)")

	if err := systemDB.GetMetaStore().DurablyPrepareTransaction(req.TxnID); err != nil {
		_ = txnMgr.AbortTransaction(txn)
		return &PrepareResult{Success: false, Error: fmt.Sprintf("failed to durably prepare transaction: %v", err)}
	}

	return &PrepareResult{Success: true}
}

// resolveDatabaseOpKey computes the DatabaseRegistryKey a CREATE/DROP
// DATABASE statement's PREPARE resolves to for stmt.Database, and gates a
// coordinator's stamp against this participant's local key.
//
// stmt.DatabaseGeneration == 0 means "unstamped": a coordinator that predates
// Statement.DatabaseGeneration (rolling upgrade). The key is then computed
// locally, the same way a stamping coordinator would from its own registry
// (CREATE: the current key if live, else one generation above it; DROP: the
// current key), and the gate below is skipped - this participant simply
// trusts the legacy request, as it always has.
//
// Otherwise the stamped key is compared with the local key: below local means
// the coordinator's view of the database's history is stale, so the PREPARE
// is refused with ErrStaleDatabaseOpCoordinator (see
// DatabaseManager.ApplyDatabaseOp's proof for why this is required for
// convergence). Equal or above is accepted unchanged.
func (re *ReplicationEngine) resolveDatabaseOpKey(stmt protocol.Statement, dbOp DatabaseOpType) (DatabaseRegistryKey, *PrepareResult) {
	dbMgr, ok := re.dbMgr.(*DatabaseManager)
	if !ok {
		return DatabaseRegistryKey{}, &PrepareResult{Success: false, Error: "database manager does not support database operations"}
	}

	local, err := dbMgr.RegistryKey(stmt.Database)
	if err != nil {
		return DatabaseRegistryKey{}, &PrepareResult{Success: false, Error: fmt.Sprintf("failed to read database registry: %v", err)}
	}

	if stmt.DatabaseGeneration == 0 {
		if dbOp == DatabaseOpDrop {
			return local, nil
		}
		if !local.Dropped {
			return local, nil // already live: CREATE is a no-op at the current generation
		}
		return DatabaseRegistryKey{Generation: local.Generation + 1, Dropped: false}, nil
	}

	opKey := DatabaseRegistryKey{Generation: stmt.DatabaseGeneration, Dropped: dbOp == DatabaseOpDrop}
	if opKey.Compare(local) < 0 {
		return DatabaseRegistryKey{}, &PrepareResult{
			Success:  false,
			Rejected: true,
			Error: fmt.Sprintf("%v: database %s op key %+v is below local key %+v",
				ErrStaleDatabaseOpCoordinator, stmt.Database, opKey, local),
		}
	}
	return opKey, nil
}

// prepareRegularTransaction handles regular transaction preparation
func (re *ReplicationEngine) prepareRegularTransaction(ctx context.Context, req *PrepareRequest) *PrepareResult {
	replicatedDB, err := re.dbMgr.GetDatabase(req.Database)
	if err != nil {
		return &PrepareResult{Success: false, Error: fmt.Sprintf("database not found: %s", req.Database)}
	}

	txnMgr := replicatedDB.GetTransactionManager()
	metaStore := replicatedDB.GetMetaStore()

	// DDL is only executed during COMMIT, so validate it up front: a participant
	// that ACKs PREPARE must be able to commit. Without this, invalid DDL fails
	// after peers have committed and leaves the cluster inconsistent.
	//
	// Statements are validated together, in order, because an explicit client
	// transaction can carry several DDL statements where a later one depends on
	// the schema an earlier one creates.
	if ddlSQL := ddlStatementsToValidate(req.Statements); len(ddlSQL) > 0 {
		if err := txnMgr.ValidateDDL(ctx, ddlSQL); err != nil {
			// Only a verdict on the statement itself is a rejection. A cancelled or
			// timed-out validation, or a transient SQLite condition such as a
			// shared-cache lock race with hookDB (SQLITE_LOCKED, SQLITE_BUSY), says
			// nothing about the DDL, so it must stay a missing ACK the coordinator
			// can retry rather than a final answer.
			rejected := isDDLRejection(ctx, err)
			log.Debug().
				Err(err).
				Uint64("txn_id", req.TxnID).
				Uint64("node_id", re.nodeID).
				Bool("rejected", rejected).
				Msg("DDL validation failed during PREPARE")
			return &PrepareResult{Success: false, Error: err.Error(), Rejected: rejected,
				ErrorCode: mysqlCodeForError(err)}
		}
	}

	txn, err := txnMgr.BeginTransactionWithID(req.TxnID, req.NodeID, req.StartTS)
	if err != nil {
		return &PrepareResult{Success: false, Error: err.Error()}
	}

	adoptCapturedRows := metaStore.HasCapturedRows(req.TxnID)
	var stmtSeq uint64 = 0
	for _, stmt := range req.Statements {
		stmtSeq++

		// An AUTO_INCREMENT range claim is evaluated here, before
		// processStatement, because it needs replicatedDB - this node's own
		// SQLite - and because it must not reach either the empty-IntentKey
		// gate or createDMLIntent's row-image check, both of which are written
		// for ordinary DML.
		if stmt.AutoIDClaim {
			if result := re.prepareAutoIncClaim(replicatedDB, txn, txnMgr, metaStore, stmt, req); result != nil {
				_ = txnMgr.AbortTransaction(txn)
				return result
			}
			continue
		}

		if result := re.processStatement(txn, txnMgr, metaStore, stmt, req, stmtSeq, adoptCapturedRows); result != nil {
			return result
		}
	}

	if err := metaStore.DurablyPrepareTransaction(req.TxnID); err != nil {
		_ = txnMgr.AbortTransaction(txn)
		return &PrepareResult{Success: false, Error: fmt.Sprintf("failed to durably prepare transaction: %v", err)}
	}

	return &PrepareResult{Success: true}
}

// autoIncClaimStore resolves the AUTO_INCREMENT claim store, which is always
// backed by the system database rather than req.Database (db/autoinc_claim.go):
// a claim applied inside a pinned session on the user database would try to
// take that database's single SQLite writer a second time and block on its
// own BEGIN. Callers must reject the PREPARE / refuse the COMMIT on error
// rather than silently skip the claim.
func (re *ReplicationEngine) autoIncClaimStore() (*AutoIncClaimStore, error) {
	return autoIncClaimStoreOf(re.dbMgr)
}

// autoIncClaimStoreOf resolves dbMgr's AUTO_INCREMENT claim store (see
// ReplicationEngine.autoIncClaimStore).
func autoIncClaimStoreOf(dbMgr DatabaseProvider) (*AutoIncClaimStore, error) {
	systemDB, err := dbMgr.GetDatabase(SystemDatabaseName)
	if err != nil {
		return nil, fmt.Errorf("system database unavailable: %w", err)
	}
	return NewAutoIncClaimStore(systemDB), nil
}

// prepareAutoIncClaim evaluates an AUTO_INCREMENT range claim on this
// participant and, on acceptance, writes the claim intent whose DataSnapshot
// the COMMIT handler will apply. It returns nil to accept and a failed
// PrepareResult to reject.
//
// The condition, evaluated against this node's OWN committed state:
//
//	storedBase <= prevBase && newBase == prevBase && size >= 1 && newBase+size <= widthMax
//
// The second term is not "newBase > prevBase": a claim proposes its own view
// of the base as newBase, so that form could never hold and no claim would
// ever be accepted. The quantity that increases monotonically is the
// COMMITTED base, which COMMIT sets to newBase + size, and size >= 1.
//
// The term is "==" rather than ">=" on purpose: allocation is lowest-free, so a
// claimant never proposes above its own view of the base. A claimant that wants
// a higher base gets one only by retrying with the base a participant returned,
// which is what keeps a single-node cluster minting 1, 2, 3 with no jump at any
// range boundary.
//
// and an ABSENT stored base is never read as 0. That clause is the one an
// implementer will get wrong: the natural implementation is "absent means 0
// means yes", and a cache that defaults to 0 and accepts is precisely the
// thing that cannot cast the rejection that would repair it. An absent row is
// instead backfilled from the table itself (backfillAutoIncBase) on a node
// whose votes are not held, and refused otherwise.
//
// Two more gates come before the condition. A node whose votes are held
// (AutoIncHoldTable) declines without a verdict. A claim whose Membership is
// not this node's own count of the cluster is rejected: majorities of two
// different memberships need not intersect.
//
// widthMax is derived from THIS node's own sqlite_master, never taken from the
// claimant: the width marker rides in the replicated DDL text so every node
// derives it identically, and trusting the claimant would let a node with a
// stale schema be rubber-stamped.
//
// Soundness: any committed claim reached a majority, this claim needs a
// majority, and majorities intersect, so at least one participant holds a
// storedBase above prevBase and rejects. A rejection returns this node's own
// base so the claimant can retry above it rather than spin.
//
// That argument only holds if a participant's read of its own base and its
// vote on that read cannot straddle another claimant's commit. The intent
// this function writes is what makes them atomic: MetaStore.WriteIntent takes
// the per-(table, intentKey) row lock and holds it until the transaction
// commits or aborts (db/meta_store_pebble.go WriteIntent -> rowLocks.
// AcquireLock; released by DeleteIntentsByTxn / ReleaseByTxn on abort and by
// CommitTransaction on commit; a pending holder is never overwritten by
// another writer, however old its heartbeat - see resolveIntentConflictPebble),
// and every claim for one table shares one intent key
// (protocol.AutoIncClaimKey). The COMMIT handler writes the new base BEFORE
// it marks the transaction committed (Commit's ApplyClaims call runs ahead of
// txnMgr.CommitTransaction), so the base is durable while the lock is still
// held. ApplyClaims also refuses a COMMIT whose claim intent is gone or whose
// newBase this node's committed or seed floor has passed, so an ACK never
// depends on the lock alone (see ApplyClaims for the per-node disjointness
// argument). The base read below is the largest of the row's committed, seed
// and merged floors (AutoIncClaimStore.ReadBase).
//
// The intent is therefore written FIRST and the base read SECOND. Read first
// and the read is unlocked: two claimants can both read the pre-commit base,
// both find themselves in range, and both be granted the same ids - a
// check-then-act gap that -race cannot see, because each half is individually
// synchronised and only the pair is not. A claimant that loses the race for
// the lock gets a write-write conflict rather than a rejection, which the
// coordinator surfaces as the retryable 1205; a claimant that takes the lock
// after the winner committed reads the advanced base and rejects with it.
//
// Every rejection below returns a non-nil PrepareResult, and Prepare aborts
// the transaction on one (db/replication_engine.go, the AutoIDClaim branch of
// the statement loop), which releases the intent and its lock. No rejection
// path may return without going through that abort.
func (re *ReplicationEngine) prepareAutoIncClaim(
	replicatedDB *ReplicatedDatabase,
	txn *Transaction,
	txnMgr *TransactionManager,
	metaStore MetaStore,
	stmt protocol.Statement,
	req *PrepareRequest,
) *PrepareResult {
	claim, err := protocol.DecodeAutoIncClaim(stmt.AutoIDClaimPayload)
	if err != nil {
		return &PrepareResult{Success: false, Rejected: true, Error: err.Error()}
	}
	if len(stmt.IntentKey) == 0 {
		return &PrepareResult{Success: false, Rejected: true,
			Error: "auto-increment claim carries no intent key"}
	}

	// Take the claim's row lock before reading anything this node will vote
	// on - see the "Soundness" paragraph above. AddStatement precedes it for
	// the same reason processStatement orders the two that way: the statement
	// must be on the transaction before an intent refers to it.
	if err := txnMgr.AddStatement(txn, stmt); err != nil {
		return &PrepareResult{Success: false, Error: err.Error()}
	}
	if err := metaStore.WriteIntent(req.TxnID, IntentTypeAutoIDClaim, AutoIncClaimTable,
		string(stmt.IntentKey), OpTypeInsert, "", stmt.AutoIDClaimPayload, req.StartTS, req.NodeID); err != nil {
		// A lost race for the lock is not a verdict on the claim, so it is a
		// missing ACK rather than a rejection: the coordinator turns it into
		// the retryable 1205 and the claimant tries again.
		return &PrepareResult{Success: false, Error: err.Error()}
	}

	claimStore, err := re.autoIncClaimStore()
	if err != nil {
		// The claim store is backed by the system database (db/autoinc_claim.go);
		// if it cannot be resolved this node must refuse the claim rather than
		// silently skip the check.
		return &PrepareResult{Success: false, Rejected: true,
			Error: fmt.Sprintf("auto-increment claim for %s refused: %v", claim.Table, err)}
	}

	held, err := claimStore.VotesHeld()
	if err != nil {
		return &PrepareResult{Success: false, Rejected: true,
			Error: fmt.Sprintf("auto-increment claim for %s refused: %v", claim.Table, err)}
	}
	if held {
		// Not a verdict on the claim: this node's bases may be below a range
		// it or a majority granted, until it has merged bases from a
		// majority (AutoIncHoldTable). A missing ACK leaves the claim to the
		// rest of the cluster, and to this node once the merge completes.
		return &PrepareResult{Success: false,
			Error: fmt.Sprintf("auto-increment claim for %s declined: this node's votes are held until it merges claim bases from a majority", claim.Table)}
	}

	storedBase, err := claimStore.ReadBase(req.Database, claim.Table)
	if errors.Is(err, ErrAutoIncBaseAbsent) {
		storedBase, err = re.backfillAutoIncBase(replicatedDB, claimStore, req.Database, claim.Table)
	}
	if err != nil {
		// Absent with nothing to backfill from, or unreadable - either way
		// this node cannot vote yes.
		return &PrepareResult{Success: false, Rejected: true,
			Error: fmt.Sprintf("auto-increment claim for %s refused: %v", claim.Table, err)}
	}

	membership, err := re.dbMgr.ClusterMembership()
	if err != nil || claim.Membership == 0 || int(claim.Membership) != membership {
		// Majorities of two different memberships need not intersect, so a
		// claim is only voted on by a node that counts the cluster the way
		// the claimant did. A claimant that predates the field sends 0,
		// which no membership equals. The base rides along like any
		// rejection's; the claimant retries once gossip has converged.
		return &PrepareResult{Success: false, Rejected: true,
			Error: fmt.Sprintf("auto-increment claim for %s counted a membership of %d, this node counts %d (%v)",
				claim.Table, claim.Membership, membership, err),
			AutoIDStoredBase: storedBase}
	}

	widthMax, err := replicatedDB.AutoIncWidthMax(claim.Table)
	if err != nil {
		return &PrepareResult{Success: false, Rejected: true,
			Error:            fmt.Sprintf("auto-increment claim for %s refused: %v", claim.Table, err),
			AutoIDStoredBase: storedBase}
	}

	switch {
	case storedBase > claim.PrevBase:
		return &PrepareResult{Success: false, Rejected: true,
			Error:            fmt.Sprintf("auto-increment claim for %s is stale: this node holds base %d", claim.Table, storedBase),
			AutoIDStoredBase: storedBase}
	case claim.NewBase != claim.PrevBase:
		return &PrepareResult{Success: false, Rejected: true,
			Error:            fmt.Sprintf("auto-increment claim for %s must propose its own view of the base, not %d over %d", claim.Table, claim.NewBase, claim.PrevBase),
			AutoIDStoredBase: storedBase}
	case claim.Size == 0:
		// A zero-size claim would commit base = newBase + 0, leaving the base
		// where it was, so the same range could be handed out again.
		return &PrepareResult{Success: false, Rejected: true,
			Error:            fmt.Sprintf("auto-increment claim for %s does not advance the base", claim.Table),
			AutoIDStoredBase: storedBase}
	case claim.NewBase > widthMax || claim.Size > widthMax-claim.NewBase:
		return &PrepareResult{Success: false, Rejected: true,
			// Written as two subtractions rather than newBase+size > widthMax:
			// the sum is uint64 and a large size wraps, so the additive form
			// lets a claim past the ceiling read as if it were inside it.
			Error:            fmt.Sprintf("auto-increment claim for %s exhausts the column: %d+%d > %d", claim.Table, claim.NewBase, claim.Size, widthMax),
			AutoIDStoredBase: storedBase,
			// ER_DUP_ENTRY is what MySQL reports for a full AUTO_INCREMENT
			// column, and it is how the claimant tells this terminal verdict
			// apart from every retryable one (coordinator.ClaimRange).
			ErrorCode: mysqlcode.ErrCodeDupEntry}
	}

	return nil
}

// backfillAutoIncBase gives a table with no claim row the base the DDL-time
// seed would have written (autoIncSeedFloor: the declared floor, raised by
// the largest id the column's width can hold) and returns it. It runs under
// the claim key's lock, only on a node whose votes are not held.
//
// That is what makes it sound where "absent means 0" is not. On such a node
// an absent row means this node never ACKed a claim on the table: an ACK
// writes the row durably, no DDL removes a row, a restore never lowers or
// drops one, and a node that lost its rows is held (AutoIncHoldTable). So the node is exactly one that missed every claim on
// the table, whose low base majority intersection already tolerates. Rows
// go missing this way for a table marked before claim rows existed, and on a
// node whose user database was restored from a peer while it was down for
// the table's CREATE.
//
// A table with no explicitly declared AUTO_INCREMENT marker has nothing to
// backfill and
// stays refused.
func (re *ReplicationEngine) backfillAutoIncBase(replicatedDB *ReplicatedDatabase, claimStore *AutoIncClaimStore, database, table string) (uint64, error) {
	floor, ok, err := autoIncSeedFloor(replicatedDB.GetReadDB(), table)
	if err != nil {
		return 0, fmt.Errorf("backfill auto-increment base: %w", err)
	}
	if !ok {
		return 0, ErrAutoIncBaseAbsent
	}
	if err := claimStore.Seed(database, table, floor, re.nodeID); err != nil {
		return 0, fmt.Errorf("backfill auto-increment base: %w", err)
	}
	return claimStore.ReadBase(database, table)
}

// processStatement processes a single statement within a transaction
func (re *ReplicationEngine) processStatement(txn *Transaction, txnMgr *TransactionManager, metaStore MetaStore, stmt protocol.Statement, req *PrepareRequest, stmtSeq uint64, adoptCapturedRows bool) *PrepareResult {
	if err := txnMgr.AddStatement(txn, stmt); err != nil {
		_ = txnMgr.AbortTransaction(txn)
		return &PrepareResult{Success: false, Error: err.Error()}
	}

	if isVectorIndexControlStatement(stmt.Type) {
		return re.createVectorIndexIntent(txnMgr, txn, stmt, req)
	}

	intentKey := stmt.IntentKey
	if len(intentKey) == 0 {
		if stmt.Type == protocol.StatementLoadData {
			return re.createLoadDataIntent(txnMgr, txn, stmt, req.StartTS)
		}
		if stmt.Type == protocol.StatementInsert {
			log.Trace().
				Str("table", stmt.TableName).
				Msg("INSERT with auto-increment PK - skipping write intent")
			return nil
		}

		if stmt.Type == protocol.StatementUpdate || stmt.Type == protocol.StatementDelete {
			log.Debug().
				Str("table", stmt.TableName).
				Int("stmt_type", int(stmt.Type)).
				Msg("Empty IntentKey for UPDATE/DELETE - CDC will provide it during commit")
			return nil
		}

		if stmt.Type == protocol.StatementDDL {
			return re.createDDLIntent(txnMgr, txn, stmt, req.StartTS)
		}

		return nil
	}

	return re.createDMLIntent(txnMgr, metaStore, txn, stmt, string(intentKey), req, stmtSeq, adoptCapturedRows)
}

func isVectorIndexControlStatement(stmtType protocol.StatementCode) bool {
	switch stmtType {
	case protocol.StatementCreateVectorIndex, protocol.StatementDropVectorIndex,
		protocol.StatementReindexVectorIndex, protocol.StatementVectorIndexControl:
		return true
	default:
		return false
	}
}

// createLoadDataIntent creates a durable intent for LOAD DATA LOCAL INFILE statements.
func (re *ReplicationEngine) createLoadDataIntent(txnMgr *TransactionManager, txn *Transaction, stmt protocol.Statement, startTS hlc.Timestamp) *PrepareResult {
	loadIntentKey := filter.EncodeDDLIntentKey(stmt.TableName)

	snapshotData := LoadDataSnapshot{
		Type:      int(stmt.Type),
		Timestamp: startTS.WallTime,
		SQL:       stmt.SQL,
		TableName: stmt.TableName,
		Data:      stmt.LoadDataPayload,
	}
	dataSnapshot, serErr := SerializeData(snapshotData)
	if serErr != nil {
		_ = txnMgr.AbortTransaction(txn)
		return &PrepareResult{Success: false, Error: fmt.Sprintf("failed to serialize LOAD DATA payload: %v", serErr)}
	}

	if err := txnMgr.WriteIntent(txn, IntentTypeDDL, stmt.TableName, string(loadIntentKey), stmt, dataSnapshot); err != nil {
		_ = txnMgr.AbortTransaction(txn)
		return &PrepareResult{
			Success:          false,
			Error:            fmt.Sprintf("LOAD DATA conflict: %v", err),
			ConflictDetected: true,
			ConflictDetails:  err.Error(),
		}
	}

	return nil
}

// createDDLIntent creates a write intent for DDL statements
func (re *ReplicationEngine) createDDLIntent(txnMgr *TransactionManager, txn *Transaction, stmt protocol.Statement, startTS hlc.Timestamp) *PrepareResult {
	ddlIntentKey := filter.EncodeDDLIntentKey(stmt.TableName)

	snapshotData := DDLSnapshot{
		Type:      int(stmt.Type),
		Timestamp: startTS.WallTime,
		SQL:       stmt.SQL,
		TableName: stmt.TableName,
	}
	dataSnapshot, serErr := SerializeData(snapshotData)
	if serErr != nil {
		_ = txnMgr.AbortTransaction(txn)
		return &PrepareResult{Success: false, Error: fmt.Sprintf("failed to serialize DDL data: %v", serErr)}
	}

	if err := txnMgr.WriteIntent(txn, IntentTypeDDL, stmt.TableName, string(ddlIntentKey), stmt, dataSnapshot); err != nil {
		_ = txnMgr.AbortTransaction(txn)
		return &PrepareResult{
			Success:          false,
			Error:            fmt.Sprintf("DDL conflict: %v", err),
			ConflictDetected: true,
			ConflictDetails:  err.Error(),
		}
	}

	log.Debug().
		Str("table", stmt.TableName).
		Str("ddl_intent_key", string(ddlIntentKey)).
		Msg("Created write intent for DDL statement")

	return nil
}

func (re *ReplicationEngine) createVectorIndexIntent(txnMgr *TransactionManager, txn *Transaction, stmt protocol.Statement, req *PrepareRequest) *PrepareResult {
	dataSnapshot, change, err := vectorIndexChangeSnapshot(stmt, req.Database, req.StartTS)
	if err != nil {
		_ = txnMgr.AbortTransaction(txn)
		return &PrepareResult{Success: false, Error: fmt.Sprintf("failed to serialize vector index control: %v", err)}
	}
	intentKey := vectorControlIntentKey(change)
	stmt.VectorIndexChange = &change
	stmt.TableName = change.TableName
	stmt.Database = change.Database
	stmt.IntentKey = []byte(intentKey)
	if err := txnMgr.WriteIntent(txn, IntentTypeDDL, change.TableName, intentKey, stmt, dataSnapshot); err != nil {
		_ = txnMgr.AbortTransaction(txn)
		return &PrepareResult{
			Success:          false,
			Error:            fmt.Sprintf("vector index control conflict: %v", err),
			ConflictDetected: true,
			ConflictDetails:  err.Error(),
		}
	}
	return nil
}

// createDMLIntent creates the row lock and persists the DML redo image.
// PREPARE is the durability point for 2PC, so row images must be present
// before a participant can ACK prepare.
func (re *ReplicationEngine) createDMLIntent(txnMgr *TransactionManager, metaStore MetaStore, txn *Transaction, stmt protocol.Statement, intentKey string, req *PrepareRequest, stmtSeq uint64, adoptCapturedRows bool) *PrepareResult {
	if !adoptCapturedRows && (len(stmt.EncodedRow) == 0 || stmt.EncodedCodec != EncodedCapturedRowCodecMsgpack()) {
		_ = txnMgr.AbortTransaction(txn)
		return &PrepareResult{Success: false, Error: fmt.Sprintf("DML prepare missing encoded CDC row for %s", stmt.TableName)}
	}

	if err := txnMgr.WriteIntent(txn, IntentTypeDML, stmt.TableName, intentKey, stmt, nil); err != nil {
		_ = txnMgr.AbortTransaction(txn)
		return &PrepareResult{
			Success:          false,
			Error:            err.Error(),
			ConflictDetected: true,
			ConflictDetails:  err.Error(),
		}
	}

	if !adoptCapturedRows {
		if _, err := DecodeRow(stmt.EncodedRow); err != nil {
			_ = txnMgr.AbortTransaction(txn)
			return &PrepareResult{Success: false, Error: fmt.Sprintf("failed to decode prepared CDC row: %v", err)}
		}
		if err := metaStore.WriteCapturedRow(req.TxnID, stmtSeq, stmt.EncodedRow); err != nil {
			_ = txnMgr.AbortTransaction(txn)
			return &PrepareResult{Success: false, Error: fmt.Sprintf("failed to persist prepared CDC row: %v", err)}
		}
	}

	return nil
}

// Commit handles the commit phase of 2PC replication
func (re *ReplicationEngine) Commit(ctx context.Context, req *CommitRequest) *CommitResult {
	// Check if this is a database operation (CREATE/DROP DATABASE)
	// These are tracked in the system database
	systemDB, err := re.dbMgr.GetDatabase(SystemDatabaseName)
	if err == nil {
		// Try to find transaction in system database first
		systemTxnMgr := systemDB.GetTransactionManager()
		systemTxn := systemTxnMgr.GetTransaction(req.TxnID)

		if systemTxn != nil {
			// GetTransaction returns empty Statements, so we check intents instead
			// Intents are persisted during prepare phase and contain operation details
			metaStore := systemDB.GetMetaStore()
			intents, intentErr := metaStore.GetIntentsByTxn(req.TxnID)
			if intentErr == nil && len(intents) > 0 {
				// Check if any intent is for database operations
				for _, intent := range intents {
					if intent.IntentType == IntentTypeDatabaseOp {
						// Found a database operation - extract details from DataSnapshot
						var snapshotData DatabaseOperationSnapshot
						if err := DeserializeData(intent.DataSnapshot, &snapshotData); err != nil {
							log.Error().Err(err).Uint64("txn_id", req.TxnID).Msg("Failed to deserialize DB op snapshot")
							continue
						}

						dbOp := snapshotData.Operation
						dbName := snapshotData.DatabaseName
						if dbOp != DatabaseOpCreate && dbOp != DatabaseOpDrop {
							log.Error().Str("operation", dbOp.String()).Uint64("txn_id", req.TxnID).Msg("Unknown database operation")
							continue
						}

						// Execute the database operation BEFORE committing the transaction,
						// through the same key-fenced merge ApplyDatabaseOp gives
						// anti-entropy reconciliation.
						dbMgr, ok := re.dbMgr.(*DatabaseManager)
						if !ok {
							_ = systemTxnMgr.AbortTransaction(systemTxn)
							return &CommitResult{Success: false, Error: "database manager does not support database operations"}
						}

						opKey := DatabaseRegistryKey{Generation: snapshotData.Generation, Dropped: dbOp == DatabaseOpDrop}
						log.Info().
							Str("database", dbName).
							Str("operation", dbOp.String()).
							Uint64("generation", opKey.Generation).
							Uint64("node_id", re.nodeID).
							Msg("Applying database operation in commit phase")
						if _, dbOpErr := dbMgr.ApplyDatabaseOp(dbName, opKey); dbOpErr != nil {
							log.Error().Err(dbOpErr).Str("database", dbName).Str("operation", dbOp.String()).Msg("Database operation failed in commit phase")
							_ = systemTxnMgr.AbortTransaction(systemTxn)
							return &CommitResult{Success: false, Error: fmt.Sprintf("database operation failed: %v", dbOpErr)}
						}

						// Now commit the transaction to mark it as completed
						if err := systemTxnMgr.CommitTransaction(systemTxn); err != nil {
							// Database operation succeeded but transaction commit failed
							// This is not ideal but the operation is done
							log.Warn().Err(err).Str("database", dbName).Msg("Database operation succeeded but transaction commit failed")
						}

						log.Info().
							Str("database", dbName).
							Str("operation", dbOp.String()).
							Uint64("node_id", re.nodeID).
							Msg("Database operation committed successfully")

						return &CommitResult{Success: true}
					}
				}
			}
		}
	}

	// Regular operation - get user database
	replicatedDB, err := re.dbMgr.GetDatabase(req.Database)
	if err != nil {
		return &CommitResult{Success: false, Error: fmt.Sprintf("database not found: %s", req.Database)}
	}

	txn := replicatedDB.GetTransactionManager().GetTransaction(req.TxnID)
	if txn == nil {
		if committedLocally(replicatedDB, req.TxnID) {
			// The log puller already committed it here (CommitLocallyPrepared).
			return &CommitResult{Success: true}
		}
		log.Error().
			Uint64("txn_id", req.TxnID).
			Uint64("node_id", re.nodeID).
			Str("database", req.Database).
			Msg("COMMIT FAILED: Transaction not found - possibly GC'd or never prepared")
		return &CommitResult{Success: false, Error: "transaction not found"}
	}
	txn.Statements = req.Statements

	// The gate is the statement flag, not a store lookup: reading intents on
	// every commit would put a Pebble scan on the write path. The flag decides
	// only whether to look; every value written comes from the intent.
	result, _ := commitPreparedTxn(re.dbMgr, replicatedDB, txn, req.Database, req.CommitTS, statementsCarryAutoIDClaim(req.Statements))
	return result
}

// CommitLocallyPrepared commits database's durably prepared transaction
// txnID through the same local commit path a COMMIT RPC takes, for a
// transaction some peer's committed log proves was decided COMMITTED while
// this node never received (or failed to apply) its COMMIT, with the commit
// timestamp that log records for it (zero if the peer sent none). With no
// coordinator statements to read the claim flag from, whether the
// transaction carries an AUTO_INCREMENT claim is read from its intents.
func (dm *DatabaseManager) CommitLocallyPrepared(database string, txnID uint64, commitTS hlc.Timestamp) error {
	replicatedDB, err := dm.GetDatabase(database)
	if err != nil {
		return fmt.Errorf("database %s: %w", database, err)
	}
	txn := replicatedDB.GetTransactionManager().GetTransaction(txnID)
	if txn == nil {
		if committedLocally(replicatedDB, txnID) {
			return nil
		}
		return fmt.Errorf("transaction %d is not pending in %s", txnID, database)
	}
	intents, err := replicatedDB.GetMetaStore().GetIntentsByTxn(txnID)
	if err != nil {
		return fmt.Errorf("read intents for txn %d: %w", txnID, err)
	}
	carriesClaim := false
	for _, intent := range intents {
		if intent.IntentType == IntentTypeAutoIDClaim {
			carriesClaim = true
			break
		}
	}
	_, err = commitPreparedTxn(dm, replicatedDB, txn, database, commitTS, carriesClaim)
	return err
}

// committedLocally reports whether replicatedDB's local record for txnID is
// COMMITTED.
func committedLocally(replicatedDB *ReplicatedDatabase, txnID uint64) bool {
	rec, err := replicatedDB.GetMetaStore().GetTransaction(txnID)
	return err == nil && rec != nil && rec.Status == TxnStatusCommitted
}

// commitPreparedTxn is the user-database half of the COMMIT phase, shared by
// ReplicationEngine.Commit and DatabaseManager.CommitLocallyPrepared: apply
// txn's AUTO_INCREMENT claim first when applyClaims is set, then commit txn
// and refresh the read pool after DDL. It returns the result to answer a
// COMMIT with and the underlying error (nil when that result is a success,
// including for a transaction another caller already committed).
func commitPreparedTxn(dbMgr DatabaseProvider, replicatedDB *ReplicatedDatabase, txn *Transaction, database string, commitTS hlc.Timestamp, applyClaims bool) (*CommitResult, error) {
	// Apply any AUTO_INCREMENT range claim BEFORE committing, and refuse the
	// commit if it cannot be applied (the claim's first invariant: a
	// participant must never ACK COMMIT unless the claim row is durably in its
	// own SQLite). The order follows the CREATE DATABASE precedent above, which
	// executes its operation first and only then marks the transaction
	// committed; applying afterwards would leave the transaction committed
	// locally while this node correctly reports failure.
	//
	// Unlike that precedent this does NOT abort the transaction. A database
	// operation is alone in its transaction, so aborting discards only itself,
	// whereas a user-database transaction may carry client DML beside the
	// claim. Leaving it prepared is what 2PC expects of a participant that
	// cannot complete: it withholds the ACK and lets recovery resolve the
	// transaction, instead of unilaterally discarding writes the rest of the
	// cluster may be committing.
	//
	// The claim is applied under CommitTransactionAfter's concurrent-commit
	// guard: it is conditional on the claim row's floors, so applying it a
	// second time, for a transaction the log puller (or an earlier COMMIT)
	// already committed, would be refused as not applicable.
	var claimErr error
	applyClaim := func() error {
		if applyClaims {
			claimErr = applyCommitClaims(dbMgr, replicatedDB, txn.ID, database)
		}
		return claimErr
	}
	err := replicatedDB.GetTransactionManager().CommitTransactionAfter(txn, commitTS, applyClaim)
	switch {
	case claimErr != nil:
		return &CommitResult{Success: false, ClaimNotApplicable: errors.Is(claimErr, ErrAutoIncClaimNotApplicable),
			Error: fmt.Sprintf("auto-increment claim apply failed: %v", claimErr)}, claimErr
	case errors.Is(err, ErrTxnAlreadyCommitted):
		return &CommitResult{Success: true}, nil
	case err != nil:
		return &CommitResult{Success: false, Error: err.Error()}, err
	}

	// If DDL was committed, refresh read pool to pick up schema changes.
	// SQLite caches schema per-connection; read connections need refresh.
	for _, stmt := range txn.Statements {
		if stmt.Type == protocol.StatementDDL {
			replicatedDB.RefreshReadPool()
			break
		}
	}
	return &CommitResult{Success: true}, nil
}

// applyCommitClaims applies txnID's AUTO_INCREMENT claim intents. An error
// wrapping ErrAutoIncClaimNotApplicable is the protocol refusing an ACK this
// node must not give; any other is a failed write.
func applyCommitClaims(dbMgr DatabaseProvider, replicatedDB *ReplicatedDatabase, txnID uint64, database string) error {
	claimStore, err := autoIncClaimStoreOf(dbMgr)
	if err != nil {
		log.Error().Err(err).Uint64("txn_id", txnID).Str("database", database).
			Msg("COMMIT REFUSED: system database unavailable for auto-increment claim apply")
		return err
	}
	if err := claimStore.ApplyClaims(database, txnID, replicatedDB.GetMetaStore()); err != nil {
		ev := log.Error()
		if errors.Is(err, ErrAutoIncClaimNotApplicable) {
			ev = log.Warn()
		}
		ev.Err(err).Uint64("txn_id", txnID).Str("database", database).
			Msg("COMMIT REFUSED: auto-increment claim could not be applied")
		return err
	}
	return nil
}

// Abort handles the abort phase of 2PC replication
func (re *ReplicationEngine) Abort(ctx context.Context, req *AbortRequest) *AbortResult {
	// Check system database first for database operations
	systemDB, err := re.dbMgr.GetDatabase(SystemDatabaseName)
	if err == nil {
		systemTxnMgr := systemDB.GetTransactionManager()
		systemTxn := systemTxnMgr.GetTransaction(req.TxnID)
		if systemTxn != nil {
			if err := systemTxnMgr.AbortTransaction(systemTxn); err != nil {
				log.Warn().Err(err).Uint64("txn_id", req.TxnID).Msg("Failed to abort system database transaction")
			}
			return &AbortResult{Success: true}
		}
	}

	// Try user database
	replicatedDB, err := re.dbMgr.GetDatabase(req.Database)
	if err != nil {
		// If database doesn't exist, consider abort successful
		return &AbortResult{Success: true}
	}

	txnMgr := replicatedDB.GetTransactionManager()
	txn := txnMgr.GetTransaction(req.TxnID)
	if txn == nil {
		return &AbortResult{Success: true}
	}

	if err := txnMgr.AbortTransaction(txn); err != nil {
		return &AbortResult{Success: false, Error: err.Error()}
	}

	return &AbortResult{Success: true}
}

// ToCoordinatorResponse converts PrepareResult to coordinator.ReplicationResponse
func (pr *PrepareResult) ToCoordinatorResponse() *coordinator.ReplicationResponse {
	return &coordinator.ReplicationResponse{
		Success:          pr.Success,
		Error:            pr.Error,
		ConflictDetected: pr.ConflictDetected,
		ConflictDetails:  pr.ConflictDetails,
		Rejected:         pr.Rejected,
		ErrorCode:        pr.ErrorCode,
		AutoIDStoredBase: pr.AutoIDStoredBase,
	}
}

// ToCoordinatorResponse converts CommitResult to coordinator.ReplicationResponse
func (cr *CommitResult) ToCoordinatorResponse() *coordinator.ReplicationResponse {
	return &coordinator.ReplicationResponse{
		Success:            cr.Success,
		Error:              cr.Error,
		ClaimNotApplicable: cr.ClaimNotApplicable,
	}
}

// ToCoordinatorResponse converts AbortResult to coordinator.ReplicationResponse
func (ar *AbortResult) ToCoordinatorResponse() *coordinator.ReplicationResponse {
	return &coordinator.ReplicationResponse{
		Success: ar.Success,
		Error:   ar.Error,
	}
}
