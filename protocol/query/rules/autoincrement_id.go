package rules

import (
	"errors"
	"slices"
	"strconv"
	"strings"

	"github.com/maxpert/marmot/id"
	"github.com/maxpert/marmot/protocol/mysqlcode"
	"github.com/maxpert/marmot/protocol/query/transform"
	"github.com/maxpert/marmot/protocol/query/transform/intmarker"
	"vitess.io/vitess/go/vt/sqlparser"
)

// SchemaLookup returns the schema facts an INSERT needs for id injection, for
// the table in database - the statement's qualifier, or "" for the
// session's current database. It returns nil when the table is unknown.
type SchemaLookup func(database, table string) *transform.SchemaInfo

// NarrowAllocator mints ids for AUTO_INCREMENT columns declared narrower than
// BIGINT, from ranges claimed cluster-wide so every id fits the column. It is
// id.RangeAllocator.
type NarrowAllocator interface {
	// Allocate returns the first of n contiguous ids.
	Allocate(database, table string, widthMax uint64, n int) (uint64, error)
	// Admit decides whether the table may hold an id that did not come from
	// Allocate (see id.RangeAllocator.Admit).
	Admit(database, table string, widthMax, explicit uint64) error
}

// AutoIncrementIDRule injects distributed IDs for auto-increment columns.
// Detection is based on the SQLite schema: a single INTEGER/BIGINT PRIMARY KEY.
//
// The governing principle is that SQLite must never assign a primary key,
// because two nodes assigning rowids independently both mint 1, 2, 3 and CDC
// replays the collision as INSERT OR REPLACE. The rule therefore substitutes a
// generated id wherever the id's position in the statement is knowable, and
// refuses the statement only where safe substitution is structurally
// impossible.
//
// Which ids a table gets depends on how its key was declared. A column
// declared AUTO_INCREMENT narrower than BIGINT gets the narrow allocator's
// ids, which fit the declared width, and every id a client supplies for it is
// admitted (id.RangeAllocator.Admit) before any row is written. Every other
// key - BIGINT, a table created before width markers existed, and a narrow
// INTEGER PRIMARY KEY not declared AUTO_INCREMENT, whose ids MySQL never
// generates - keeps the wide generator's 53- or 64-bit ids for a row that
// leaves the key out, as always, and its client-supplied ids untouched.
type AutoIncrementIDRule struct {
	generator id.Generator
}

// NewAutoIncrementIDRule creates a new rule with the given wide ID generator.
func NewAutoIncrementIDRule(gen id.Generator) *AutoIncrementIDRule {
	return &AutoIncrementIDRule{
		generator: gen,
	}
}

func (r *AutoIncrementIDRule) Name() string  { return "AutoIncrementID" }
func (r *AutoIncrementIDRule) Priority() int { return 5 }

// IsNarrow reports whether a table's auto-increment column was declared
// AUTO_INCREMENT with a width marker, and so takes its ids from the narrow
// allocator.
func IsNarrow(info *transform.SchemaInfo) bool {
	return info.HasAutoIncrement() && info.AutoIncrementWidth != 0 && info.AutoIncrementExplicit
}

// insertPlan is what analyzeInsert decided ApplyAST must do to an INSERT.
// Exactly one of appendColumn and valueIdx is meaningful: appendColumn names a
// column to add to an explicit column list (with one generated value per row),
// while valueIdx is the position in each VALUES tuple whose placeholder value
// is replaced by a generated id.
type insertPlan struct {
	insert       *sqlparser.Insert
	rows         sqlparser.Values
	info         *transform.SchemaInfo
	table        string
	appendColumn string
	valueIdx     int
	// bound holds a prepared statement's bound values, and args the
	// position among them of each of the statement's placeholders.
	bound []interface{}
	args  map[*sqlparser.Argument]int
}

// needsID reports whether expr, in the auto-increment column's position,
// asks the server for an id. A narrow column treats a placeholder bound to
// NULL or 0 exactly as the literal: SQLite must never assign its rowid.
func (p *insertPlan) needsID(expr sqlparser.Expr) bool {
	if needsIDInjection(expr) {
		return true
	}
	_, ok := p.boundIDRequest(expr)
	return ok
}

// boundIDRequest returns the bound-value position of a placeholder bound to
// NULL or 0 in a narrow column.
func (p *insertPlan) boundIDRequest(expr sqlparser.Expr) (int, bool) {
	arg, ok := expr.(*sqlparser.Argument)
	if !ok || !IsNarrow(p.info) {
		return 0, false
	}
	i, ok := p.args[arg]
	if !ok || i >= len(p.bound) || !boundRequestsID(p.bound[i]) {
		return 0, false
	}
	return i, true
}

// boundRequestsID reports whether a bound value asks for a generated id, as
// NULL and 0 do in MySQL.
func boundRequestsID(v interface{}) bool {
	switch x := v.(type) {
	case nil:
		return true
	case int64:
		return x == 0
	case uint64:
		return x == 0
	case int:
		return x == 0
	case []byte:
		return string(x) == "0"
	case string:
		return x == "0"
	}
	return false
}

// analyzeInsert decides what the rule must do with a statement, without
// mutating it. It returns (nil, nil) when there is nothing to do, a plan when
// the statement can be rewritten, and an error when the statement must be
// refused. NeedsIDInjection and ApplyAST both go through it so the transpiler's
// gate and the rewrite can never disagree.
func analyzeInsert(stmt sqlparser.Statement, schemaLookup SchemaLookup, bound []interface{}) (*insertPlan, error) {
	insert, ok := stmt.(*sqlparser.Insert)
	if !ok {
		return nil, nil
	}

	database, tableName := extractInsertTableName(insert)
	if tableName == "" {
		return nil, nil
	}

	info := schemaLookup(database, tableName)
	if !info.HasAutoIncrement() {
		return nil, nil
	}
	autoIncCol := info.AutoIncrementColumn
	colIdx := findColumnIndex(insert.Columns, autoIncCol)

	rows, isValues := insert.Rows.(sqlparser.Values)
	if !isValues {
		// The rows come from a query, not from literals this rule can rewrite:
		// sqlparser.InsertRows is only Values, *Select, *Union or
		// *ValuesStatement, and the last three all supply their values at
		// execution time. When the column list names the auto-increment
		// column the query supplies its values and the statement is left
		// alone; otherwise SQLite would assign the rowid itself, so the
		// statement is refused.
		// A table value constructor (VALUES ROW(...)) is refused in BOTH forms.
		// Its literals live inside a statement node this rule cannot index
		// positionally, so the column-listed form would otherwise pass the gate
		// with a NULL id and reach SQLite, which cannot parse VALUES ROW at all.
		if _, isRowConstructor := insert.Rows.(*sqlparser.ValuesStatement); isRowConstructor {
			return nil, transform.NewCodedError(transform.ErrCodeNotSupportedYet,
				"This version of MySQL doesn't yet support 'INSERT ... VALUES ROW(...) into table `%s`, which has AUTO_INCREMENT column `%s`'",
				tableName, autoIncCol)
		}
		if colIdx >= 0 {
			return nil, nil
		}
		return nil, transform.NewCodedError(transform.ErrCodeNotSupportedYet,
			"This version of MySQL doesn't yet support 'INSERT ... SELECT into table `%s` without a projected value for AUTO_INCREMENT column `%s`'",
			tableName, autoIncCol)
	}

	plan := &insertPlan{insert: insert, rows: rows, info: info, table: tableName, valueIdx: -1, bound: bound}
	if len(bound) > 0 && IsNarrow(info) {
		plan.args = transform.ArgumentPositions(insert)
	}

	if colIdx < 0 && len(insert.Columns) > 0 {
		// Explicit column list that omits the auto-increment column: add it,
		// with one generated id per row.
		plan.appendColumn = autoIncCol
		return plan, nil
	}

	// A column list naming the auto-increment column puts its values at
	// colIdx. A column-less INSERT gives every column a value in the table's
	// own order, so the id sits at the auto-increment column's ordinal; a row
	// of the wrong length is SQLite's error to report, exactly as it is
	// today, so rows that cannot hold the ordinal are left untouched.
	idx := colIdx
	if idx < 0 {
		idx = info.AutoIncrementOrdinal
		if idx < 0 {
			return nil, nil
		}
	}
	plan.valueIdx = idx

	// A narrow column's explicit ids are admitted even when no row asks for
	// a generated one, so that no node issues them later.
	if plan.rowsNeedID() || (IsNarrow(info) && rowsHaveExplicitID(rows, idx)) {
		return plan, nil
	}
	return nil, nil
}

// rowsNeedID reports whether any row leaves the auto-increment position for
// the server to fill in.
func (p *insertPlan) rowsNeedID() bool {
	for _, row := range p.rows {
		if p.valueIdx < len(row) && p.needsID(row[p.valueIdx]) {
			return true
		}
	}
	return false
}

// rowsHaveExplicitID reports whether any row supplies a positive integer
// literal at position idx.
func rowsHaveExplicitID(rows sqlparser.Values, idx int) bool {
	for _, row := range rows {
		if idx < len(row) {
			if _, ok := explicitID(row[idx]); ok {
				return true
			}
		}
	}
	return false
}

// explicitID returns the id a client supplied as a positive integer literal.
// Anything else - a negative number, an expression, a bound parameter - is
// not an id the allocator could ever issue or cannot be read before
// execution, and is left to the database as it always was.
func explicitID(expr sqlparser.Expr) (uint64, bool) {
	lit, ok := expr.(*sqlparser.Literal)
	if !ok || lit.Type != sqlparser.IntVal {
		return 0, false
	}
	v, err := strconv.ParseUint(strings.TrimSpace(lit.Val), 10, 64)
	if err != nil || v == 0 {
		return 0, false
	}
	return v, true
}

// NeedsIDInjection reports whether the statement needs this rule to run. The
// transpiler uses it both to decide whether to call ApplyAST and to bypass its
// SQL cache, so a statement this rule would refuse must answer true as well.
// bound is a prepared statement's bound values, nil for a text query.
func (r *AutoIncrementIDRule) NeedsIDInjection(stmt sqlparser.Statement, schemaLookup SchemaLookup, bound []interface{}) bool {
	if r.generator == nil || schemaLookup == nil {
		return false
	}
	plan, err := analyzeInsert(stmt, schemaLookup, bound)
	return err != nil || plan != nil
}

// ApplyAST rewrites the statement in place, or returns an error when the
// statement must be refused. The error is returned to the caller verbatim; a
// *transform.CodedError carries the MySQL error code the client must see.
// narrow mints the ids of a column declared narrower than BIGINT; nil refuses
// such an INSERT. bound is a prepared statement's bound values, nil for a
// text query. A generated id for a placeholder bound to NULL or 0 is not
// written into the statement: it is returned in boundIDs, keyed by the
// placeholder's position among the bound values, to be bound in its place
// (protocol.Statement.MergeExecParams).
func (r *AutoIncrementIDRule) ApplyAST(stmt sqlparser.Statement, schemaLookup SchemaLookup, narrow NarrowAllocator, bound []interface{}) (out sqlparser.Statement, applied bool, boundIDs map[int]uint64, err error) {
	if r.generator == nil || schemaLookup == nil {
		return stmt, false, nil, nil
	}

	plan, err := analyzeInsert(stmt, schemaLookup, bound)
	if err != nil {
		return stmt, false, nil, err
	}
	if plan == nil {
		return stmt, false, nil, nil
	}

	var targets []int
	if plan.appendColumn == "" {
		for i, row := range plan.rows {
			if plan.valueIdx < len(row) && plan.needsID(row[plan.valueIdx]) {
				targets = append(targets, i)
			}
		}
	}
	count := len(targets)
	if plan.appendColumn != "" {
		count = len(plan.rows)
	}

	ids, err := r.mint(plan, count, narrow)
	if err != nil {
		return stmt, false, nil, err
	}

	if plan.appendColumn != "" {
		plan.insert.Columns = append(plan.insert.Columns, sqlparser.NewIdentifierCI(plan.appendColumn))
		for i := range plan.rows {
			plan.rows[i] = append(plan.rows[i], idLiteral(ids[i]))
		}
		return plan.insert, true, nil, nil
	}
	for i, row := range targets {
		if pos, ok := plan.boundIDRequest(plan.rows[row][plan.valueIdx]); ok {
			if boundIDs == nil {
				boundIDs = make(map[int]uint64)
			}
			boundIDs[pos] = ids[i]
			continue
		}
		plan.rows[row][plan.valueIdx] = idLiteral(ids[i])
	}
	return plan.insert, count > 0, boundIDs, nil
}

// mint returns count ids for the plan's rows, in row order. For a narrow
// column it first observes every id the client supplied, so the ids it then
// generates can never equal one of them.
func (r *AutoIncrementIDRule) mint(plan *insertPlan, count int, narrow NarrowAllocator) ([]uint64, error) {
	if !IsNarrow(plan.info) {
		ids := make([]uint64, count)
		if count > 0 {
			if err := r.generator.NextIDs(ids); err != nil {
				return nil, AllocationError(err, plan.table, WidthMax(plan.info))
			}
		}
		return ids, nil
	}

	if narrow == nil {
		return nil, transform.NewCodedError(mysqlcode.ErrCodeUnknown,
			"no allocator for AUTO_INCREMENT column `%s` of table `%s`", plan.info.AutoIncrementColumn, plan.table)
	}
	ceiling := WidthMax(plan.info)
	if err := admitExplicitIDs(plan, ceiling, narrow); err != nil {
		return nil, err
	}

	ids := make([]uint64, count)
	if count == 0 {
		return ids, nil
	}
	first, err := narrow.Allocate(plan.info.Database, plan.table, ceiling, count)
	if err != nil {
		return nil, AllocationError(err, plan.table, WidthMax(plan.info))
	}
	for i := range ids {
		ids[i] = first + uint64(i)
	}
	return ids, nil
}

// admitExplicitIDs admits every id the plan's rows supply themselves, in
// ascending order, before any row is written, so a refused id refuses the
// statement and a mixed statement's generated ids can never equal one of
// them. Ids known only once the statement has run are admitted from its CDC
// rows by the same allocator (coordinator.admitNarrowIDs).
func admitExplicitIDs(plan *insertPlan, ceiling uint64, narrow NarrowAllocator) error {
	if plan.valueIdx < 0 {
		return nil
	}
	var explicit []uint64
	for i, row := range plan.rows {
		if plan.valueIdx >= len(row) {
			continue
		}
		v, ok := explicitID(row[plan.valueIdx])
		if !ok {
			continue
		}
		if v > ceiling {
			return transform.NewCodedError(mysqlcode.ErrCodeDataOutOfRange,
				"Out of range value for column '%s' at row %d", plan.info.AutoIncrementColumn, i+1)
		}
		explicit = append(explicit, v)
	}
	slices.Sort(explicit)
	for _, v := range explicit {
		if err := narrow.Admit(plan.info.Database, plan.table, ceiling, v); err != nil {
			return AdmissionError(err, plan.table, plan.info.AutoIncrementColumn, ceiling)
		}
	}
	return nil
}

// AdmissionError turns a refused id for table's narrow column into the error
// a client sees: an id the column cannot hold is ER_WARN_DATA_OUT_OF_RANGE,
// as MySQL's strict mode reports it; an id at or below the cluster's
// allocation base that no range of this node holds is ER_NOT_SUPPORTED_YET,
// since it may lie in another node's unissued range; anything else is an
// allocation failure (AllocationError).
func AdmissionError(err error, table, column string, widthMax uint64) error {
	var below *id.BelowBaseError
	switch {
	case errors.Is(err, id.ErrIDOutOfRange):
		return transform.NewCodedError(mysqlcode.ErrCodeDataOutOfRange,
			"Out of range value for column '%s'", column)
	case errors.As(err, &below):
		return transform.NewCodedError(mysqlcode.ErrCodeNotSupportedYet,
			"This version of MySQL doesn't yet support 'an explicit id %d for AUTO_INCREMENT column `%s` of table `%s` at or below the cluster's allocation base %d'",
			below.ID, column, table, below.Base)
	default:
		return AllocationError(err, table, widthMax)
	}
}

// AllocationError turns a failure to mint or observe narrow ids for table,
// whose column holds at most widthMax, into the error a client sees: a full
// column is ER_DUP_ENTRY on the column's maximum, as MySQL reports it, and
// never retryable; anything else is the retryable lock-wait timeout.
func AllocationError(err error, table string, widthMax uint64) error {
	if errors.Is(err, id.ErrRangeExhausted) {
		return transform.NewCodedError(mysqlcode.ErrCodeDupEntry,
			"Duplicate entry '%d' for key '%s.PRIMARY'", widthMax, table)
	}
	return transform.NewCodedError(mysqlcode.ErrCodeLockTimeout,
		"Lock wait timeout exceeded; try restarting transaction (auto-increment ids for `%s`: %v)", table, err)
}

// WidthMax is the largest id a table's narrow auto-increment column can hold;
// zero for a column with no width marker.
func WidthMax(info *transform.SchemaInfo) uint64 {
	return intmarker.Attributes{Bits: info.AutoIncrementWidth, Unsigned: info.AutoIncrementUnsigned}.WidthMax()
}

// idLiteral renders an id as an integer literal.
func idLiteral(v uint64) *sqlparser.Literal {
	return sqlparser.NewIntLiteral(strconv.FormatUint(v, 10))
}

// extractInsertTableName returns the database qualifier ("" when absent) and
// the table name of an INSERT statement.
func extractInsertTableName(insert *sqlparser.Insert) (database, table string) {
	if insert.Table == nil {
		return "", ""
	}
	if tn, ok := insert.Table.Expr.(sqlparser.TableName); ok {
		return tn.Qualifier.String(), tn.Name.String()
	}
	return "", ""
}

// findColumnIndex returns the index of a column in the columns list, or -1 if not found.
func findColumnIndex(columns sqlparser.Columns, name string) int {
	nameLower := strings.ToLower(name)
	for i, col := range columns {
		if strings.ToLower(col.String()) == nameLower {
			return i
		}
	}
	return -1
}

// needsIDInjection reports whether an expression in the auto-increment
// column's position asks the server to supply the value. MySQL treats NULL,
// literal 0 and DEFAULT identically here.
func needsIDInjection(expr sqlparser.Expr) bool {
	switch v := expr.(type) {
	case *sqlparser.NullVal:
		return true
	case *sqlparser.Default:
		return true
	case *sqlparser.Literal:
		if v.Type == sqlparser.IntVal {
			return strings.TrimSpace(string(v.Val)) == "0"
		}
	}
	return false
}
