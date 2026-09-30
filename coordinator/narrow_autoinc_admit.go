package coordinator

import (
	"github.com/maxpert/marmot/encoding"
	"github.com/maxpert/marmot/protocol"
	"github.com/maxpert/marmot/protocol/mysqlcode"
	"github.com/maxpert/marmot/protocol/query/rules"
	"github.com/maxpert/marmot/protocol/query/transform"
)

// admitNarrowIDs admits every id a write transaction's rows put into a narrow
// AUTO_INCREMENT column, through the same allocator rule that admits literal
// ids at parse time (id.RangeAllocator.Admit), before the transaction is
// replicated. It is what covers the ids no parser can see: bound parameters,
// INSERT ... SELECT, REPLACE, LOAD DATA, an UPDATE that changes the column,
// and a key SQLite assigned itself.
//
// An id this node's allocator issued, or already admitted, passes. An id
// above the cluster's allocation base raises the base past it. An id at or
// below the base that no range of this node holds is refused: replicated
// inserts apply as INSERT OR REPLACE (db/cdc_applier.go), so once the node
// whose range holds it issued it, one of the two rows would silently replace
// the other everywhere.
//
// defaultDB names the database of a statement that does not carry one. The
// error is the coded one the client must see.
func (h *CoordinatorHandler) admitNarrowIDs(defaultDB string, stmts []protocol.Statement) error {
	if h.dbManager == nil {
		return nil
	}
	var lookups narrowTableLookups
	for i := range stmts {
		stmt := &stmts[i]
		if len(stmt.NewValues) == 0 {
			continue
		}
		database := stmt.Database
		if database == "" {
			database = defaultDB
		}
		info := lookups.get(h.dbManager, database, stmt.TableName)
		if info == nil {
			continue
		}
		newID, ok := cdcRowID(stmt.NewValues, info.AutoIncrementColumn)
		if !ok {
			continue
		}
		if oldID, had := cdcRowID(stmt.OldValues, info.AutoIncrementColumn); had && oldID == newID {
			// An UPDATE that left the column alone writes no new id.
			continue
		}
		if h.narrowIDs == nil {
			return transform.NewCodedError(mysqlcode.ErrCodeUnknown,
				"no allocator for AUTO_INCREMENT column `%s` of table `%s`", info.AutoIncrementColumn, stmt.TableName)
		}
		widthMax := rules.WidthMax(info)
		if err := h.narrowIDs.Admit(database, stmt.TableName, widthMax, newID); err != nil {
			return rules.AdmissionError(err, stmt.TableName, info.AutoIncrementColumn, widthMax)
		}
	}
	return nil
}

// narrowTableLookups memoises, for one transaction, which of its tables have
// a narrow auto-increment column. Transactions touch few tables, so a slice
// beats a map.
type narrowTableLookups []narrowTableLookup

type narrowTableLookup struct {
	database string
	table    string
	info     *transform.SchemaInfo // nil: no narrow auto-increment column
}

func (l *narrowTableLookups) get(dbManager DatabaseManager, database, table string) *transform.SchemaInfo {
	for _, known := range *l {
		if known.database == database && known.table == table {
			return known.info
		}
	}
	info, err := dbManager.GetTranspilerSchema(database, table)
	if err != nil || !rules.IsNarrow(info) {
		info = nil
	}
	*l = append(*l, narrowTableLookup{database: database, table: table, info: info})
	return info
}

// cdcRowID decodes column's value from a CDC row image as a positive id. A
// missing, non-integer or non-positive value is not an id the allocator could
// ever issue, so it can never meet one.
func cdcRowID(values map[string][]byte, column string) (uint64, bool) {
	raw, ok := values[column]
	if !ok {
		return 0, false
	}
	var id int64
	if err := encoding.Unmarshal(raw, &id); err != nil || id <= 0 {
		return 0, false
	}
	return uint64(id), true
}
