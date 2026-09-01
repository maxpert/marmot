package db

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/mattn/go-sqlite3"
	"github.com/maxpert/marmot/common"
)

// DescribeResultColumns reports the columns a query produces without running
// it. SQLite fills in column count, names, and declared types at prepare time,
// before the first step, so no rows are read and no side effects occur.
//
// The MySQL protocol needs this for COM_STMT_PREPARE: clients build their
// column-name index from the prepare response, so a server that reports no
// columns leaves them unable to address any column by name.
func (mdb *ReplicatedDatabase) DescribeResultColumns(ctx context.Context, query string) ([]common.ResultColumn, error) {
	conn, err := mdb.readDB.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()

	var columns []common.ResultColumn
	err = conn.Raw(func(driverConn interface{}) error {
		sqliteConn, ok := driverConn.(*sqlite3.SQLiteConn)
		if !ok {
			return fmt.Errorf("unexpected driver connection type: %T", driverConn)
		}
		columns, err = describeStatementColumns(sqliteConn, query)
		return err
	})
	if err != nil {
		return nil, err
	}
	return columns, nil
}

// describeStatementColumns prepares a statement and reads its result metadata.
// Placeholders are bound to NULL: values cannot affect the shape of the result,
// and the statement is never stepped, so the binding is inert.
func describeStatementColumns(conn *sqlite3.SQLiteConn, query string) ([]common.ResultColumn, error) {
	stmt, err := conn.Prepare(query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	sqliteStmt, ok := stmt.(*sqlite3.SQLiteStmt)
	if !ok {
		return nil, fmt.Errorf("unexpected statement type: %T", stmt)
	}

	args := make([]driver.Value, sqliteStmt.NumInput())
	rows, err := sqliteStmt.Query(args)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	names := rows.Columns()
	if len(names) == 0 {
		return nil, nil // not a result-producing statement
	}

	declTypes := make([]string, len(names))
	if sqliteRows, ok := rows.(*sqlite3.SQLiteRows); ok {
		if declared := sqliteRows.DeclTypes(); len(declared) == len(names) {
			copy(declTypes, declared)
		}
	}

	columns := make([]common.ResultColumn, len(names))
	for i, name := range names {
		columns[i] = common.ResultColumn{Name: name, DeclType: declTypes[i]}
	}
	return columns, nil
}
