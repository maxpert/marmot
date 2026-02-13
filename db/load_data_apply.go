package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/maxpert/marmot/hlc"
	"github.com/maxpert/marmot/protocol"
)

// ApplyLoadData applies a LOAD DATA LOCAL INFILE statement payload to a local SQLite database.
// It reuses protocol.ExecuteLoadDataLocal so parsing/option handling stays centralized.
func ApplyLoadData(sqlDB *sql.DB, sqlStmt string, data []byte) (*protocol.ResultSet, error) {
	handler := &loadDataApplyHandler{db: sqlDB}
	session := &protocol.ConnectionSession{}
	return protocol.ExecuteLoadDataLocal(session, handler, sqlStmt, data)
}

// ApplyLoadDataInTx applies LOAD DATA payload inside an existing SQLite transaction.
func ApplyLoadDataInTx(tx *sql.Tx, sqlStmt string, data []byte) (*protocol.ResultSet, error) {
	handler := &loadDataApplyHandler{tx: tx}
	session := &protocol.ConnectionSession{}
	// Mark session as in-transaction so ExecuteLoadDataLocal does not emit BEGIN/COMMIT.
	session.BeginTransaction(1, hlc.Timestamp{}, "")
	defer session.EndTransaction()
	return protocol.ExecuteLoadDataLocal(session, handler, sqlStmt, data)
}

type loadDataApplyHandler struct {
	db *sql.DB
	tx *sql.Tx
}

func (h *loadDataApplyHandler) HandleQuery(_ *protocol.ConnectionSession, sqlStmt string, params []interface{}) (*protocol.ResultSet, error) {
	normalized := strings.ToUpper(strings.TrimSpace(sqlStmt))
	switch normalized {
	case "BEGIN":
		if h.tx != nil {
			return nil, fmt.Errorf("nested transaction is not supported")
		}
		tx, err := h.db.BeginTx(context.Background(), nil)
		if err != nil {
			return nil, err
		}
		h.tx = tx
		return nil, nil
	case "COMMIT":
		if h.tx == nil {
			return nil, nil
		}
		if err := h.tx.Commit(); err != nil {
			return nil, err
		}
		h.tx = nil
		return nil, nil
	case "ROLLBACK":
		if h.tx == nil {
			return nil, nil
		}
		err := h.tx.Rollback()
		h.tx = nil
		return nil, err
	}

	if h.tx == nil {
		if h.db == nil {
			return nil, fmt.Errorf("database handle is not initialized")
		}
		result, err := h.db.ExecContext(context.Background(), sqlStmt, params...)
		if err != nil {
			return nil, err
		}
		rows, _ := result.RowsAffected()
		lastID, _ := result.LastInsertId()
		return &protocol.ResultSet{RowsAffected: rows, LastInsertId: lastID}, nil
	}

	result, err := h.tx.ExecContext(context.Background(), sqlStmt, params...)
	if err != nil {
		return nil, err
	}
	rows, _ := result.RowsAffected()
	lastID, _ := result.LastInsertId()
	return &protocol.ResultSet{RowsAffected: rows, LastInsertId: lastID}, nil
}
