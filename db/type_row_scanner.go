package db

import (
    "database/sql"

    "github.com/mitchellh/mapstructure"
)

type TypedRowScanner[T any] struct {
    rows *enhancedRows
}

func NewTypedRowScanner[T any](rows *sql.Rows) (*TypedRowScanner[T], error) {
    return &TypedRowScanner[T]{
        rows: &enhancedRows{rows},
    }, nil
}

func (t *TypedRowScanner[T]) FetchAll() ([]*T, error) {
    result := make([]*T, 0)
    row, err := t.FetchNext()

    if row != nil {
        result = append(result, row)
    }

    for err == nil && row != nil {
        row, err = t.FetchNext()
        if row != nil {
            result = append(result, row)
        }
    }

    if err != nil {
        return nil, err
    }

    return result, nil
}

func (t *TypedRowScanner[T]) FetchNext() (*T, error) {
    var ret T
    next, err := t.Scan(&ret)
    if !next {
        return nil, nil
    }

    if err != nil {
        return nil, err
    }

    return &ret, err
}

func (t *TypedRowScanner[T]) Scan(ret *T) (bool, error) {
    next := t.rows.Next()
    if !next {
        return next, nil
    }

    if t.rows.Err() != nil {
        return next, t.rows.Err()
    }

    row, err := t.rows.fetchRow()
    if err != nil {
        return next, err
    }

    err = mapstructure.Decode(row, &ret)
    if err != nil {
        return next, err
    }

    return next, nil
}
