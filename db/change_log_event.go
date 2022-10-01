package db

import (
	"hash/crc64"
	"sort"
	"sync"

	"github.com/fxamacker/cbor/v2"
)

var tablePKColumnsCache = make(map[string][]string)
var tablePKColumnsLock = sync.RWMutex{}
var crc64Table = crc64.MakeTable(crc64.ECMA)

type ChangeLogEvent struct {
	Id        int64
	Type      string
	TableName string
	Row       map[string]any
	tableInfo []*ColumnInfo `cbor:"-"`
}

func (e *ChangeLogEvent) Marshal() ([]byte, error) {
	return cbor.Marshal(e)
}

func (e *ChangeLogEvent) Unmarshal(data []byte) error {
	return cbor.Unmarshal(data, e)
}

func (e *ChangeLogEvent) Hash() (uint64, error) {
	hasher := crc64.New(crc64Table)
	enc := cbor.NewEncoder(hasher)
	err := enc.StartIndefiniteArray()
	if err != nil {
		return 0, err
	}

	err = enc.Encode(e.TableName)
	if err != nil {
		return 0, err
	}

	pkColumns := e.getSortedPKColumns()
	for _, pk := range pkColumns {
		err = enc.Encode([]any{pk, e.Row[pk]})
		if err != nil {
			return 0, err
		}
	}

	err = enc.EndIndefinite()
	if err != nil {
		return 0, err
	}

	return hasher.Sum64(), nil
}

func (e *ChangeLogEvent) getSortedPKColumns() []string {
	tablePKColumnsLock.RLock()

	if values, found := tablePKColumnsCache[e.TableName]; found {
		tablePKColumnsLock.RUnlock()
		return values
	}
	tablePKColumnsLock.RUnlock()

	pkColumns := make([]string, 0, len(e.tableInfo))
	for _, itm := range e.tableInfo {
		if itm.IsPrimaryKey {
			pkColumns = append(pkColumns, itm.Name)
		}
	}
	sort.Strings(pkColumns)

	tablePKColumnsLock.Lock()
	defer tablePKColumnsLock.Unlock()

	tablePKColumnsCache[e.TableName] = pkColumns
	return pkColumns
}
