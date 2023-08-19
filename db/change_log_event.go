package db

import (
	"hash/fnv"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/maxpert/marmot/core"
	"github.com/rs/zerolog/log"
)

var tablePKColumnsCache = make(map[string][]string)
var tablePKColumnsLock = sync.RWMutex{}

type sensitiveTypeWrapper struct {
	Time *time.Time `cbor:"1,keyasint,omitempty"`
}

type ChangeLogEvent struct {
	Id        int64
	Type      string
	TableName string
	Row       map[string]any
	tableInfo []*ColumnInfo `cbor:"-"`
}

func init() {
	err := core.CBORTags.Add(
		cbor.TagOptions{
			DecTag: cbor.DecTagRequired,
			EncTag: cbor.EncTagRequired,
		},
		reflect.TypeOf(sensitiveTypeWrapper{}),
		32,
	)

	log.Panic().Err(err)
}

func (s sensitiveTypeWrapper) GetValue() any {
	// Right now only sensitive value is Time
	return s.Time
}

func (e ChangeLogEvent) Wrap() (ChangeLogEvent, error) {
	return e.prepare(), nil
}

func (e ChangeLogEvent) Unwrap() (ChangeLogEvent, error) {
	ret := ChangeLogEvent{
		Id:        e.Id,
		TableName: e.TableName,
		Type:      e.Type,
		Row:       map[string]any{},
		tableInfo: e.tableInfo,
	}

	for k, v := range e.Row {
		if st, ok := v.(sensitiveTypeWrapper); ok {
			ret.Row[k] = st.GetValue()
			continue
		}

		ret.Row[k] = v
	}

	return ret, nil
}

func (e ChangeLogEvent) Hash() (uint64, error) {
	hasher := fnv.New64()
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

func (e ChangeLogEvent) getSortedPKColumns() []string {
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

func (e ChangeLogEvent) prepare() ChangeLogEvent {
	needsTransform := false
	preparedRow := map[string]any{}
	for k, v := range e.Row {
		if t, ok := v.(time.Time); ok {
			preparedRow[k] = sensitiveTypeWrapper{Time: &t}
			needsTransform = true
		} else {
			preparedRow[k] = v
		}
	}

	if !needsTransform {
		return e
	}

	return ChangeLogEvent{
		Id:        e.Id,
		Type:      e.Type,
		TableName: e.TableName,
		Row:       preparedRow,
		tableInfo: e.tableInfo,
	}
}
