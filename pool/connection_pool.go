package pool

import (
	"database/sql"
	"errors"
	"sync/atomic"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/mattn/go-sqlite3"
)

var ErrWrongPool = errors.New("returning object to wrong pool")

type ConnectionDisposer interface {
	Dispose(obj *SQLiteConnection) error
}

type SQLiteConnection struct {
	db       *sql.DB
	raw      *sqlite3.SQLiteConn
	gSQL     *goqu.Database
	disposer ConnectionDisposer
	state    int32
}

type SQLitePool struct {
	connections chan *SQLiteConnection
	dns         string
}

func (q *SQLiteConnection) SQL() *sql.DB {
	return q.db
}

func (q *SQLiteConnection) Raw() *sqlite3.SQLiteConn {
	return q.raw
}

func (q *SQLiteConnection) DB() *goqu.Database {
	return q.gSQL
}

func (q *SQLiteConnection) Return() error {
	return q.disposer.Dispose(q)
}

func (q *SQLiteConnection) init(dns string, disposer ConnectionDisposer) error {
	if !atomic.CompareAndSwapInt32(&q.state, 0, 1) {
		return nil
	}

	dbC, rawC, err := OpenRaw(dns)
	if err != nil {
		atomic.SwapInt32(&q.state, 0)
		return err
	}

	q.raw = rawC
	q.db = dbC
	q.gSQL = goqu.New("sqlite", dbC)
	q.disposer = disposer
	return nil
}

func (q *SQLiteConnection) reset() {
	if !atomic.CompareAndSwapInt32(&q.state, 1, 0) {
		return
	}

	q.db.Close()
	q.raw.Close()

	q.db = nil
	q.raw = nil
	q.gSQL = nil
	q.disposer = nil
}

func NewSQLitePool(dns string, poolSize int, lazy bool) (*SQLitePool, error) {
	ret := &SQLitePool{
		connections: make(chan *SQLiteConnection, poolSize),
		dns:         dns,
	}

	for i := 0; i < poolSize; i++ {
		con := &SQLiteConnection{}
		if !lazy {
			err := con.init(dns, ret)
			if err != nil {
				return nil, err
			}
		}
		ret.connections <- con
	}

	return ret, nil
}

func (q *SQLitePool) Borrow() (*SQLiteConnection, error) {
	c := <-q.connections
	err := c.init(q.dns, q)

	if err != nil {
		q.connections <- &SQLiteConnection{}
		return nil, err
	}

	return c, nil
}

func (q *SQLitePool) Dispose(obj *SQLiteConnection) error {
	if obj.disposer != q {
		return ErrWrongPool
	}

	q.connections <- obj
	return nil
}

func OpenRaw(dns string) (*sql.DB, *sqlite3.SQLiteConn, error) {
	var rawConn *sqlite3.SQLiteConn
	d := &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			rawConn = conn
			return conn.RegisterFunc("marmot_version", func() string {
				return "0.1"
			}, true)
		},
	}

	conn := sql.OpenDB(SqliteDriverConnector{driver: d, dns: dns})
	conn.SetConnMaxLifetime(0)
	conn.SetConnMaxIdleTime(10 * time.Second)

	err := conn.Ping()
	if err != nil {
		return nil, nil, err
	}

	return conn, rawConn, nil
}
