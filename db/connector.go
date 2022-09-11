package db

import (
	"context"
	"database/sql/driver"
)

type SqliteDriverConnector struct {
	driver driver.Driver
	dns    string
}

func (t SqliteDriverConnector) Connect(_ context.Context) (driver.Conn, error) {
	return t.driver.Open(t.dns)
}

func (t SqliteDriverConnector) Driver() driver.Driver {
	return t.driver
}
