package common

// ResultColumn describes one column a query would return.
//
// It lives here because the database layer produces it and the coordinator
// consumes it, and db imports coordinator rather than the other way round.
type ResultColumn struct {
	// Name is the column label the database reports for the result.
	Name string
	// DeclType is the declared type of the underlying table column. It is
	// empty for expressions and computed columns, where only a value can
	// establish the type.
	DeclType string
}
