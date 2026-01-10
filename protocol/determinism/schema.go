package determinism

import (
	"strings"

	"github.com/cespare/xxhash/v2"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/rqlite/sql"
)

// Cache size for parsed CREATE TABLE/TRIGGER statements
const schemaCacheSize = 1024

// tableCache caches parsed TableInfo keyed by XXH64 hash of CREATE TABLE SQL
var tableCache *lru.Cache[uint64, *TableInfo]

// triggerCache caches trigger parsing results keyed by XXH64 hash of CREATE TRIGGER SQL
type triggerCacheEntry struct {
	tableName       string
	isDeterministic bool
}

var triggerCache *lru.Cache[uint64, *triggerCacheEntry]

func init() {
	var err error
	tableCache, err = lru.New[uint64, *TableInfo](schemaCacheSize)
	if err != nil {
		panic("failed to create table cache: " + err.Error())
	}
	triggerCache, err = lru.New[uint64, *triggerCacheEntry](schemaCacheSize)
	if err != nil {
		panic("failed to create trigger cache: " + err.Error())
	}
}

// Schema holds table metadata for schema-aware determinism checking.
// Build this from CREATE TABLE and CREATE TRIGGER statements.
type Schema struct {
	Tables map[string]*TableInfo
}

// TableInfo holds metadata about a single table.
type TableInfo struct {
	Name       string
	Columns    map[string]*ColumnInfo
	HasTrigger bool // true if any trigger exists on this table
}

// ColumnInfo holds metadata about a single column.
type ColumnInfo struct {
	Name                   string
	HasDefault             bool
	DefaultIsDeterministic bool
}

// NewSchema creates an empty schema.
func NewSchema() *Schema {
	return &Schema{
		Tables: make(map[string]*TableInfo),
	}
}

// AddTable adds or updates table info from a CREATE TABLE statement.
// Uses XXH64-keyed LRU cache to avoid re-parsing identical SQL.
func (s *Schema) AddTable(createSQL string) error {
	hash := xxhash.Sum64String(createSQL)

	// Check cache first
	if cached, ok := tableCache.Get(hash); ok {
		// Clone - HasTrigger is per-schema state, not cached
		table := &TableInfo{
			Name:       cached.Name,
			Columns:    cached.Columns,
			HasTrigger: false, // Reset - this is set by AddTrigger, not cached
		}
		if existing, ok := s.Tables[cached.Name]; ok {
			table.HasTrigger = existing.HasTrigger
		}
		s.Tables[table.Name] = table
		return nil
	}

	// Parse and cache
	parser := sql.NewParser(strings.NewReader(createSQL))
	stmt, err := parser.ParseStatement()
	if err != nil {
		return err
	}

	create, ok := stmt.(*sql.CreateTableStatement)
	if !ok {
		return nil // not a CREATE TABLE, ignore
	}

	tableName := strings.ToLower(create.Name.Name)
	table := &TableInfo{
		Name:    tableName,
		Columns: make(map[string]*ColumnInfo),
	}

	for _, col := range create.Columns {
		colInfo := &ColumnInfo{
			Name:                   strings.ToLower(col.Name.Name),
			HasDefault:             false,
			DefaultIsDeterministic: true,
		}

		// Check for DEFAULT constraint
		for _, constraint := range col.Constraints {
			if defConstraint, ok := constraint.(*sql.DefaultConstraint); ok {
				colInfo.HasDefault = true
				colInfo.DefaultIsDeterministic = isExprDeterministic(defConstraint.Expr)
			}
		}

		table.Columns[colInfo.Name] = colInfo
	}

	// Cache the parsed result (without HasTrigger - that's per-schema state)
	tableCache.Add(hash, table)

	// Check if table already exists (might have trigger info)
	if existing, ok := s.Tables[tableName]; ok {
		table.HasTrigger = existing.HasTrigger
	}

	s.Tables[tableName] = table
	return nil
}

// AddTrigger marks a table as having a trigger.
// Uses XXH64-keyed LRU cache to avoid re-parsing identical SQL.
// For fail-safe, any table with non-deterministic triggers is marked.
func (s *Schema) AddTrigger(createSQL string) error {
	hash := xxhash.Sum64String(createSQL)

	// Check cache first
	if cached, ok := triggerCache.Get(hash); ok {
		if !cached.isDeterministic {
			// Get or create table info
			table, ok := s.Tables[cached.tableName]
			if !ok {
				table = &TableInfo{
					Name:    cached.tableName,
					Columns: make(map[string]*ColumnInfo),
				}
				s.Tables[cached.tableName] = table
			}
			table.HasTrigger = true
		}
		return nil
	}

	// Parse and cache
	parser := sql.NewParser(strings.NewReader(createSQL))
	stmt, err := parser.ParseStatement()
	if err != nil {
		return err
	}

	trigger, ok := stmt.(*sql.CreateTriggerStatement)
	if !ok {
		return nil // not a CREATE TRIGGER, ignore
	}

	tableName := strings.ToLower(trigger.Table.Name)

	// Check if trigger body contains non-deterministic expressions
	triggerIsDet := isTriggerBodyDeterministic(trigger)

	// Cache the result
	triggerCache.Add(hash, &triggerCacheEntry{
		tableName:       tableName,
		isDeterministic: triggerIsDet,
	})

	if !triggerIsDet {
		// Get or create table info
		table, ok := s.Tables[tableName]
		if !ok {
			table = &TableInfo{
				Name:    tableName,
				Columns: make(map[string]*ColumnInfo),
			}
			s.Tables[tableName] = table
		}
		table.HasTrigger = true
	}

	return nil
}

// DropTable removes a table from the schema.
func (s *Schema) DropTable(tableName string) {
	delete(s.Tables, strings.ToLower(tableName))
}

// DropTrigger would need trigger name tracking - for now, triggers stay marked.
// In practice, you'd rebuild schema periodically or track trigger names.

// isExprDeterministic checks if a DEFAULT expression is deterministic.
func isExprDeterministic(expr sql.Expr) bool {
	checker := &exprDeterminismChecker{}
	sql.Walk(checker, expr)
	return checker.isDeterministic
}

// exprDeterminismChecker walks an expression AST to check for non-deterministic elements.
type exprDeterminismChecker struct {
	isDeterministic bool
}

func (c *exprDeterminismChecker) Visit(node sql.Node) (sql.Visitor, sql.Node, error) {
	// Initialize as deterministic on first visit
	if !c.isDeterministic && node != nil {
		c.isDeterministic = true
	}

	switch n := node.(type) {
	case *sql.Call:
		name := strings.ToLower(n.Name.Name)
		if nonDeterministicFunctions[name] {
			c.isDeterministic = false
			return nil, node, nil
		}
		if timeFunctionsWithNow[name] && hasNowArg(n) {
			c.isDeterministic = false
			return nil, node, nil
		}
		if !deterministicFunctions[name] && !timeFunctionsWithNow[name] {
			// Unknown function - fail safe
			c.isDeterministic = false
			return nil, node, nil
		}
	case *sql.Ident:
		name := strings.ToLower(n.Name)
		if nonDeterministicIdents[name] {
			c.isDeterministic = false
			return nil, node, nil
		}
	case *sql.TimestampLit:
		// CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_TIME literals
		c.isDeterministic = false
		return nil, node, nil
	}
	return c, node, nil
}

func (c *exprDeterminismChecker) VisitEnd(node sql.Node) (sql.Node, error) {
	return node, nil
}

// hasNowArg checks if a time function call has 'now' as an argument.
func hasNowArg(call *sql.Call) bool {
	name := strings.ToLower(call.Name.Name)

	if name == "strftime" {
		if len(call.Args) <= 1 {
			return true
		}
		if len(call.Args) >= 2 {
			if lit, ok := call.Args[1].(*sql.StringLit); ok {
				if strings.ToLower(lit.Value) == "now" {
					return true
				}
			}
		}
		return false
	}

	if len(call.Args) == 0 {
		return true
	}

	if lit, ok := call.Args[0].(*sql.StringLit); ok {
		if strings.ToLower(lit.Value) == "now" {
			return true
		}
	}
	return false
}

// isTriggerBodyDeterministic checks if a trigger body is deterministic.
// Only DML statements (INSERT/UPDATE/DELETE) matter for replication.
func isTriggerBodyDeterministic(trigger *sql.CreateTriggerStatement) bool {
	for _, stmt := range trigger.Body {
		// Only check DML statements - SELECT doesn't modify data
		switch stmt.(type) {
		case *sql.InsertStatement, *sql.UpdateStatement, *sql.DeleteStatement:
			result := CheckStatement(stmt)
			if !result.IsDeterministic {
				return false
			}
		}
	}
	return true
}

// CheckSQLWithSchema checks a DML statement with schema context.
// This detects non-determinism from:
// - DEFAULT values with non-deterministic expressions (when column not specified in INSERT)
// - Tables with non-deterministic triggers
func CheckSQLWithSchema(sqlStr string, schema *Schema) Result {
	// First do the basic check
	result := CheckSQL(sqlStr)
	if !result.IsDeterministic {
		return result
	}

	if schema == nil {
		return result
	}

	// Parse to get statement details
	parser := sql.NewParser(strings.NewReader(sqlStr))
	stmt, err := parser.ParseStatement()
	if err != nil {
		return result // already checked above
	}

	return checkStatementWithSchema(stmt, schema)
}

// CheckStatementWithSchema checks a parsed statement with schema context.
func CheckStatementWithSchema(stmt sql.Statement, schema *Schema) Result {
	// First do the basic check
	result := CheckStatement(stmt)
	if !result.IsDeterministic {
		return result
	}

	if schema == nil {
		return result
	}

	return checkStatementWithSchema(stmt, schema)
}

func checkStatementWithSchema(stmt sql.Statement, schema *Schema) Result {
	switch s := stmt.(type) {
	case *sql.InsertStatement:
		return checkInsertWithSchema(s, schema)
	case *sql.UpdateStatement:
		return checkUpdateWithSchema(s, schema)
	case *sql.DeleteStatement:
		return checkDeleteWithSchema(s, schema)
	}
	return Result{IsDeterministic: true}
}

func checkInsertWithSchema(stmt *sql.InsertStatement, schema *Schema) Result {
	tableName := strings.ToLower(stmt.Table.Name)
	table, ok := schema.Tables[tableName]
	if !ok {
		// Unknown table - can't check schema, assume OK
		return Result{IsDeterministic: true}
	}

	// Check for triggers
	if table.HasTrigger {
		return Result{
			IsDeterministic: false,
			Reason:          "table has non-deterministic trigger: " + tableName,
		}
	}

	// Check for non-deterministic DEFAULTs on columns not in INSERT
	if len(stmt.Columns) > 0 {
		// Explicit column list - check which columns are missing
		specifiedCols := make(map[string]bool)
		for _, col := range stmt.Columns {
			specifiedCols[strings.ToLower(col.Name)] = true
		}

		for colName, colInfo := range table.Columns {
			if !specifiedCols[colName] && colInfo.HasDefault && !colInfo.DefaultIsDeterministic {
				return Result{
					IsDeterministic: false,
					Reason:          "column '" + colName + "' has non-deterministic DEFAULT",
				}
			}
		}
	}
	// If no column list, all columns must be provided or have deterministic defaults
	// This is harder to check without knowing the value count, so we're lenient here

	return Result{IsDeterministic: true}
}

func checkUpdateWithSchema(stmt *sql.UpdateStatement, schema *Schema) Result {
	tableName := strings.ToLower(stmt.Table.Name.Name)
	table, ok := schema.Tables[tableName]
	if !ok {
		return Result{IsDeterministic: true}
	}

	// Check for triggers
	if table.HasTrigger {
		return Result{
			IsDeterministic: false,
			Reason:          "table has non-deterministic trigger: " + tableName,
		}
	}

	return Result{IsDeterministic: true}
}

func checkDeleteWithSchema(stmt *sql.DeleteStatement, schema *Schema) Result {
	tableName := strings.ToLower(stmt.Table.Name.Name)
	table, ok := schema.Tables[tableName]
	if !ok {
		return Result{IsDeterministic: true}
	}

	// Check for triggers
	if table.HasTrigger {
		return Result{
			IsDeterministic: false,
			Reason:          "table has non-deterministic trigger: " + tableName,
		}
	}

	return Result{IsDeterministic: true}
}
