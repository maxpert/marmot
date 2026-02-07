package query

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/maxpert/marmot/id"
	"github.com/maxpert/marmot/protocol/query/rules"
	"github.com/maxpert/marmot/protocol/query/transform"
	"github.com/rs/zerolog/log"
	"vitess.io/vitess/go/vt/sqlparser"
)

// CachedTranspilation holds transpiled SQL statements and their transformations for caching.
type CachedTranspilation struct {
	Statements      []transform.TranspiledStatement
	Transformations []Transformation
}

// Transpiler converts MySQL SQL to SQLite SQL by applying transformation rules.
type Transpiler struct {
	transformRules []transform.Rule
	serializer     *transform.SQLiteSerializer
	cache          *lru.Cache[string, CachedTranspilation]
	autoIncRule    *rules.AutoIncrementIDRule
}

// NewTranspiler creates a new transpiler with the given cache size.
// idGen is the ID generator for auto-increment columns (nil to disable).
func NewTranspiler(cacheSize int, idGen id.Generator) (*Transpiler, error) {
	cache, err := lru.New[string, CachedTranspilation](cacheSize)
	if err != nil {
		return nil, err
	}

	// Transform rules modify AST for semantic changes
	ruleSet := []transform.Rule{
		&transform.DualTableRule{},            // Priority 1: Strip FROM dual
		&transform.LastInsertIDRule{},         // Priority 5: LAST_INSERT_ID() → last_insert_rowid()
		&transform.IntTypeRule{},              // Priority 5: Strip UNSIGNED, normalize int types
		&transform.CreateTableRule{},          // Priority 10: Extract KEY → CREATE INDEX
		&transform.InsertOnDuplicateKeyRule{}, // Priority 20: ON DUPLICATE KEY → ON CONFLICT
		&transform.DeleteJoinRule{},           // Priority 30: DELETE+JOIN → subquery
		&transform.UpdateJoinRule{},           // Priority 40: UPDATE+JOIN → subquery
	}
	sort.Slice(ruleSet, func(i, j int) bool {
		return ruleSet[i].Priority() < ruleSet[j].Priority()
	})

	// AutoIncrementIDRule is applied separately because it generates unique IDs
	var autoIncRule *rules.AutoIncrementIDRule
	if idGen != nil {
		autoIncRule = rules.NewAutoIncrementIDRule(idGen)
	}

	return &Transpiler{
		transformRules: ruleSet,
		serializer:     &transform.SQLiteSerializer{},
		cache:          cache,
		autoIncRule:    autoIncRule,
	}, nil
}

// Transpile converts a MySQL query to SQLite by applying transformation rules in priority order.
// It checks the cache first (if safe to cache) and stores results for future use.
// Queries that will have literal extraction are NOT cached because literal extraction modifies
// the AST after transpilation, and caching would return SQL with embedded literals instead of placeholders.
func (t *Transpiler) Transpile(ctx *QueryContext) error {
	// Check if this statement needs ID injection
	needsIDInjection := t.autoIncRule != nil && ctx.SchemaLookup != nil &&
		t.autoIncRule.NeedsIDInjection(ctx.MySQLState.AST, ctx.SchemaLookup)

	// Check if literal extraction will be applied
	needsLiteralExtraction := ctx.ExtractLiterals && len(ctx.Input.Parameters) == 0 && isDMLStatement(ctx.MySQLState.AST)

	// Only use cache when safe (no ID injection and no literal extraction needed)
	if !needsIDInjection && !needsLiteralExtraction {
		cacheKey := hashSQL(ctx.Input.SQL)
		if cached, ok := t.cache.Get(cacheKey); ok {
			ctx.Output.Statements = make([]TranspiledStatement, len(cached.Statements))
			for i, ts := range cached.Statements {
				ctx.Output.Statements[i] = TranspiledStatement{
					SQL:    ts.SQL,
					Params: ts.Params,
				}
			}
			ctx.MySQLState.Transformations = append([]Transformation{}, cached.Transformations...)
			ctx.MySQLState.WasCached = true
			return nil
		}
	}

	// Full transpilation
	transformations := []Transformation{}
	ast := ctx.MySQLState.AST
	var conflictColumns []string
	ruleApplied := false

	// Apply ID injection FIRST if needed
	if needsIDInjection {
		newAST, applied, err := t.autoIncRule.ApplyAST(ast, ctx.SchemaLookup)
		if err == nil && applied {
			ast = newAST
			transformations = append(transformations, Transformation{
				Rule:   t.autoIncRule.Name(),
				Method: "AST",
			})
			log.Debug().Str("rule", t.autoIncRule.Name()).Msg("Applied ID injection")
		}
	}

	// Apply transform rules in priority order
	for _, rule := range t.transformRules {
		results, err := rule.Transform(ast, ctx.Input.Parameters, ctx.SchemaProvider, ctx.Output.Database, t.serializer)
		if errors.Is(err, transform.ErrRuleNotApplicable) {
			continue
		}
		if err != nil {
			return err
		}

		// Extract conflict columns from metadata (InsertOnDuplicateKeyRule)
		if len(results) > 0 && results[0].Metadata != nil {
			if cols, ok := results[0].Metadata["conflictColumns"].([]string); ok {
				conflictColumns = cols
				ctx.MySQLState.ConflictColumns = cols
			}
		}

		// If rule returned non-empty SQL, use it; otherwise mark rule applied (will serialize later)
		if len(results) > 0 && results[0].SQL != "" {
			// Rule did its own serialization (like CreateTableRule with multiple statements)
			ctx.Output.Statements = make([]TranspiledStatement, len(results))
			for i, ts := range results {
				ctx.Output.Statements[i] = TranspiledStatement{
					SQL:    ts.SQL,
					Params: ts.Params,
				}
			}
			ctx.MySQLState.Transformations = append(transformations, Transformation{
				Rule:   rule.Name(),
				Method: "AST",
			})
			ctx.MySQLState.AST = ast
			// Don't cache rules that do their own serialization
			return nil
		}

		ruleApplied = true
		transformations = append(transformations, Transformation{
			Rule:   rule.Name(),
			Method: "AST",
		})
		log.Debug().Str("rule", rule.Name()).Msg("Applied transform rule")
	}

	// Strip database qualifiers (AST mutation only)
	if ctx.Output.Database != "" {
		stripDatabaseQualifiersAST(ast)
	}

	// Extract literals BEFORE serialization (AST mutation only)
	var extractedParams []interface{}
	if needsLiteralExtraction {
		extractedParams = transform.ExtractLiterals(ast)
	}

	// Single serialization with all state
	sql := t.serializer.SerializeWithOpts(ast, transform.SerializeOpts{
		ConflictColumns: conflictColumns,
	})

	// Determine params
	params := ctx.Input.Parameters
	if extractedParams != nil {
		params = extractedParams
	}

	ctx.Output.Statements = []TranspiledStatement{{SQL: sql, Params: params}}
	ctx.MySQLState.AST = ast
	ctx.MySQLState.Transformations = transformations

	// Cache if appropriate (and no literal extraction happened)
	if !needsIDInjection && !needsLiteralExtraction && !ruleApplied {
		cacheKey := hashSQL(ctx.Input.SQL)
		t.cache.Add(cacheKey, CachedTranspilation{
			Statements:      []transform.TranspiledStatement{{SQL: sql, Params: params}},
			Transformations: transformations,
		})
	}

	return nil
}

// isDMLStatement returns true if the AST represents a DML statement (INSERT, UPDATE, DELETE, SELECT).
// These statements are not cached because literal extraction modifies the AST after transpilation.
func isDMLStatement(stmt sqlparser.Statement) bool {
	switch stmt.(type) {
	case *sqlparser.Insert, *sqlparser.Update, *sqlparser.Delete, *sqlparser.Select:
		return true
	default:
		return false
	}
}

// hashSQL generates a SHA256 hash of the SQL string for cache key generation.
func hashSQL(sql string) string {
	h := sha256.New()
	h.Write([]byte(sql))
	return hex.EncodeToString(h.Sum(nil))
}

// stripDatabaseQualifiersAST removes database qualifiers from table references in-place.
func stripDatabaseQualifiersAST(stmt sqlparser.Statement) {
	sqlparser.Rewrite(stmt, func(cursor *sqlparser.Cursor) bool {
		if tn, ok := cursor.Node().(sqlparser.TableName); ok {
			cursor.Replace(sqlparser.TableName{
				Name:      tn.Name,
				Qualifier: sqlparser.NewIdentifierCS(""),
			})
		}
		return true
	}, nil)
}
