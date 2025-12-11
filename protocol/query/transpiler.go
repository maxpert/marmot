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
// It checks the cache first (if no ID injection is needed) and stores results for future use.
func (t *Transpiler) Transpile(ctx *QueryContext) error {
	if !ctx.NeedsTranspile {
		ctx.TranspiledSQL = ctx.OriginalSQL
		ctx.TranspiledStatements = []transform.TranspiledStatement{{SQL: ctx.OriginalSQL, Params: ctx.Parameters}}
		return nil
	}

	// Check if this statement needs ID injection
	needsIDInjection := t.autoIncRule != nil && ctx.SchemaLookup != nil &&
		t.autoIncRule.NeedsIDInjection(ctx.AST, ctx.SchemaLookup)

	if !needsIDInjection {
		// Safe to use cache
		cacheKey := hashSQL(ctx.OriginalSQL)
		if cached, ok := t.cache.Get(cacheKey); ok {
			if len(cached.Statements) > 0 {
				ctx.TranspiledSQL = cached.Statements[0].SQL
			}
			ctx.TranspiledStatements = append([]transform.TranspiledStatement{}, cached.Statements...)
			ctx.Transformations = append([]Transformation{}, cached.Transformations...)
			ctx.WasCached = true
			return nil
		}
	}

	// Full transpilation
	transformations := []Transformation{}
	var transpiledStatements []transform.TranspiledStatement

	// Reset serializer before use
	t.serializer.Reset()

	// Apply ID injection FIRST if needed
	ast := ctx.AST
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
		results, err := rule.Transform(ast, ctx.Parameters, ctx.SchemaProvider, ctx.Database, t.serializer)
		if errors.Is(err, transform.ErrRuleNotApplicable) {
			continue
		}
		if err != nil {
			return err
		}
		// Rule applied - collect results
		transpiledStatements = append(transpiledStatements, results...)
		transformations = append(transformations, Transformation{
			Rule:   rule.Name(),
			Method: "AST",
		})
		log.Debug().Str("rule", rule.Name()).Msg("Applied transform rule")
	}

	// If no rules applied, serialize original AST
	if len(transpiledStatements) == 0 {
		sql := t.serializer.Serialize(ast)
		transpiledStatements = []transform.TranspiledStatement{{SQL: sql, Params: ctx.Parameters}}

		// Collect any indexes extracted by serializer (for KEY definitions in CREATE TABLE)
		if indexes := t.serializer.ExtractedIndexes(); len(indexes) > 0 {
			for _, idx := range indexes {
				transpiledStatements = append(transpiledStatements, transform.TranspiledStatement{SQL: idx, Params: nil})
			}
		}
	}

	ctx.TranspiledSQL = transpiledStatements[0].SQL
	ctx.TranspiledStatements = transpiledStatements
	ctx.AST = ast
	ctx.Transformations = transformations

	// Only cache if no ID injection was needed
	if !needsIDInjection {
		cacheKey := hashSQL(ctx.OriginalSQL)
		t.cache.Add(cacheKey, CachedTranspilation{
			Statements:      transpiledStatements,
			Transformations: transformations,
		})
	}

	return nil
}

// hashSQL generates a SHA256 hash of the SQL string for cache key generation.
func hashSQL(sql string) string {
	h := sha256.New()
	h.Write([]byte(sql))
	return hex.EncodeToString(h.Sum(nil))
}
