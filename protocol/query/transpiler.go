package query

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/maxpert/marmot/id"
	"github.com/maxpert/marmot/protocol/query/rules"
	"vitess.io/vitess/go/vt/sqlparser"
)

type CachedTranspilation struct {
	SQL             string
	Transformations []Transformation
}

type Transpiler struct {
	rules       RuleSet
	cache       *lru.Cache[string, CachedTranspilation]
	autoIncRule *rules.AutoIncrementIDRule
}

// NewTranspiler creates a new transpiler with the given cache size.
// idGen is the ID generator for auto-increment columns (nil to disable).
func NewTranspiler(cacheSize int, idGen id.Generator) (*Transpiler, error) {
	cache, err := lru.New[string, CachedTranspilation](cacheSize)
	if err != nil {
		return nil, err
	}

	// Cacheable rules - these produce deterministic output
	ruleSet := RuleSet{
		&rules.IntToBigintRule{}, // Priority 4: Transform INT AUTO_INCREMENT → BIGINT
		&rules.CreateTableRule{},
		&rules.TransactionSyntaxRule{},
		&rules.InsertIgnoreRule{},
		&rules.ReplaceIntoRule{},
		&rules.IndexHintsRule{},
		&rules.SelectModifiersRule{},
		&rules.LockingRule{},
		&rules.LimitRule{},
		&rules.EscapingRule{},
	}

	sort.Sort(ruleSet)

	// AutoIncrementIDRule is applied separately because it generates unique IDs
	var autoIncRule *rules.AutoIncrementIDRule
	if idGen != nil {
		autoIncRule = rules.NewAutoIncrementIDRule(idGen)
	}

	return &Transpiler{
		rules:       ruleSet,
		cache:       cache,
		autoIncRule: autoIncRule,
	}, nil
}

func (t *Transpiler) Transpile(ctx *QueryContext) error {
	if !ctx.NeedsTranspile {
		ctx.TranspiledSQL = ctx.OriginalSQL
		return nil
	}

	// Check if this statement needs ID injection
	// SchemaLookup is provided via context by the handler
	needsIDInjection := t.autoIncRule != nil && ctx.SchemaLookup != nil &&
		t.autoIncRule.NeedsIDInjection(ctx.AST, ctx.SchemaLookup)

	if !needsIDInjection {
		// Safe to use cache
		cacheKey := hashSQL(ctx.OriginalSQL)
		if cached, ok := t.cache.Get(cacheKey); ok {
			ctx.TranspiledSQL = cached.SQL
			ctx.Transformations = append([]Transformation{}, cached.Transformations...)
			ctx.WasCached = true
			return nil
		}
	}

	// Full transpilation (cache miss or ID injection needed)
	sql := ctx.OriginalSQL
	ast := ctx.AST
	transformations := []Transformation{}

	// Apply ID injection FIRST, before other rules
	// This ensures IDs are injected before pattern transformations
	if needsIDInjection {
		newAST, applied, err := t.autoIncRule.ApplyAST(ast, ctx.SchemaLookup)
		if err == nil && applied {
			ast = newAST
			sql = sqlparser.String(newAST)
			transformations = append(transformations, Transformation{
				Rule:   t.autoIncRule.Name(),
				Method: "AST",
			})
		}
	}

	// Apply other rules
	for _, rule := range t.rules {
		if ast != nil {
			newAST, applied, err := rule.ApplyAST(ast)
			if err == nil && applied {
				ast = newAST
				sql = sqlparser.String(newAST)
				transformations = append(transformations, Transformation{
					Rule:   rule.Name(),
					Method: "AST",
				})
				continue
			}
		}

		newSQL, applied, err := rule.ApplyPattern(sql)
		if err == nil && applied {
			transformations = append(transformations, Transformation{
				Rule:   rule.Name(),
				Method: "Pattern",
				Before: sql,
				After:  newSQL,
			})
			sql = newSQL
		}
	}

	ctx.TranspiledSQL = sql
	ctx.AST = ast
	ctx.Transformations = transformations

	// Only cache if no ID injection was needed
	// (since ID injection produces unique output each time)
	if !needsIDInjection {
		cacheKey := hashSQL(ctx.OriginalSQL)
		t.cache.Add(cacheKey, CachedTranspilation{
			SQL:             sql,
			Transformations: transformations,
		})
	}

	return nil
}

func hashSQL(sql string) string {
	h := sha256.New()
	h.Write([]byte(sql))
	return hex.EncodeToString(h.Sum(nil))
}
