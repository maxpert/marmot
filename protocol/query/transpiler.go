package query

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/maxpert/marmot/protocol/query/rules"
	"vitess.io/vitess/go/vt/sqlparser"
)

type CachedTranspilation struct {
	SQL             string
	Transformations []Transformation
}

type Transpiler struct {
	rules RuleSet
	cache *lru.Cache[string, CachedTranspilation]
}

func NewTranspiler(cacheSize int) (*Transpiler, error) {
	cache, err := lru.New[string, CachedTranspilation](cacheSize)
	if err != nil {
		return nil, err
	}

	ruleSet := RuleSet{
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

	return &Transpiler{
		rules: ruleSet,
		cache: cache,
	}, nil
}

func (t *Transpiler) Transpile(ctx *QueryContext) error {
	if !ctx.NeedsTranspile {
		ctx.TranspiledSQL = ctx.OriginalSQL
		return nil
	}

	cacheKey := hashSQL(ctx.OriginalSQL)
	if cached, ok := t.cache.Get(cacheKey); ok {
		ctx.TranspiledSQL = cached.SQL
		ctx.Transformations = cached.Transformations
		ctx.WasCached = true
		return nil
	}

	sql := ctx.OriginalSQL
	ast := ctx.AST
	transformations := []Transformation{}

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

	t.cache.Add(cacheKey, CachedTranspilation{
		SQL:             sql,
		Transformations: transformations,
	})

	return nil
}

func hashSQL(sql string) string {
	h := sha256.New()
	h.Write([]byte(sql))
	return hex.EncodeToString(h.Sum(nil))
}
