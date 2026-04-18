package coordinator

import (
	"errors"
	"fmt"
	"strings"

	"github.com/maxpert/marmot/common"
	"github.com/maxpert/marmot/modules/vecindex"
	"vitess.io/vitess/go/vt/sqlparser"
)

type goRankTemplateKey struct {
	SQL           string
	Database      string
	SessionNprobe int
	ForcePlan     string
	PrefilterCap  int64
}

type goRankRewriteTemplate struct {
	selectStmt    *sqlparser.Select
	userPred      sqlparser.Expr
	tableName     string
	tableAlias    string
	columnName    string
	queryArgIdx   int
	queryLiteral  []byte
	vecDistArgIdx int
	origArgCount  int
	k             int
}

func makeGoRankTemplateKey(sql, database string, session *connQuerySession) goRankTemplateKey {
	return goRankTemplateKey{
		SQL:           sql,
		Database:      database,
		SessionNprobe: session.vars.Nprobe,
		ForcePlan:     session.ForcePlan(),
		PrefilterCap:  session.PrefilterCap(),
	}
}

func (h *CoordinatorHandler) loadGoRankTemplate(key goRankTemplateKey) (*goRankRewriteTemplate, bool) {
	v, ok := h.vecGoRankTemplates.Load(key)
	if !ok {
		return nil, false
	}
	tpl, ok := v.(*goRankRewriteTemplate)
	return tpl, ok
}

func (h *CoordinatorHandler) storeGoRankTemplate(key goRankTemplateKey, tpl *goRankRewriteTemplate) {
	if tpl != nil {
		h.vecGoRankTemplates.Store(key, tpl)
	}
}

func buildGoRankRewriteTemplate(
	ast sqlparser.Statement,
	info *RewriteInfo,
	queryVec []byte,
	queryArgIdx int,
) (*goRankRewriteTemplate, error) {
	sel, ok := ast.(*sqlparser.Select)
	if !ok || info == nil || info.GoRank == nil {
		return nil, nil
	}
	matches, err := findVecMatches(sel)
	if err != nil || len(matches) != 1 {
		return nil, err
	}
	vm := matches[0]
	vd := findCachedVecDistance(sel, vm.colName, vm.tableRef)
	if vd == nil {
		return nil, err
	}
	vmIdx, vdIdx, totalArgs, err := findArgIndices(sel, vm.node, vd)
	if err != nil {
		return nil, err
	}
	if queryArgIdx >= 0 {
		vmIdx = queryArgIdx
	}
	literal := []byte(nil)
	if vmIdx < 0 && len(queryVec) > 0 {
		literal = append(literal, queryVec...)
	}
	return &goRankRewriteTemplate{
		selectStmt:    sel,
		userPred:      stripVecMatch(sel.Where),
		tableName:     info.TableName,
		tableAlias:    info.GoRank.BaseAlias,
		columnName:    info.ColumnName,
		queryArgIdx:   vmIdx,
		queryLiteral:  literal,
		vecDistArgIdx: vdIdx,
		origArgCount:  totalArgs,
		k:             info.K,
	}, nil
}

func findCachedVecDistance(sel *sqlparser.Select, colName, tableRef string) *sqlparser.FuncExpr {
	for _, order := range sel.OrderBy {
		fn, ok := order.Expr.(*sqlparser.FuncExpr)
		if !ok {
			continue
		}
		name := strings.ToLower(fn.Name.String())
		if !strings.HasPrefix(name, "vec_distance") || len(fn.Exprs) != 2 {
			continue
		}
		col, ok := fn.Exprs[0].(*sqlparser.ColName)
		if !ok || col.Name.String() != colName {
			continue
		}
		colTable := col.Qualifier.Name.String()
		if tableRef != "" && colTable != "" && tableRef != colTable {
			continue
		}
		return fn
	}
	return nil
}

func (tpl *goRankRewriteTemplate) buildInfo(
	queryVec []byte,
	meta *common.VectorIndexMeta,
	clusterIDs []int64,
	probeEpoch uint64,
	nprobe int,
) (*RewriteInfo, error) {
	goRank, err := BuildGoRankPlan(
		tpl.selectStmt,
		tpl.userPred,
		queryVec,
		tpl.queryArgIdx,
		tpl.vecDistArgIdx,
		tpl.origArgCount,
		meta,
		metricKindFromString(meta.Metric),
		clusterIDs,
		nprobe,
		tpl.tableAlias,
		tpl.k,
	)
	if err != nil {
		return nil, fmt.Errorf("build go-rank plan from cache: %w", err)
	}
	goRank.Database = meta.Database
	goRank.ProbeEpoch = probeEpoch
	return &RewriteInfo{
		Plan:       PlanPostFilter,
		IndexName:  meta.IndexName,
		Database:   meta.Database,
		TableName:  meta.TableName,
		ColumnName: meta.ColumnName,
		Metric:     meta.Metric,
		K:          tpl.k,
		ClusterIDs: append([]int64(nil), clusterIDs...),
		GoRank:     goRank,
	}, nil
}

func (tpl *goRankRewriteTemplate) resolveQueryVec(params []interface{}) ([]byte, error) {
	if tpl.queryArgIdx >= 0 {
		if tpl.queryArgIdx >= len(params) {
			return nil, fmt.Errorf("MARMOT-VEC-020: vec_match query argument index %d out of range (have %d params)", tpl.queryArgIdx, len(params))
		}
		return paramToBytes(params[tpl.queryArgIdx])
	}
	return append([]byte(nil), tpl.queryLiteral...), nil
}

func probeClustersWithEpoch(
	engine vecindex.VectorUDFProvider,
	indexName string,
	queryVec []byte,
	nprobe int,
) ([]int64, uint64, error) {
	type epochProvider interface {
		TopNprobeClustersWithEpoch(indexName string, vec []byte, n int) ([]int64, uint64, error)
	}
	if ep, ok := engine.(epochProvider); ok {
		clusterIDs, probeEpoch, err := ep.TopNprobeClustersWithEpoch(indexName, queryVec, nprobe)
		if err != nil {
			if errors.Is(err, vecindex.ErrNoCentroidsLoaded) {
				return nil, 0, nil
			}
			return nil, 0, fmt.Errorf("MARMOT-VEC-023: top-n probe clusters failed: %w", err)
		}
		return clusterIDs, probeEpoch, nil
	}
	clusterIDs, err := engine.TopNprobeClusters(indexName, queryVec, nprobe)
	if err != nil {
		if errors.Is(err, vecindex.ErrNoCentroidsLoaded) {
			return nil, 0, nil
		}
		return nil, 0, fmt.Errorf("MARMOT-VEC-023: top-n probe clusters failed: %w", err)
	}
	return clusterIDs, 0, nil
}
