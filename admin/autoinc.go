package admin

import (
	"net/http"

	"github.com/maxpert/marmot/db"
	marmotgrpc "github.com/maxpert/marmot/grpc"
)

// handleAutoIncVotes handles GET /admin/cluster/autoinc/votes: whether this
// node's AUTO_INCREMENT claim votes are held (db.AutoIncHoldTable). While
// they are, the node declines every claim at PREPARE, so narrow
// AUTO_INCREMENT inserts that need a new range can return 1205 until enough
// members answered its merge. It is the "unheld" check of the
// membership-change procedure (handleAutoIncSync), and a readiness signal
// after a node's first start.
func (h *AdminHandlers) handleAutoIncVotes(w http.ResponseWriter, r *http.Request) {
	held, err := db.NewAutoIncClaimStore(h.dbManager.GetSystemDatabase()).VotesHeld()
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSONResponse(w, map[string]interface{}{"votes_held": held}, false, "")
}

// handleAutoIncReleaseVotes handles POST
// /admin/cluster/autoinc/release-votes?accept_risk=duplicate-ids.
//
// It releases this node's held AUTO_INCREMENT claim votes after merging the
// bases of every peer that answers, without the safety condition the
// automatic merge waits for (grpc.ForceReleaseAutoIncVotes). It is for a
// cluster held beyond its failure model, and it can make the cluster issue
// the same id twice if a member holding a newer claim does not answer, so the
// caller must name that risk in accept_risk.
func (h *AdminHandlers) handleAutoIncReleaseVotes(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("accept_risk") != marmotgrpc.ForceReleaseRiskToken {
		writeErrorResponse(w, http.StatusBadRequest,
			"releasing held AUTO_INCREMENT claim votes without a safe merge can issue the same id twice "+
				"if a member holding a newer claim does not answer; pass accept_risk="+marmotgrpc.ForceReleaseRiskToken)
		return
	}
	registry := h.resolveRegistryOrError(w)
	if registry == nil {
		return
	}
	client := h.server.GetGossipProtocol().GetClient()
	if client == nil {
		writeErrorResponse(w, http.StatusServiceUnavailable, "cluster client not started")
		return
	}

	answered, wasHeld, err := marmotgrpc.ForceReleaseAutoIncVotes(r.Context(), registry.GetLocalNodeID(), h.dbManager, registry, client)
	if err != nil {
		writeErrorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSONResponse(w, map[string]interface{}{
		"was_held":       wasHeld,
		"released":       wasHeld,
		"peers_answered": answered,
	}, false, "")
}

// handleAutoIncSync handles POST /admin/cluster/autoinc/sync.
//
// It runs the AUTO_INCREMENT claim base sync now on this node and on every
// member of its membership (grpc.SyncAutoIncBasesEverywhere), and reports
// per node whether it answered, whether its votes are held and whether it
// reached every member. It is the confirmation step of the membership-change
// procedure: add or remove one node, wait until it is ALIVE and unheld, run
// this - re-run until the report is complete - and make the next change only
// when "complete" is true.
func (h *AdminHandlers) handleAutoIncSync(w http.ResponseWriter, r *http.Request) {
	registry := h.resolveRegistryOrError(w)
	if registry == nil {
		return
	}
	client := h.server.GetGossipProtocol().GetClient()
	if client == nil {
		writeErrorResponse(w, http.StatusServiceUnavailable, "cluster client not started")
		return
	}
	outcomes := marmotgrpc.SyncAutoIncBasesEverywhere(r.Context(), registry.GetLocalNodeID(), h.dbManager, registry, client)
	writeJSONResponse(w, autoIncSyncBody(outcomes), false, "")
}

// autoIncSyncBody is the sync command's answer: one entry per member, and
// complete when every member answered, is unheld and reached every member.
func autoIncSyncBody(outcomes []marmotgrpc.AutoIncSyncOutcome) map[string]interface{} {
	complete := len(outcomes) > 0
	nodes := make([]map[string]interface{}, 0, len(outcomes))
	for _, o := range outcomes {
		node := map[string]interface{}{
			"node_id":  o.NodeID,
			"answered": o.Err == nil,
			"complete": o.Complete(),
		}
		if o.Err != nil {
			node["error"] = o.Err.Error()
		} else {
			node["votes_held"] = o.Report.VotesHeld
			node["members"] = o.Report.Members
			node["reached"] = o.Report.Reached
			node["reached_every_member"] = o.Report.ReachedEveryMember()
		}
		complete = complete && o.Complete()
		nodes = append(nodes, node)
	}
	return map[string]interface{}{"complete": complete, "nodes": nodes}
}
