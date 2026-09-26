package admin

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/maxpert/marmot/cfg"
	marmotgrpc "github.com/maxpert/marmot/grpc"
)

// TestAutoIncReleaseVotesRequiresTheRiskToken pins that the forced release is
// never reached by accident: without accept_risk naming the risk, or with any
// other value, it is refused before anything is touched, and the refusal
// states the risk.
//
// Mutation: drop the accept_risk check. The handler reaches the nil server
// and panics instead of answering 400.
func TestAutoIncReleaseVotesRequiresTheRiskToken(t *testing.T) {
	h := &AdminHandlers{}
	for _, query := range []string{"", "?accept_risk=yes"} {
		rec := httptest.NewRecorder()
		h.handleAutoIncReleaseVotes(rec, httptest.NewRequest(http.MethodPost, "/cluster/autoinc/release-votes"+query, nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%q: status %d, want 400", query, rec.Code)
		}
		if body := rec.Body.String(); !strings.Contains(body, "same id twice") || !strings.Contains(body, marmotgrpc.ForceReleaseRiskToken) {
			t.Fatalf("%q: the refusal does not state the risk and the token: %s", query, body)
		}
	}
}

// TestAutoIncEndpointsRequireTheClusterSecret pins R3c-15: the release and
// sync endpoints are refused on a cluster with no secret, and served only
// with the right one on a cluster that has one - unlike the other admin
// endpoints, which AuthMiddleware serves to anyone when no secret is set.
//
// Mutation: route the autoinc endpoints through chiAuthMiddleware only. The
// no-secret request reaches the handler and "served without a cluster
// secret" fires.
func TestAutoIncEndpointsRequireTheClusterSecret(t *testing.T) {
	saved := cfg.Config.Cluster.ClusterSecret
	t.Cleanup(func() { cfg.Config.Cluster.ClusterSecret = saved })

	mux := http.NewServeMux()
	RegisterRoutes(mux, &AdminHandlers{})
	do := func(path, secret string) int {
		req := httptest.NewRequest(http.MethodPost, "/admin"+path, nil)
		if secret != "" {
			req.Header.Set("X-Marmot-Secret", secret)
		}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec.Code
	}

	for _, path := range []string{"/cluster/autoinc/sync", "/cluster/autoinc/release-votes?accept_risk=" + marmotgrpc.ForceReleaseRiskToken} {
		cfg.Config.Cluster.ClusterSecret = ""
		if code := do(path, ""); code != http.StatusForbidden {
			t.Fatalf("%s: status %d, want 403: served without a cluster secret", path, code)
		}
		cfg.Config.Cluster.ClusterSecret = "s3cret"
		if code := do(path, ""); code != http.StatusUnauthorized {
			t.Fatalf("%s: status %d without the secret, want 401", path, code)
		}
		if code := do(path, "wrong"); code != http.StatusUnauthorized {
			t.Fatalf("%s: status %d with a wrong secret, want 401", path, code)
		}
	}
}

// TestAutoIncSyncBodyIsCompleteOnlyWhenEveryMemberIs pins the sync command's
// verdict: complete only when every member answered, is unheld and reached
// every member.
func TestAutoIncSyncBodyIsCompleteOnlyWhenEveryMemberIs(t *testing.T) {
	ready := marmotgrpc.AutoIncSyncOutcome{NodeID: 1, Report: marmotgrpc.AutoIncSyncReport{
		NodeID: 1, Members: []uint64{1, 2}, Reached: []uint64{1, 2}}}
	if body := autoIncSyncBody([]marmotgrpc.AutoIncSyncOutcome{ready, ready}); body["complete"] != true {
		t.Fatalf("every member ready: %v", body)
	}
	down := marmotgrpc.AutoIncSyncOutcome{NodeID: 2, Err: errors.New("unreachable")}
	body := autoIncSyncBody([]marmotgrpc.AutoIncSyncOutcome{ready, down})
	if body["complete"] != false {
		t.Fatalf("an unreachable member reported complete: %v", body)
	}
	nodes := body["nodes"].([]map[string]interface{})
	if nodes[1]["answered"] != false || nodes[1]["error"] != "unreachable" {
		t.Fatalf("the unreachable member's entry: %v", nodes[1])
	}
	if autoIncSyncBody(nil)["complete"] != false {
		t.Fatal("no outcome at all reported complete")
	}
}
