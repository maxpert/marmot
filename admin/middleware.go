package admin

import (
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/maxpert/marmot/cfg"
)

// AuthMiddleware validates PSK authentication for admin endpoints
func AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// If cluster auth is not enabled, skip authentication
		if !cfg.IsClusterAuthEnabled() {
			next.ServeHTTP(w, r)
			return
		}

		secret := cfg.GetClusterSecret()

		// Check X-Marmot-Secret header
		providedSecret := r.Header.Get("X-Marmot-Secret")
		if providedSecret == "" {
			// Check Authorization: Bearer header
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				writeErrorResponse(w, http.StatusUnauthorized, "missing authentication header")
				return
			}
			// Parse "Bearer <token>"
			parts := strings.SplitN(authHeader, " ", 2)
			if len(parts) != 2 || parts[0] != "Bearer" {
				writeErrorResponse(w, http.StatusUnauthorized, "invalid authorization header format")
				return
			}
			providedSecret = parts[1]
		}

		if subtle.ConstantTimeCompare([]byte(providedSecret), []byte(secret)) != 1 {
			writeErrorResponse(w, http.StatusUnauthorized, "invalid secret")
			return
		}

		// Authenticated - proceed to next handler
		next.ServeHTTP(w, r)
	})
}

// RequireSecretMiddleware serves an endpoint only to a caller holding the
// cluster secret, and refuses it outright on a cluster with none. It guards
// the endpoints that change which AUTO_INCREMENT ids the cluster can issue,
// which AuthMiddleware alone would serve to anyone when no secret is set.
func RequireSecretMiddleware(next http.Handler) http.Handler {
	authenticated := AuthMiddleware(next)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !cfg.IsClusterAuthEnabled() {
			writeErrorResponse(w, http.StatusForbidden,
				"this endpoint is never served unauthenticated: set cluster.cluster_secret to use it")
			return
		}
		authenticated.ServeHTTP(w, r)
	})
}
