package grpc

import (
	"context"

	"github.com/maxpert/marmot/cfg"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// ClusterSecretHeader is the metadata key for the cluster secret
	ClusterSecretHeader = "x-marmot-cluster-secret"
)

// UnaryServerInterceptor returns a server interceptor that validates the cluster secret
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if err := validateClusterSecret(ctx); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamServerInterceptor returns a server interceptor for streaming RPCs
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := validateClusterSecret(ss.Context()); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

// validateClusterSecret checks if the request contains a valid cluster secret
func validateClusterSecret(ctx context.Context) error {
	if !cfg.IsClusterAuthEnabled() {
		return nil
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}

	secrets := md.Get(ClusterSecretHeader)
	if len(secrets) == 0 {
		return status.Error(codes.Unauthenticated, "missing cluster secret")
	}

	if secrets[0] != cfg.GetClusterSecret() {
		return status.Error(codes.Unauthenticated, "invalid cluster secret")
	}

	return nil
}

// UnaryClientInterceptor returns a client interceptor that adds the cluster secret
func UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = appendClusterSecret(ctx)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// StreamClientInterceptor returns a client interceptor for streaming RPCs
func StreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = appendClusterSecret(ctx)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

// appendClusterSecret adds the cluster secret to outgoing context
func appendClusterSecret(ctx context.Context) context.Context {
	if !cfg.IsClusterAuthEnabled() {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, ClusterSecretHeader, cfg.GetClusterSecret())
}

// UnaryClientInterceptorWithSecret returns a client interceptor that uses a specific secret
func UnaryClientInterceptorWithSecret(secret string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if secret != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, ClusterSecretHeader, secret)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// StreamClientInterceptorWithSecret returns a stream client interceptor that uses a specific secret
func StreamClientInterceptorWithSecret(secret string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if secret != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, ClusterSecretHeader, secret)
		}
		return streamer(ctx, desc, cc, method, opts...)
	}
}
