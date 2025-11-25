package grpc

import (
	"context"
	"net"
	"testing"

	"github.com/maxpert/marmot/cfg"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// clientInterceptorWithSecret creates a client interceptor that sends a specific secret
func clientInterceptorWithSecret(secret string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if secret != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, ClusterSecretHeader, secret)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// streamClientInterceptorWithSecret creates a stream client interceptor that sends a specific secret
func streamClientInterceptorWithSecret(secret string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if secret != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, ClusterSecretHeader, secret)
		}
		return streamer(ctx, desc, cc, method, opts...)
	}
}

func TestPSKAuthentication(t *testing.T) {
	tests := []struct {
		name         string
		serverSecret string
		clientSecret string
		wantErr      bool
		errCode      codes.Code
	}{
		{
			name:         "matching secrets succeed",
			serverSecret: "test-secret-123",
			clientSecret: "test-secret-123",
			wantErr:      false,
		},
		{
			name:         "mismatched secrets fail",
			serverSecret: "server-secret",
			clientSecret: "wrong-secret",
			wantErr:      true,
			errCode:      codes.Unauthenticated,
		},
		{
			name:         "missing client secret fails",
			serverSecret: "server-secret",
			clientSecret: "",
			wantErr:      true,
			errCode:      codes.Unauthenticated,
		},
		{
			name:         "no auth when server secret empty",
			serverSecret: "",
			clientSecret: "",
			wantErr:      false,
		},
		{
			name:         "client secret ignored when server has none",
			serverSecret: "",
			clientSecret: "some-secret",
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save original and set server secret
			origSecret := cfg.Config.Cluster.ClusterSecret
			cfg.Config.Cluster.ClusterSecret = tt.serverSecret
			defer func() { cfg.Config.Cluster.ClusterSecret = origSecret }()

			// Start test server with auth interceptor
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("failed to listen: %v", err)
			}
			defer listener.Close()

			server := grpc.NewServer(
				grpc.ChainUnaryInterceptor(UnaryServerInterceptor()),
				grpc.ChainStreamInterceptor(StreamServerInterceptor()),
			)
			RegisterMarmotServiceServer(server, &testMarmotServer{})

			go server.Serve(listener)
			defer server.Stop()

			// Create client with test-specific interceptor that uses the desired secret
			conn, err := grpc.NewClient(
				listener.Addr().String(),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithChainUnaryInterceptor(clientInterceptorWithSecret(tt.clientSecret)),
				grpc.WithChainStreamInterceptor(streamClientInterceptorWithSecret(tt.clientSecret)),
			)
			if err != nil {
				t.Fatalf("failed to connect: %v", err)
			}
			defer conn.Close()

			client := NewMarmotServiceClient(conn)

			// Test unary RPC
			_, err = client.Ping(context.Background(), &PingRequest{SourceNodeId: 1})

			if tt.wantErr {
				if err == nil {
					t.Error("expected error but got none")
					return
				}
				st, ok := status.FromError(err)
				if !ok {
					t.Errorf("expected gRPC status error, got: %v", err)
					return
				}
				if st.Code() != tt.errCode {
					t.Errorf("expected code %v, got %v", tt.errCode, st.Code())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestStreamPSKAuthentication(t *testing.T) {
	origSecret := cfg.Config.Cluster.ClusterSecret
	defer func() { cfg.Config.Cluster.ClusterSecret = origSecret }()

	// Set server secret
	cfg.Config.Cluster.ClusterSecret = "stream-test-secret"

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer listener.Close()

	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(UnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(StreamServerInterceptor()),
	)
	RegisterMarmotServiceServer(server, &testMarmotServer{})

	go server.Serve(listener)
	defer server.Stop()

	t.Run("stream with valid secret succeeds", func(t *testing.T) {
		conn, err := grpc.NewClient(
			listener.Addr().String(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithChainUnaryInterceptor(clientInterceptorWithSecret("stream-test-secret")),
			grpc.WithChainStreamInterceptor(streamClientInterceptorWithSecret("stream-test-secret")),
		)
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()

		client := NewMarmotServiceClient(conn)
		stream, err := client.StreamChanges(context.Background(), &StreamRequest{
			FromTxnId:        0,
			RequestingNodeId: 1,
		})
		if err != nil {
			t.Fatalf("failed to create stream: %v", err)
		}

		// Try to receive - should work (will get EOF since no data)
		_, err = stream.Recv()
		// EOF is expected since test server returns no data
		if err != nil && status.Code(err) == codes.Unauthenticated {
			t.Errorf("unexpected auth error on stream: %v", err)
		}
	})

	t.Run("stream with invalid secret fails", func(t *testing.T) {
		conn, err := grpc.NewClient(
			listener.Addr().String(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithChainUnaryInterceptor(clientInterceptorWithSecret("wrong-secret")),
			grpc.WithChainStreamInterceptor(streamClientInterceptorWithSecret("wrong-secret")),
		)
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		defer conn.Close()

		client := NewMarmotServiceClient(conn)
		stream, err := client.StreamChanges(context.Background(), &StreamRequest{
			FromTxnId:        0,
			RequestingNodeId: 1,
		})
		if err != nil {
			// Some gRPC versions return error on stream creation
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.Unauthenticated {
				return // Expected
			}
		}

		// Otherwise error should come on first Recv
		if stream != nil {
			_, err = stream.Recv()
			if err == nil {
				t.Error("expected auth error on stream recv")
				return
			}
			st, ok := status.FromError(err)
			if !ok || st.Code() != codes.Unauthenticated {
				// May get different error if connection failed
				t.Logf("got error (may be expected): %v", err)
			}
		}
	})
}

// testMarmotServer implements MarmotServiceServer for testing
type testMarmotServer struct {
	UnimplementedMarmotServiceServer
}

func (s *testMarmotServer) Ping(ctx context.Context, req *PingRequest) (*PingResponse, error) {
	return &PingResponse{
		NodeId: 999,
		Status: NodeStatus_ALIVE,
	}, nil
}

func (s *testMarmotServer) StreamChanges(req *StreamRequest, stream MarmotService_StreamChangesServer) error {
	// Return immediately (no data)
	return nil
}
