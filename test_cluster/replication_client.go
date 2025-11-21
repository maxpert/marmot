//go:build ignore
// +build ignore

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/maxpert/marmot/grpc"
)

var (
	nodeAddr = flag.String("node", "localhost:8081", "Node address (host:port)")
)

func main() {
	flag.Parse()

	fmt.Printf("Testing replication handler on %s\n", *nodeAddr)

	// Connect to node
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, *nodeAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewMarmotServiceClient(conn)

	// Test 1: Ping to verify connectivity
	fmt.Println("\n[Test 1] Pinging node...")
	pingResp, err := client.Ping(context.Background(), &pb.PingRequest{})
	if err != nil {
		log.Fatalf("Ping failed: %v", err)
	}
	fmt.Printf("✓ Ping successful - Node ID: %d, Status: %s\n", pingResp.NodeId, pingResp.Status)

	// Test 2: Send a test transaction (PREPARE phase)
	fmt.Println("\n[Test 2] Testing PREPARE transaction...")
	txReq := &pb.TransactionRequest{
		TxnId:        12345,
		SourceNodeId: 9999, // Test coordinator
		Phase:        pb.TransactionPhase_PREPARE,
		Timestamp: &pb.HLC{
			WallTime: time.Now().UnixNano(),
			Logical:  0,
			NodeId:   9999,
		},
		Statements: []*pb.Statement{
			{
				Sql:       "INSERT INTO test_table (id, value) VALUES (1, 'test_value_1')",
				Type:      pb.StatementType_INSERT,
				TableName: "test_table",
			},
		},
		Consistency: pb.ConsistencyLevel_CONSISTENCY_QUORUM,
	}

	txResp, err := client.ReplicateTransaction(context.Background(), txReq)
	if err != nil {
		log.Fatalf("PREPARE failed: %v", err)
	}

	if txResp.Success {
		fmt.Printf("✓ PREPARE successful - Transaction ID: %d\n", txReq.TxnId)
	} else {
		fmt.Printf("✗ PREPARE failed - Error: %s\n", txResp.ErrorMessage)
	}

	// Test 3: Send COMMIT phase
	fmt.Println("\n[Test 3] Testing COMMIT transaction...")
	txReq.Phase = pb.TransactionPhase_COMMIT

	txResp, err = client.ReplicateTransaction(context.Background(), txReq)
	if err != nil {
		log.Fatalf("COMMIT failed: %v", err)
	}

	if txResp.Success {
		fmt.Printf("✓ COMMIT successful - Transaction ID: %d\n", txReq.TxnId)
	} else {
		fmt.Printf("✗ COMMIT failed - Error: %s\n", txResp.ErrorMessage)
	}

	// Test 4: Send another transaction (PREPARE -> ABORT)
	fmt.Println("\n[Test 4] Testing ABORT transaction...")
	txReq2 := &pb.TransactionRequest{
		TxnId:        12346,
		SourceNodeId: 9999,
		Phase:        pb.TransactionPhase_PREPARE,
		Timestamp: &pb.HLC{
			WallTime: time.Now().UnixNano(),
			Logical:  0,
			NodeId:   9999,
		},
		Statements: []*pb.Statement{
			{
				Sql:       "UPDATE test_table SET value = 'updated' WHERE id = 2",
				Type:      pb.StatementType_UPDATE,
				TableName: "test_table",
			},
		},
		Consistency: pb.ConsistencyLevel_CONSISTENCY_QUORUM,
	}

	txResp, err = client.ReplicateTransaction(context.Background(), txReq2)
	if err != nil {
		log.Fatalf("PREPARE (abort test) failed: %v", err)
	}

	if txResp.Success {
		fmt.Printf("✓ PREPARE successful - Transaction ID: %d\n", txReq2.TxnId)
	} else {
		fmt.Printf("✗ PREPARE failed - Error: %s\n", txResp.ErrorMessage)
	}

	// Now abort it
	txReq2.Phase = pb.TransactionPhase_ABORT
	txResp, err = client.ReplicateTransaction(context.Background(), txReq2)
	if err != nil {
		log.Fatalf("ABORT failed: %v", err)
	}

	if txResp.Success {
		fmt.Printf("✓ ABORT successful - Transaction ID: %d\n", txReq2.TxnId)
	} else {
		fmt.Printf("✗ ABORT failed - Error: %s\n", txResp.ErrorMessage)
	}

	fmt.Println("\n✅ All replication handler tests completed!")
}
