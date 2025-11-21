package grpc

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

// ConsistentHash implements consistent hashing with virtual nodes
type ConsistentHash struct {
	replicas int               // Number of replicas (N in Cassandra terms)
	vnodes   int               // Virtual nodes per physical node
	ring     []uint64          // Sorted hash ring
	ringMap  map[uint64]uint64 // hash -> nodeID
	nodes    map[uint64]bool   // Set of physical nodes
	mu       sync.RWMutex
}

// NewConsistentHash creates a new consistent hash
func NewConsistentHash(replicas, vnodes int) *ConsistentHash {
	return &ConsistentHash{
		replicas: replicas,
		vnodes:   vnodes,
		ring:     make([]uint64, 0),
		ringMap:  make(map[uint64]uint64),
		nodes:    make(map[uint64]bool),
	}
}

// AddNode adds a physical node to the hash ring
func (ch *ConsistentHash) AddNode(nodeID uint64) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.nodes[nodeID] {
		return // Already exists
	}

	ch.nodes[nodeID] = true

	// Add virtual nodes
	for i := 0; i < ch.vnodes; i++ {
		vnode := ch.hashVNode(nodeID, i)
		ch.ring = append(ch.ring, vnode)
		ch.ringMap[vnode] = nodeID
	}

	// Sort the ring
	sort.Slice(ch.ring, func(i, j int) bool {
		return ch.ring[i] < ch.ring[j]
	})
}

// RemoveNode removes a physical node from the hash ring
func (ch *ConsistentHash) RemoveNode(nodeID uint64) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if !ch.nodes[nodeID] {
		return // Doesn't exist
	}

	delete(ch.nodes, nodeID)

	// Remove virtual nodes
	newRing := make([]uint64, 0)
	for _, vnode := range ch.ring {
		if ch.ringMap[vnode] != nodeID {
			newRing = append(newRing, vnode)
		} else {
			delete(ch.ringMap, vnode)
		}
	}

	ch.ring = newRing
}

// GetNode returns the primary node responsible for a key
func (ch *ConsistentHash) GetNode(key string) (uint64, error) {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.ring) == 0 {
		return 0, fmt.Errorf("no nodes in ring")
	}

	hash := ch.hashKey(key)

	// Binary search for the first node >= hash
	idx := sort.Search(len(ch.ring), func(i int) bool {
		return ch.ring[i] >= hash
	})

	// Wrap around if necessary
	if idx >= len(ch.ring) {
		idx = 0
	}

	vnode := ch.ring[idx]
	nodeID := ch.ringMap[vnode]

	return nodeID, nil
}

// GetReplicas returns N nodes responsible for a key (for replication)
func (ch *ConsistentHash) GetReplicas(key string) []uint64 {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.ring) == 0 {
		return nil
	}

	hash := ch.hashKey(key)

	// Find starting position
	idx := sort.Search(len(ch.ring), func(i int) bool {
		return ch.ring[i] >= hash
	})

	if idx >= len(ch.ring) {
		idx = 0
	}

	// Collect N distinct physical nodes
	replicas := make([]uint64, 0, ch.replicas)
	seen := make(map[uint64]bool)

	for len(replicas) < ch.replicas && len(replicas) < len(ch.nodes) {
		vnode := ch.ring[idx]
		nodeID := ch.ringMap[vnode]

		if !seen[nodeID] {
			replicas = append(replicas, nodeID)
			seen[nodeID] = true
		}

		idx = (idx + 1) % len(ch.ring)
	}

	return replicas
}

// GetNodes returns all physical nodes
func (ch *ConsistentHash) GetNodes() []uint64 {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	nodes := make([]uint64, 0, len(ch.nodes))
	for nodeID := range ch.nodes {
		nodes = append(nodes, nodeID)
	}

	return nodes
}

// Count returns the number of physical nodes
func (ch *ConsistentHash) Count() int {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	return len(ch.nodes)
}

// hashKey hashes a key to a position on the ring using MD5
func (ch *ConsistentHash) hashKey(key string) uint64 {
	hash := md5.Sum([]byte(key))
	// Use first 8 bytes as uint64
	return binary.BigEndian.Uint64(hash[:8])
}

// hashVNode hashes a virtual node identifier using MD5
func (ch *ConsistentHash) hashVNode(nodeID uint64, vnodeIndex int) uint64 {
	data := fmt.Sprintf("%d:%d", nodeID, vnodeIndex)
	hash := md5.Sum([]byte(data))
	// Use first 8 bytes as uint64
	return binary.BigEndian.Uint64(hash[:8])
}

// DistributionStats returns statistics about key distribution
func (ch *ConsistentHash) DistributionStats() map[uint64]int {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	stats := make(map[uint64]int)

	for _, vnode := range ch.ring {
		nodeID := ch.ringMap[vnode]
		stats[nodeID]++
	}

	return stats
}
