package grpc

// NodeRegistryAdapter wraps NodeRegistry to provide []any interface
type NodeRegistryAdapter struct {
	*NodeRegistry
}

// GetAll returns nodes as []any to avoid import cycles
func (a *NodeRegistryAdapter) GetAll() []any {
	nodes := a.NodeRegistry.GetAll()
	result := make([]any, len(nodes))
	for i, node := range nodes {
		result[i] = node
	}
	return result
}

// NewNodeRegistryAdapter creates an adapter
func NewNodeRegistryAdapter(nr *NodeRegistry) *NodeRegistryAdapter {
	return &NodeRegistryAdapter{NodeRegistry: nr}
}
