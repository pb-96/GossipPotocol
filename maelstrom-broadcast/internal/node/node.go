package node

import (
	"log"
	"maelstrom-broadcast/internal/cache"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Node represents a node in the distributed system.
type Node struct {
	N        *maelstrom.Node // Reference to the Maelstrom node
	Store    cache.KVStore   // Each node has got its own KV store
	Topology []string        // Each node will have its own topology
}

func NewNode() *Node {
	return &Node{
		N:     maelstrom.NewNode(),
		Store: *cache.NewCache(),
	}
}

func (n *Node) SetTopology(config []string) {
	n.Topology = config
}

func (n *Node) GetTopology() []string {

	if len(n.Topology) == 0 {
		log.Printf("no topology initialized for the node %s", n.N.ID())
		return n.N.NodeIDs()
	}

	return n.Topology
}
