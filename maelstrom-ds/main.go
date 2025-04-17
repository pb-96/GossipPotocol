package main

import (
	"encoding/binary"
	"encoding/json"
	"log"
	"math"
	"math/big"
	"math/rand/v2"
	"os"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	uuid "github.com/satori/go.uuid"
)

// TODO: Make set class based of a hashmap
type ExtendedNode struct {
	*maelstrom.Node
	messages map[interface{}]struct{}
	// Messages That we know the other nodes have -> Basically using the inner map as a set
	known_messages map[interface{}]map[interface{}]struct{}
	/*
		Messages that we have communicated with other nodes -> Also using the inner map as a set
		This will be used to help identify and errors or leaks when nodes sync
	*/
	msg_communicated map[interface{}]map[interface{}]struct{}
}

func NewExtendedNode() *ExtendedNode {
	return &ExtendedNode{
		Node:     maelstrom.NewNode(),
		messages: make(map[interface{}]struct{}, 0), // Initialize your storage
	}
}

func (n *ExtendedNode) gen_uuid() string {
	// Generate UUID part
	uuid := uuid.NewV4()
	uuidStr := new(big.Int).SetBytes(uuid[:]).Text(62)

	// Generate timestamp part
	timestamp := make([]byte, 8)
	binary.LittleEndian.PutUint64(timestamp, uint64(time.Now().Unix()))
	timeStr := new(big.Int).SetBytes(timestamp).Text(62)

	return timeStr + ":" + uuidStr
}

func (n *ExtendedNode) filter_self(topology []string) map[string][]string {
	seen := make(map[string][]string)
	other_nodes := make([]string, 0)
	for _, node := range topology {
		if node == n.ID() {
			continue
		} else {
			other_nodes = append(other_nodes, node)
		}
	}
	if len(other_nodes) == 0 {
		return seen
	} else {
		seen[n.ID()] = other_nodes
		return seen
	}
}

func (n *ExtendedNode) get_topology() map[string][]string {
	// Placeholder to generate a random tree
	nodeIDs := n.NodeIDs()
	numNodes := len(nodeIDs)
	topology := make(map[string][]string, numNodes)
	for _, id := range nodeIDs {
		topology[id] = []string{}
	}

	rand.Shuffle(numNodes, func(i, j int) {
		nodeIDs[i], nodeIDs[j] = nodeIDs[j], nodeIDs[i]
	})

	for i := 0; i < numNodes; i++ {
		// Assuming rand.IntN -> returns something greater > 0
		randomNum := float64(rand.IntN(numNodes - 1))
		thisLength := int(math.Floor(math.Max(randomNum, 2)))
		for j := 0; j < thisLength; j++ {
			a := nodeIDs[j]
			b := nodeIDs[rand.IntN(j+1)]
			topology[a] = append(topology[a], b)
			topology[b] = append(topology[b], a)
		}
	}

	return topology
}

func (n *ExtendedNode) get_messages() []interface{} {
	keys := make([]interface{}, len(n.messages))
	i := 0
	for key := range n.messages {
		keys[i] = key
		i++
	}
	return keys
}

func main() {
	n := NewExtendedNode()
	n.Handle("echo", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		body["type"] = "echo_ok"
		return n.Reply(msg, body)
	})

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		generated_id := n.gen_uuid()
		body["type"] = "generate_ok"
		body["id"] = generated_id
		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		body["type"] = "read_ok"
		body["messages"] = n.get_messages()
		return n.Reply(msg, body)
	})

	n.Handle("consume", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		message := body["message"]
		if _, exists := n.messages[message]; exists {
			return n.Reply(msg, map[string]any{"type": "echo"})
		}
		n.messages[message] = struct{}{}

		topology := n.get_topology()
		neighbors := topology[n.ID()]

		rawFromNodes := body["from"].([]any)
		fromSet := make(map[string]struct{}, len(rawFromNodes))
		for _, node := range rawFromNodes {
			fromSet[node.(string)] = struct{}{}
		}

		directFrom := body["direct_from"].(string)
		toSend := []string{}

		for _, neighbor := range neighbors {
			if neighbor == directFrom {
				continue
			}
			if _, seen := fromSet[neighbor]; !seen {
				fromSet[neighbor] = struct{}{}
				toSend = append(toSend, neighbor)
			}
		}

		updatedFrom := make([]string, 0, len(fromSet))
		for node := range fromSet {
			updatedFrom = append(updatedFrom, node)
		}

		for _, neighbor := range toSend {
			forward := map[string]any{
				"type":        "consume",
				"message":     message,
				"direct_from": n.ID(),
				"from":        updatedFrom,
			}
			n.Send(neighbor, forward)
		}

		// Need to change this so it only sends echo message when the message has been sent twice

		return n.Reply(msg, map[string]any{"type": "echo"})
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		message := body["message"]
		n.messages[message] = struct{}{}
		topology := n.filter_self(n.NodeIDs())
		from_nodes := []string{n.ID()}
		forward_msg := map[string]any{
			"type":        "consume",
			"message":     message,
			"direct_from": n.ID(),
			"from":        from_nodes,
		}

		// Send to neighbors according to topology
		if neighbors, ok := topology[n.ID()]; ok {
			for _, neighbor := range neighbors {

				n.Send(neighbor, forward_msg)
			}
		}
		var merged_body map[string]any = map[string]any{
			"type": "broadcast_ok",
		}
		return n.Reply(msg, merged_body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		topology_as_map := n.get_topology()
		var merged_body map[string]any = map[string]any{
			"type": "topology_ok",
		}
		if len(topology_as_map) > 0 {
			merged_body["topology"] = topology_as_map
		}
		return n.Reply(msg, merged_body)
	})
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
