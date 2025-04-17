package main

import (
	"encoding/binary"
	"encoding/json"
	"log"
	"math/big"
	"os"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	uuid "github.com/satori/go.uuid"
)

// TODO: Make set class based of a hashmap

type ExtendedNode struct {
	*maelstrom.Node                                          // Embed the Maelstrom node
	messages        map[interface{}]struct{}                 // Your local storage
	known_messages  map[interface{}]map[interface{}]struct{} // Messages That we know the other nodes have -> Basically using the inner map as a set
	// Messages that we have communicated with other nodes -> Also using the inner map as a set
	// This will be used to help identify and errors or leaks when nodes sync
	msg_communicated map[interface{}]map[interface{}]struct{}
}

func NewExtendedNode() *ExtendedNode {
	return &ExtendedNode{
		Node:     maelstrom.NewNode(),
		messages: make(map[interface{}]struct{}, 0), // Initialize your storage
	}
}

func gen_uuid() string {
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

		generated_id := gen_uuid()
		body["type"] = "generate_ok"
		body["id"] = generated_id
		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		// Create slice with exact size
		keys := make([]interface{}, len(n.messages))

		// Iterate with index
		i := 0
		for key := range n.messages {
			keys[i] = key
			i++
		}

		body["type"] = "read_ok"
		body["messages"] = keys
		return n.Reply(msg, body)
	})

	n.Handle("consume", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Check if we've already processed this message
		message := body["message"]
		if _, exists := n.messages[message]; exists {
			// We've already seen this message, no need to process it again
			var merged_body map[string]any = map[string]any{
				"type": "echo",
			}
			return n.Reply(msg, merged_body)
		}

		n.messages[message] = struct{}{}
		return n.Reply(msg, body)
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		message := body["message"]
		n.messages[message] = struct{}{}
		topology := n.filter_self(n.NodeIDs())

		// Send to neighbors according to topology
		if neighbors, ok := topology[n.ID()]; ok {
			for _, neighbor := range neighbors {
				forward_msg := map[string]any{
					"type":    "consume",
					"message": message,
					"from":    n.ID(),
				}
				n.Send(neighbor, forward_msg)
			}
		}

		var merged_body map[string]any = map[string]any{
			"type": "broadcast_ok",
		}

		return n.Reply(msg, merged_body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		topology_as_map := n.filter_self(n.NodeIDs())
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
