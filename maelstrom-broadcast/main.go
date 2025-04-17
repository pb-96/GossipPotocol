package main

import (
	"encoding/json"
	"log"
	"os"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type ExtendedNode struct {
	*maelstrom.Node                          // Embed the Maelstrom node
	messages        map[interface{}]struct{} // Your local storage
	// Add any other fields you need
}

func NewExtendedNode() *ExtendedNode {
	return &ExtendedNode{
		Node:     maelstrom.NewNode(),
		messages: make(map[interface{}]struct{}, 0), // Initialize your storage
	}
}

func main() {
	n := NewExtendedNode()

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
		// Add the numbers from the broadcast to the given node itself
		n.messages[body["message"]] = struct{}{}
		body["type"] = "consume_ok"
		return n.Reply(msg, body)
	})

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		all_channels := n.NodeIDs()
		for _, channel := range all_channels {
			message := map[string]any{
				"type":    "consume",
				"message": body["message"],
			}
			n.Send(channel, message)
		}
		body["type"] = "broadcast_ok"
		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		body["topology"] = n.NodeIDs()
		body["type"] = "topology_ok"
		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
