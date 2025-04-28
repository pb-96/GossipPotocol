package main

import (
	"encoding/json"
	"maelstrom-broadcast/internal/node"
	gossip "maelstrom-broadcast/internal/p2pGossip"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type NWTopology struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

type GossipMsgs struct {
	Type string `json:"type"`
	Msgs []int  `json:"msgs"`
}

func main() {
	node := node.NewNode()
	gossip := gossip.NewGossip(100 * time.Millisecond)

	node.N.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		m := body["message"]
		v := int(m.(float64))
		node.Store.Set(v)
		gossip.Start(node)

		return node.N.Reply(msg, map[string]interface{}{
			"type": "broadcast_ok",
		})
	})

	node.N.Handle("gossip", func(msg maelstrom.Message) error {
		var body GossipMsgs
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Set all here
		node.Store.SetAll(body.Msgs)
		return node.N.Reply(msg, body)
	})

	node.N.Handle("topology", func(msg maelstrom.Message) error {
		var body NWTopology

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		node.SetTopology(body.Topology[node.N.ID()])

		return node.N.Reply(msg, map[string]interface{}{
			"type": "topology_ok",
		})
	})
}
