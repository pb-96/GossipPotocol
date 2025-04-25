package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"os"
	"time"

	"github.com/emirpasic/gods/trees/btree"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
)

var logger *log.Logger

func initLogger() {
	logger = log.New()
	logFile, _ := os.OpenFile("./maelstrom.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	logger.SetOutput(logFile)
}

// TODO: Make set class based of a hashmap
type ExtendedNode struct {
	*maelstrom.Node
	// Here would represent the topology itself
	tree     *btree.Tree
	messages map[interface{}]struct{}
	// Messages That we know the other nodes have -> Basically using the inner map as a set
	known_messages map[interface{}]map[interface{}]struct{}
	/*
		Messages that we have communicated with other nodes -> Also using the inner map as a set
		This will be used to help identify and errors or leaks when nodes sync
	*/
	msg_communicated map[interface{}]map[interface{}]struct{}
}

type SeverNode struct {
	// Could represent the sever here
	*ExtendedNode
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

func (n *ExtendedNode) get_topology() *btree.Tree {
	// Placeholder to generate a random tree
	tree := btree.NewWithIntComparator(len(n.NodeIDs()))
	for i := 0; i < len(n.NodeIDs()); i++ {
		tree.Put(i, fmt.Sprintf("n%d", i))
	}
	return *tree
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

func (n *ExtendedNode) topologyHandler(msg maelstrom.Message) error {
	topology_as_map := n.get_topology()
	var merged_body map[string]any = map[string]any{
		"type": "topology_ok",
	}
	if len(topology_as_map) > 0 {
		merged_body["topology"] = topology_as_map
	}
	return n.Reply(msg, merged_body)
}

func (n *ExtendedNode) broadCastHandler(msg maelstrom.Message) error {
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
}

func (n *ExtendedNode) handleConsume(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	message := body["message"]
	if _, exists := n.messages[message]; exists {
		return nil
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

	return nil
}

func (n *ExtendedNode) handleRead(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	body["type"] = "read_ok"
	body["messages"] = n.get_messages()
	return n.Reply(msg, body)
}

func (n *ExtendedNode) handleGenerate(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	generated_id := n.gen_uuid()
	body["type"] = "generate_ok"
	body["id"] = generated_id
	return n.Reply(msg, body)
}

func (n *ExtendedNode) handleEcho(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	body["type"] = "echo_ok"
	return n.Reply(msg, body)
}

func main() {
	n := NewExtendedNode()
	initLogger()

	n.Handle("echo", n.handleEcho)
	n.Handle("generate", n.handleGenerate)
	n.Handle("read", n.handleRead)
	n.Handle("consume", n.handleConsume)
	n.Handle("broadcast", n.broadCastHandler)
	n.Handle("topology", n.topologyHandler)

	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
