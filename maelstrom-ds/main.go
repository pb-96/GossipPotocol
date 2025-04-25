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
	logrus "github.com/sirupsen/logrus"
)

var logger *logrus.Logger

func initLogger() {
	logger = logrus.New()
	logger.SetFormatter(&logrus.TextFormatter{})
	logger.SetOutput(os.Stdout)
	logger.SetLevel(logrus.InfoLevel)
	logFile, _ := os.OpenFile("./maelstrom.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	logger.SetOutput(logFile)
}

// TODO: Make set class based of a hashmap
type ExtendedNode struct {
	*maelstrom.Node
	// Here would represent the topology itself
	messages map[interface{}]struct{}
	// Messages That we know the other nodes have -> Basically using the inner map as a set
	known_messages map[interface{}]map[interface{}]struct{}
	/*
		Messages that we have communicated with other nodes -> Also using the inner map as a set
		This will be used to help identify and errors or leaks when nodes sync
	*/
	msg_communicated map[interface{}]map[interface{}]struct{}
}

type ServerNode struct {
	// Could represent the sever here
	n    *ExtendedNode
	ids  map[int]struct{}
	tree *btree.Tree
	// These are all of the attributes we would need to make this a gossip protocol
	// nodesMu      sync.RWMutex
	// broadcastsMu sync.Mutex
	// idsMu        sync.RWMutex
	broadcasts map[string][]int
	// kv           *maelstrom.KV
	// mu           sync.Mutex
	// cache        map[string]int
}

func NewExtendedNode() *ExtendedNode {
	return &ExtendedNode{
		Node:     maelstrom.NewNode(),
		messages: make(map[interface{}]struct{}, 0), // Initialize your storage
	}
}

func NewSeverNode() *ServerNode {
	return &ServerNode{
		n:          NewExtendedNode(),
		ids:        make(map[int]struct{}, 0),
		tree:       btree.NewWithIntComparator(2),
		broadcasts: make(map[string][]int, 0),
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

func (s *ServerNode) get_topology() *btree.Tree {
	// Placeholder to generate a random tree
	tree := btree.NewWithIntComparator(len(s.n.NodeIDs()))
	for i := 0; i < len(s.n.NodeIDs()); i++ {
		tree.Put(i, fmt.Sprintf("n%d", i))
	}
	return tree
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

func (s *ServerNode) topologyHandler(msg maelstrom.Message) error {
	var merged_body map[string]any = map[string]any{
		"type": "topology_ok",
	}
	return s.n.Reply(msg, merged_body)
}

func (s *ServerNode) broadCastHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	message := body["message"]
	s.n.messages[message] = struct{}{}
	topology := s.n.filter_self(s.n.NodeIDs())
	from_nodes := []string{s.n.ID()}
	forward_msg := map[string]any{
		"type":        "consume",
		"message":     message,
		"direct_from": s.n.ID(),
		"from":        from_nodes,
	}

	// Send to neighbors according to topology
	if neighbors, ok := topology[s.n.ID()]; ok {
		for _, neighbor := range neighbors {
			s.n.Send(neighbor, forward_msg)
		}
	}
	var merged_body map[string]any = map[string]any{
		"type": "broadcast_ok",
	}
	return s.n.Reply(msg, merged_body)
}

func (s *ServerNode) handleConsume(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	message := body["message"]
	if _, exists := s.n.messages[message]; exists {
		return nil
	}
	s.n.messages[message] = struct{}{}

	topology := s.get_topology()
	neighbor, _ := topology.Get(s.n.ID())

	rawFromNodes := body["from"].([]any)
	fromSet := make(map[string]struct{}, len(rawFromNodes))
	for _, node := range rawFromNodes {
		fromSet[node.(string)] = struct{}{}
	}

	directFrom := body["direct_from"].(string)
	toSend := []string{}

	if neighborStr, ok := neighbor.(string); ok {
		if _, seen := fromSet[neighborStr]; !seen || neighborStr != directFrom {
			if neighborStr, ok := neighbor.(string); ok {
				fromSet[neighborStr] = struct{}{}
				toSend = append(toSend, neighborStr)
			}
			toSend = append(toSend, neighborStr)
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
			"direct_from": s.n.ID(),
			"from":        updatedFrom,
		}
		s.n.Send(neighbor, forward)
	}

	return nil
}

func (s *ServerNode) handleRead(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	body["type"] = "read_ok"
	body["messages"] = s.n.get_messages()
	return s.n.Reply(msg, body)
}

func (s *ServerNode) handleGenerate(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	generated_id := s.n.gen_uuid()
	body["type"] = "generate_ok"
	body["id"] = generated_id
	return s.n.Reply(msg, body)
}

func (s *ServerNode) handleEcho(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	body["type"] = "echo_ok"
	return s.n.Reply(msg, body)
}

func main() {
	s := NewSeverNode()
	initLogger()

	s.n.Node.Handle("echo", s.handleEcho)
	s.n.Handle("generate", s.handleGenerate)
	s.n.Handle("read", s.handleRead)
	s.n.Handle("consume", s.handleConsume)
	s.n.Handle("broadcast", s.broadCastHandler)
	s.n.Handle("topology", s.topologyHandler)

	if err := s.n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
