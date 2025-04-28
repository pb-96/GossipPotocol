package gossip

import (
	"maelstrom-broadcast/internal/node"
	"time"
)

type Gossip struct {
	ticker *time.Ticker
	done   chan bool
}

type GossipMsgs struct {
	Type string `json:"type"`
	Msgs []int  `json:"msgs"`
}

func NewGossip(d time.Duration) *Gossip {
	return &Gossip{
		ticker: time.NewTicker(d),
		done:   make(chan bool),
	}
}

func (gossip *Gossip) Start(n *node.Node) {
	go func() {
		for {
			select {
			case <-gossip.done:
				gossip.Stop()

			case <-gossip.ticker.C:
				err := doGossip(n)
				if err != nil {
					return
				}
			}
		}
	}()
}

func (gossip *Gossip) Stop() {
	gossip.done <- true
}

func doGossip(n *node.Node) error {
	nodes := n.N.NodeIDs()
	data := n.Store.GetAll()

	// Create message here no ?
	msg := &GossipMsgs{
		Type: "gossip",
		Msgs: data,
	}

	for _, peer := range nodes {
		go func(peer string) {
			for {
				err := n.N.Send(peer, msg)
				if err == nil {
					break
				}
			}
		}(peer)
	}

	return nil
}
