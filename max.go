package main

import (
	"slices"
	"time"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/hyparview"
)

const AGG_MAX_MSG_TYPE data.MessageType = data.UNKNOWN + 1

func (n *Node) onMax(msg MaxAgg, sender hyparview.Peer) {
	msg.HopDistance++
	// msg.TTL--
	if msg.TTL == 0 {
		return
	}
	msgRcvd := LastMax{
		Time: time.Now().UnixNano(),
		Msg:  msg,
	}
	n.lock.Lock()
	defer n.lock.Unlock()
	n.LastMaxResults[sender.Node.ID] = msgRcvd
}

func (n *Node) sendMax() {
	for range time.NewTicker(time.Duration(n.TAggMax) * time.Second).C {
		n.lock.Lock()
		n.decreaseTTL()
		n.updateMaxState()
		payload := n.CurrentMax.Msg
		n.lock.Unlock()
		msg := data.Message{
			Type:    AGG_MAX_MSG_TYPE,
			Payload: payload,
		}
		for _, peer := range n.hv.GetPeers(1000) {
			err := peer.Conn.Send(msg)
			if err != nil {
				n.logger.Println(err)
			}
		}
		n.logger.Println("current max source", n.CurrentMax.Msg.Source)
	}
}

func (n *Node) syncMaxState() {
	for range time.NewTicker(300 * time.Millisecond).C {
		n.lock.Lock()
		n.updateMaxState()
		n.lock.Unlock()
	}
}

// locked by caller
func (n *Node) decreaseTTL() {
	for from, msg := range n.LastMaxResults {
		msg.Msg.TTL--
		n.LastMaxResults[from] = msg
	}
}

// locked by caller
func (n *Node) updateMaxState() {
	remove := []string{}
	for from, msg := range n.LastMaxResults {
		if msg.Time+int64(n.TAggMax)*int64(n.InactivityIntervals)*1000000000 < time.Now().UnixNano() || msg.Msg.TTL == 0 {
			remove = append(remove, from)
		}
	}
	for _, from := range remove {
		delete(n.LastMaxResults, from)
	}
	currMax := LastMax{
		Time: time.Now().UnixNano(),
		Msg: MaxAgg{
			Value:       n.Value,
			Source:      n.ID,
			HopDistance: 0,
			TTL:         n.TTL,
		},
	}
	from := ""
	for sentBy, msg := range n.LastMaxResults {
		if msg.Msg.Value > currMax.Msg.Value ||
			(msg.Msg.Value == currMax.Msg.Value && msg.Msg.Source < currMax.Msg.Source) ||
			(msg.Msg.Value == currMax.Msg.Value && msg.Msg.Source == currMax.Msg.Source && msg.Msg.HopDistance < currMax.Msg.HopDistance) ||
			(msg.Msg.Value == currMax.Msg.Value && msg.Msg.Source == currMax.Msg.Source && msg.Msg.HopDistance == currMax.Msg.HopDistance && sentBy < from) {
			currMax = msg
			from = sentBy
		}
	}
	n.CurrentMax = currMax
	peers := n.hv.GetPeers(1000)
	idx := slices.IndexFunc(peers, func(p hyparview.Peer) bool {
		return p.Node.ID == from
	})
	if idx == -1 {
		n.Parent = nil
	} else {
		n.Parent = &peers[idx]
		n.logger.Println("parent changed", n.Parent.Node.ID)
	}
}
