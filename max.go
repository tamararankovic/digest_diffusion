package main

import (
	"log"
	"sort"

	"github.com/tamararankovic/digest_diffusion/peers"
)

// locked by caller
func (n *Node) onMax(msg *MaxAgg, sender peers.Peer, round int) {
	msg.HopDistance++
	msgRcvd := LastMax{
		Msg: *msg,
	}
	n.LastMaxResults[sender.GetID()] = msgRcvd
}

// locked by caller
func (n *Node) sendMax() {
	payload := n.CurrentMax.Msg
	msg := MsgToBytes(payload)
	for _, peer := range n.Peers.GetPeers() {
		peer.Send(msg)
	}
	log.Println("current max source", n.CurrentMax.Msg.Source)
}

// locked by caller
func (n *Node) syncMaxState(round int) {

	// 0. remove failed peers

	activeMax := make(map[string]LastMax)
	for _, peer := range n.Peers.GetPeers() {
		if lm, ok := n.LastMaxResults[peer.GetID()]; ok {
			activeMax[peer.GetID()] = lm
		}
	}
	n.LastMaxResults = activeMax

	//--------------------------------------------------------------------
	// 1. Build buckets: group LastMaxResults + selfMax by Source
	//--------------------------------------------------------------------

	type entry struct {
		from string
		max  LastMax
	}

	buckets := make(map[string][]entry)

	// include peer messages
	for from, msg := range n.LastMaxResults {
		src := msg.Msg.Source
		buckets[src] = append(buckets[src], entry{from, msg})
	}

	// include self
	selfMax := LastMax{
		Msg: MaxAgg{
			Value:       n.Value,
			Source:      n.ID,
			HopDistance: 0,
			SeqNo:       round,
		},
	}
	buckets[n.ID] = append(buckets[n.ID], entry{"", selfMax})

	//--------------------------------------------------------------------
	// 2. Compute max seqno per source (only for staleness detection)
	//--------------------------------------------------------------------

	latestSeq := make(map[string]int) // highest seqno per source

	for src, msgs := range buckets {
		maxSeq := msgs[0].max.Msg.SeqNo
		for _, m := range msgs[1:] {
			if m.max.Msg.SeqNo > maxSeq {
				maxSeq = m.max.Msg.SeqNo
			}
		}
		latestSeq[src] = maxSeq
	}

	//--------------------------------------------------------------------
	// 3. Update staleness counters and remove stale sources
	//--------------------------------------------------------------------

	for src, newSeq := range latestSeq {
		oldSeq := n.LastSeq[src]

		if newSeq > oldSeq {
			// fresh update
			n.Stagnation[src] = 0
			n.LastSeq[src] = newSeq
		} else {
			// no increase → candidate for staleness
			n.Stagnation[src]++
		}
	}

	// purge stale *sources*
	for src := range buckets {
		if n.Stagnation[src] > n.Rmax {
			delete(buckets, src)
			// delete(n.Stagnation, src)
			// delete(n.LastSeq, src)
		}
	}

	if len(buckets) == 0 {
		// only our own self remains
		n.CurrentMax = selfMax
		n.Parent = nil
		return
	}

	//--------------------------------------------------------------------
	// 4. Flatten all messages from all non-stale sources
	//--------------------------------------------------------------------

	type candidate struct {
		from string
		max  LastMax
	}

	cands := make([]candidate, 0)
	for _, list := range buckets {
		for _, e := range list {
			cands = append(cands, candidate{e.from, e.max})
		}
	}

	//--------------------------------------------------------------------
	// 5. Deterministic winner selection
	//--------------------------------------------------------------------

	// Order: Value DESC → Source ASC → HopDistance ASC → From ASC
	sort.Slice(cands, func(i, j int) bool {
		mi := cands[i].max.Msg
		mj := cands[j].max.Msg

		if mi.Value != mj.Value {
			return mi.Value > mj.Value
		}
		if mi.Source != mj.Source {
			return mi.Source < mj.Source
		}
		if mi.HopDistance != mj.HopDistance {
			return mi.HopDistance < mj.HopDistance
		}
		return cands[i].from < cands[j].from
	})

	winner := cands[0]

	//--------------------------------------------------------------------
	// 6. Determine parent
	//--------------------------------------------------------------------

	if winner.from == "" {
		// We chose our own selfMax
		n.Parent = nil
	} else {
		ps := n.Peers.GetPeers()
		var found bool
		for i := range ps {
			if ps[i].GetID() == winner.from {
				if n.Parent == nil || n.Parent.GetID() != ps[i].GetID() {
					log.Println("parent changed to", winner.from)
				}
				n.Parent = &ps[i]
				found = true
				break
			}
		}
		if !found {
			n.Parent = nil
		}
	}

	//--------------------------------------------------------------------
	// 7. Update current max
	//--------------------------------------------------------------------
	n.CurrentMax = winner.max
}
