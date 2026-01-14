package main

import (
	"encoding/csv"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/tamararankovic/digest_diffusion/config"
	"github.com/tamararankovic/digest_diffusion/peers"
)

type PartialResult struct {
	Round     int
	Aggregate []IntermediateMetric
}

type LastMax struct {
	Msg MaxAgg
}

type Node struct {
	ID             string
	Value          float64
	Parent         *peers.Peer
	PartialResults map[string]PartialResult
	LastMaxResults map[string]LastMax
	CurrentMax     LastMax
	MaxSeqNo       int
	Stagnation     map[string]int
	LastSeq        map[string]int
	LastResult     AggResult

	TAgg int
	Rmax int

	Peers         *peers.Peers
	lock          *sync.Mutex
	latestMetrics map[string]string
	latestIM      map[string][]IntermediateMetric
	rules         []AggregationRule
}

func main() {
	time.Sleep(10 * time.Second)

	cfg := config.LoadConfigFromEnv()
	params := config.LoadParamsFromEnv()

	ps, err := peers.NewPeers(cfg)
	if err != nil {
		log.Fatal(err)
	}

	val, err := strconv.Atoi(params.ID)
	if err != nil {
		log.Fatal(err)
	}

	lastRcvd := make(map[string]int)
	round := 0

	node := &Node{
		ID:             params.ID,
		Value:          float64(val),
		PartialResults: make(map[string]PartialResult),
		LastMaxResults: make(map[string]LastMax),
		Stagnation:     make(map[string]int),
		LastSeq:        make(map[string]int),
		TAgg:           params.Tagg,
		Rmax:           params.Rmax,
		Peers:          ps,
		lock:           &sync.Mutex{},
		latestMetrics:  make(map[string]string),
		latestIM:       make(map[string][]IntermediateMetric),
		rules: []AggregationRule{
			{
				InputSelector: MetricMetadata{
					Name: "app_memory_usage_bytes",
				},
				Func: SUM_FUNC,
				Output: MetricMetadata{
					Name: "total_app_memory_usage_bytes",
					Labels: map[string]string{
						"func": "sum",
					},
				},
			},
			{
				InputSelector: MetricMetadata{
					Name: "app_memory_usage_bytes",
				},
				Func: AVG_FUNC,
				Output: MetricMetadata{
					Name: "avg_app_memory_usage_bytes",
					Labels: map[string]string{
						"func": "avg",
					},
				},
			},
		},
	}
	currMax := LastMax{
		Msg: MaxAgg{
			Value:       node.Value,
			Source:      node.ID,
			HopDistance: 0,
			SeqNo:       round,
		},
	}
	node.CurrentMax = currMax
	node.LastMaxResults[node.ID] = currMax

	maxAggMsgs := make(map[peers.Peer]*MaxAgg)
	aggMsgs := make(map[peers.Peer]*Agg)
	aggResultMsgs := make(map[peers.Peer]*AggResult)

	// handle messages
	go func() {
		for msgRcvd := range ps.Messages {
			msg := BytesToMsg(msgRcvd.MsgBytes)
			if msg == nil {
				continue
			}
			node.lock.Lock()
			switch msg.Type() {
			case MAX_AGG_MSG_TYPE:
				maxAggMsgs[msgRcvd.Sender] = msg.(*MaxAgg)
			case AGG_MSG_TYPE:
				aggMsgs[msgRcvd.Sender] = msg.(*Agg)
			case AGG_RESULT_MSG_TYPE:
				aggResultMsgs[msgRcvd.Sender] = msg.(*AggResult)
			}
			node.lock.Unlock()
		}
	}()

	go func() {
		for range time.NewTicker(time.Second).C {
			node.exportMsgCount()
			node.exportResult(node.LastResult.Aggregate, "", 0, time.Now().UnixNano())
		}
	}()

	// rounds
	go func() {
		for range time.NewTicker(time.Duration(node.TAgg) * time.Second).C {
			round++
			node.lock.Lock()
			for sender, msg := range maxAggMsgs {
				lastRcvd[sender.GetID()] = round
				node.onMax(msg, sender, round)
			}
			for sender, msg := range aggMsgs {
				lastRcvd[sender.GetID()] = round
				node.onAgg(msg, sender, round)
			}
			for sender, msg := range aggResultMsgs {
				lastRcvd[sender.GetID()] = round
				node.onAggResult(msg, sender)
			}
			maxAggMsgs = make(map[peers.Peer]*MaxAgg)
			aggMsgs = make(map[peers.Peer]*Agg)
			aggResultMsgs = make(map[peers.Peer]*AggResult)
			for _, peer := range ps.GetPeers() {
				if lastRcvd[peer.GetID()]+params.Rmax < round && round > 10 {
					ps.PeerFailed(peer.GetID())
				}
			}
			node.syncMaxState(round)
			node.syncAggState(round)
			node.sendMax()
			node.sendAgg()
			node.lock.Unlock()
		}
	}()

	r := http.NewServeMux()
	r.HandleFunc("POST /metrics", node.setMetricsHandler)
	log.Println("Metrics server listening")

	log.Fatal(http.ListenAndServe(cfg.ListenIP+":9200", r))
}

func (m *Node) exportResult(ims []IntermediateMetric, tree string, reqTimestamp, rcvTimestamp int64) {
	var metric *IntermediateMetric
	for _, im := range ims {
		if im.Metadata.Name == "avg_app_memory_usage_bytes" {
			metric = &im
		}
	}
	if metric == nil {
		return
	}
	// for _, im := range ims {
	// 	name := im.Metadata.Name + "{ "
	// 	for _, k := range slices.Sorted(maps.Keys(im.Metadata.Labels)) {
	// 		name += k + "=" + im.Metadata.Labels[k] + " "
	// 	}
	// 	name += "}"
	// 	filename := fmt.Sprintf("/var/log/digest_diffusion/%s.csv", name)
	filename := "/var/log/digest_diffusion/value.csv"
	writer := writers[filename]
	if writer == nil {
		file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			log.Printf("failed to open/create file: %v", err)
			// continue
			return
		}
		writer = csv.NewWriter(file)
		writers[filename] = writer
	}
	defer writer.Flush()
	reqTsStr := strconv.Itoa(int(reqTimestamp))
	rcvTsStr := strconv.Itoa(int(rcvTimestamp))
	valStr := strconv.FormatFloat(metric.Result.ComputeFinal(), 'f', -1, 64)
	err := writer.Write([]string{tree, reqTsStr, rcvTsStr, valStr})
	if err != nil {
		log.Println(err)
	}
	// }
}

var writers map[string]*csv.Writer = map[string]*csv.Writer{}

func (m *Node) exportMsgCount() {
	filename := "/var/log/digest_diffusion/msg_count.csv"
	writer := writers[filename]
	if writer == nil {
		file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			log.Printf("failed to open/create file: %v", err)
			return
		}
		writer = csv.NewWriter(file)
		writers[filename] = writer
	}
	defer writer.Flush()
	tsStr := strconv.Itoa(int(time.Now().UnixNano()))
	peers.MessagesSentLock.Lock()
	sent := peers.MessagesSent
	peers.MessagesSentLock.Unlock()
	peers.MessagesRcvdLock.Lock()
	rcvd := peers.MessagesRcvd
	peers.MessagesRcvdLock.Unlock()
	sentStr := strconv.Itoa(sent)
	rcvdStr := strconv.Itoa(rcvd)
	err := writer.Write([]string{tsStr, sentStr, rcvdStr})
	if err != nil {
		log.Println(err)
	}
}
