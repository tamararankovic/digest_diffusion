package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"maps"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/c12s/hyparview/data"
	"github.com/c12s/hyparview/hyparview"
	"github.com/c12s/hyparview/transport"
	"github.com/caarlos0/env"
)

type PartialResult struct {
	Time      int64
	Aggregate []IntermediateMetric
}

type LastMax struct {
	Time int64
	Msg  MaxAgg
}

type Node struct {
	ID             string
	Value          float64
	Parent         *hyparview.Peer
	PartialResults map[string]PartialResult
	LastMaxResults map[string]LastMax
	CurrentMax     LastMax
	LastResult     AggResult

	TAgg                int
	TAggMax             int
	TTL                 int
	InactivityIntervals int

	hv            *hyparview.HyParView
	lock          *sync.Mutex
	logger        *log.Logger
	latestMetrics map[string]string
	latestIM      map[string][]IntermediateMetric
	rules         []AggregationRule
}

func main() {
	hvConfig := hyparview.Config{}
	err := env.Parse(&hvConfig)
	if err != nil {
		log.Fatal(err)
	}

	cfg := Config{}
	err = env.Parse(&cfg)
	if err != nil {
		log.Fatal(err)
	}

	self := data.Node{
		ID:            cfg.NodeID,
		ListenAddress: cfg.ListenAddr,
	}

	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)

	gnConnManager := transport.NewConnManager(
		transport.NewTCPConn,
		transport.AcceptTcpConnsFn(self.ListenAddress),
	)

	hv, err := hyparview.NewHyParView(hvConfig, self, gnConnManager, logger)
	if err != nil {
		log.Fatal(err)
	}

	val, err := strconv.Atoi(strings.Split(cfg.NodeID, "_")[2])
	if err != nil {
		logger.Fatal(err)
	}

	node := &Node{
		ID:                  cfg.NodeID,
		Value:               float64(val),
		PartialResults:      make(map[string]PartialResult),
		LastMaxResults:      make(map[string]LastMax),
		TAgg:                cfg.TAgg,
		TAggMax:             cfg.TAggMax,
		TTL:                 cfg.TTL,
		InactivityIntervals: cfg.InactivityIntervals,
		hv:                  hv,
		lock:                &sync.Mutex{},
		logger:              logger,
		latestMetrics:       make(map[string]string),
		latestIM:            make(map[string][]IntermediateMetric),
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
		Time: time.Now().UnixNano(),
		Msg: MaxAgg{
			Value:       node.Value,
			Source:      node.ID,
			HopDistance: 0,
			TTL:         node.TTL,
		},
	}
	node.CurrentMax = currMax
	node.LastMaxResults[node.ID] = currMax

	hv.AddClientMsgHandler(AGG_MAX_MSG_TYPE, func(msgBytes []byte, sender hyparview.Peer) {
		msg := MaxAgg{}
		err := transport.Deserialize(msgBytes, &msg)
		if err != nil {
			logger.Println(node.ID, "-", "Error unmarshaling message:", err)
			return
		}
		node.onMax(msg, sender)
	})
	hv.AddClientMsgHandler(AGG_MSG_TYPE, func(msgBytes []byte, sender hyparview.Peer) {
		msg := Agg{}
		err := transport.Deserialize(msgBytes, &msg)
		if err != nil {
			logger.Println(node.ID, "-", "Error unmarshaling message:", err)
			return
		}
		node.onAgg(msg, sender)
	})
	hv.AddClientMsgHandler(AGG_MSG_RESP_TYPE, func(msgBytes []byte, sender hyparview.Peer) {
		msg := AggResult{}
		err := transport.Deserialize(msgBytes, &msg)
		if err != nil {
			logger.Println(node.ID, "-", "Error unmarshaling message:", err)
			return
		}
		node.onAggResult(msg, sender)
	})

	go func() {
		for range time.NewTicker(time.Second).C {
			node.exportMsgCount()
			node.exportResult(node.LastResult.Aggregate, "", 0, time.Now().UnixNano())
		}
	}()

	err = hv.Join(cfg.ContactID, cfg.ContactAddr)
	if err != nil {
		logger.Fatal(err)
	}

	go node.sendMax()
	go node.sendAgg()
	go node.syncMaxState()
	go node.syncAggState()

	r := http.NewServeMux()
	r.HandleFunc("POST /metrics", node.setMetricsHandler)
	log.Println("Metrics server listening")

	go func() {
		log.Fatal(http.ListenAndServe(strings.Split(os.Getenv("LISTEN_ADDR"), ":")[0]+":9200", r))
	}()

	r2 := http.NewServeMux()
	r2.HandleFunc("GET /state", node.StateHandler)
	log.Println("State server listening on :5001/state")
	log.Fatal(http.ListenAndServe(strings.Split(os.Getenv("LISTEN_ADDR"), ":")[0]+":5001", r2))
}

func (m *Node) exportResult(ims []IntermediateMetric, tree string, reqTimestamp, rcvTimestamp int64) {
	for _, im := range ims {
		name := im.Metadata.Name + "{ "
		for _, k := range slices.Sorted(maps.Keys(im.Metadata.Labels)) {
			name += k + "=" + im.Metadata.Labels[k] + " "
		}
		name += "}"
		filename := fmt.Sprintf("/var/log/monoceros/results/%s.csv", name)
		// defer file.Close()
		writer := writers[filename]
		if writer == nil {
			file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
			if err != nil {
				m.logger.Printf("failed to open/create file: %v", err)
				continue
			}
			writer = csv.NewWriter(file)
			writers[filename] = writer
		}
		defer writer.Flush()
		reqTsStr := strconv.Itoa(int(reqTimestamp))
		rcvTsStr := strconv.Itoa(int(rcvTimestamp))
		valStr := strconv.FormatFloat(im.Result.ComputeFinal(), 'f', -1, 64)
		err := writer.Write([]string{tree, reqTsStr, rcvTsStr, valStr})
		if err != nil {
			m.logger.Println(err)
		}
	}
}

var writers map[string]*csv.Writer = map[string]*csv.Writer{}

func (m *Node) exportMsgCount() {
	filename := "/var/log/monoceros/results/msg_count.csv"
	// defer file.Close()
	writer := writers[filename]
	if writer == nil {
		file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			m.logger.Printf("failed to open/create file: %v", err)
			return
		}
		writer = csv.NewWriter(file)
		writers[filename] = writer
	}
	defer writer.Flush()
	tsStr := strconv.Itoa(int(time.Now().UnixNano()))
	transport.MessagesSentLock.Lock()
	sent := transport.MessagesSent - transport.MessagesSentSub
	transport.MessagesSentLock.Unlock()
	transport.MessagesRcvdLock.Lock()
	rcvd := transport.MessagesRcvd - transport.MessagesRcvdSub
	transport.MessagesRcvdLock.Unlock()
	sentStr := strconv.Itoa(sent)
	rcvdStr := strconv.Itoa(rcvd)
	err := writer.Write([]string{tsStr, sentStr, rcvdStr})
	if err != nil {
		m.logger.Println(err)
	}
}
