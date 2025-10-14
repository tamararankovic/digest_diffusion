package main

import (
	"encoding/json"
	"net/http"
)

type State struct {
	ID              string
	RegionalNetwork any
}

func (n *Node) GetState() any {
	s := State{
		ID: n.ID,
		RegionalNetwork: struct{ Plumtree any }{
			Plumtree: struct{ HyParViewState any }{
				HyParViewState: n.hv.GetState(),
			},
		},
	}
	return s
}

func (n *Node) StateHandler(w http.ResponseWriter, _ *http.Request) {
	state, err := json.Marshal(n.GetState())
	if err != nil {
		n.logger.Println(err)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(state)
}
