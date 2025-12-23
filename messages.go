package main

import "encoding/json"

const MAX_AGG_MSG_TYPE int8 = 1
const AGG_MSG_TYPE int8 = 2
const AGG_RESULT_MSG_TYPE int8 = 3

type Msg interface {
	Type() int8
}

type MaxAgg struct {
	Value       float64
	Source      string
	HopDistance int
	SeqNo       int
}

func (m MaxAgg) Type() int8 {
	return MAX_AGG_MSG_TYPE
}

type Agg struct {
	Aggregate []IntermediateMetric
}

func (m Agg) Type() int8 {
	return AGG_MSG_TYPE
}

type AggResult struct {
	Time      int64
	Aggregate []IntermediateMetric
}

func (m AggResult) Type() int8 {
	return AGG_RESULT_MSG_TYPE
}

func MsgToBytes(msg Msg) []byte {
	msgBytes, _ := json.Marshal(&msg)
	return append([]byte{byte(msg.Type())}, msgBytes...)
}

func BytesToMsg(msgBytes []byte) Msg {
	msgType := int8(msgBytes[0])
	var msg Msg
	switch msgType {
	case MAX_AGG_MSG_TYPE:
		msg = &MaxAgg{}
	case AGG_MSG_TYPE:
		msg = &Agg{}
	case AGG_RESULT_MSG_TYPE:
		msg = &AggResult{}
	}
	if msg == nil {
		return nil
	}
	json.Unmarshal(msgBytes[1:], msg)
	return msg
}
