package main

type MaxAgg struct {
	Value       float64
	Source      string
	HopDistance int
	TTL         int
}

type Agg struct {
	Aggregate []IntermediateMetric
}

type AggResult struct {
	Time      int64
	Aggregate []IntermediateMetric
}
