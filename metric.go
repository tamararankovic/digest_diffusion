package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"maps"
	"net/http"
	"os"
	"slices"
	"strings"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

type MetricMetadata struct {
	Name   string
	Labels map[string]string
}

type ScrapeTarget struct {
	Name    string
	Address string
}

var metrics = `# HELP app_request_processing_time_seconds Average request processing time
# TYPE app_request_processing_time_seconds gauge
app_request_processing_time_seconds 0.256

# HELP app_memory_usage_bytes Current memory usage in bytes
# TYPE app_memory_usage_bytes gauge
app_memory_usage_bytes %s

# HELP app_cpu_load_ratio CPU load (0-1)
# TYPE app_cpu_load_ratio gauge
app_cpu_load_ratio 0.13

# HELP app_active_sessions Current active user sessions
# TYPE app_active_sessions gauge
app_active_sessions 42

# HELP app_queue_depth_pending_jobs Jobs waiting in queue
# TYPE app_queue_depth_pending_jobs gauge
app_queue_depth_pending_jobs 7

# HELP app_cache_hit_ratio Cache hit ratio
# TYPE app_cache_hit_ratio gauge
app_cache_hit_ratio 0.82

# HELP app_current_goroutines Goroutine count
# TYPE app_current_goroutines gauge
app_current_goroutines 33

# HELP app_last_backup_timestamp_seconds Unix timestamp of last successful backup
# TYPE app_last_backup_timestamp_seconds gauge
app_last_backup_timestamp_seconds 1.700000e+09

# HELP app_http_requests_total Total HTTP requests processed
# TYPE app_http_requests_total counter
app_http_requests_total 12890

# HELP app_errors_total Total errors encountered
# TYPE app_errors_total counter
app_errors_total 17

# EOF
`

var initVal string = os.Getenv("ID")
var wasSet bool = false

func (m *Node) fetchNodeMetrics() []*dto.MetricFamily {
	result := make([]*dto.MetricFamily, 0)
	tmpMetrics := metrics
	if !wasSet {
		tmpMetrics = fmt.Sprintf(metrics, initVal)
	}
	mfs, err := parseOpenMetrics(tmpMetrics)
	if err != nil {
		log.Println(err)
		return result
	}
	sourceLabelName := "source"
	sourceLabelValue := "tmp"
	levelLabelName := "level"
	levelLabelValue := "node"
	regionLabelName := "regionID"
	regionLabelValue := "r1"
	for _, mf := range mfs {
		for _, m := range mf.Metric {
			m.Label = append(m.Label, &dto.LabelPair{Name: &sourceLabelName, Value: &sourceLabelValue})
			m.Label = append(m.Label, &dto.LabelPair{Name: &levelLabelName, Value: &levelLabelValue})
			m.Label = append(m.Label, &dto.LabelPair{Name: &regionLabelName, Value: &regionLabelValue})
		}
		result = append(result, mf)
	}
	om, err := toOpenMetrics(result)
	if err != nil {
		log.Println(err)
	} else {
		m.latestMetrics["node"] = om
	}
	return result
}

func parseOpenMetrics(data string) (map[string]*dto.MetricFamily, error) {
	parser := expfmt.TextParser{}
	mf, err := parser.TextToMetricFamilies(strings.NewReader(data))
	if err != nil {
		return nil, err
	}
	return mf, nil
}

func toOpenMetrics(mfs []*dto.MetricFamily) (string, error) {
	var buf bytes.Buffer
	encoder := expfmt.NewEncoder(&buf, expfmt.NewFormat(expfmt.TypeOpenMetrics))
	for _, mf := range mfs {
		if err := encoder.Encode(mf); err != nil {
			return "", fmt.Errorf("failed to encode %s: %w", mf.GetName(), err)
		}
	}
	return buf.String(), nil
}

func filterByTypes(input []*dto.MetricFamily, types []*dto.MetricType) []*dto.MetricFamily {
	filtered := make([]*dto.MetricFamily, 0)
	for _, mf := range input {
		if !slices.ContainsFunc(types, func(t *dto.MetricType) bool {
			return t.String() == mf.Type.String()
		}) {
			continue
		}
		filtered = append(filtered, mf)
	}
	return filtered
}

func selectRawMetricsValues(selector MetricMetadata, metrics []*dto.MetricFamily) []float64 {
	selected := make([]float64, 0)
	for _, mf := range metrics {
		if mf.Name == nil {
			continue
		}
		if *mf.Name != selector.Name {
			continue
		}
		if len(mf.Metric) == 0 {
			continue
		}
		metric := mf.Metric[0]
		labelMap := make(map[string]string)
		for _, label := range metric.Label {
			if label.Name == nil || label.Value == nil {
				continue
			}
			labelMap[*label.Name] = *label.Value
		}
		labelsMatch := true
		for name, value := range selector.Labels {
			if v, ok := labelMap[name]; !ok || v != value {
				labelsMatch = false
				break
			}
		}
		if !labelsMatch || metric.Gauge.Value == nil {
			continue
		}
		selected = append(selected, *metric.Gauge.Value)
	}
	return selected
}

func selectIMValues(selector MetricMetadata, metrics []IntermediateMetric) []IntermediateMetric {
	selected := make([]IntermediateMetric, 0)
	for _, metric := range metrics {
		if metric.Metadata.Name != selector.Name {
			continue
		}
		labelsMatch := true
		for name, value := range selector.Labels {
			if v, ok := metric.Metadata.Labels[name]; !ok || v != value {
				labelsMatch = false
				break
			}
		}
		if !labelsMatch {
			continue
		}
		selected = append(selected, metric)
	}
	return selected
}

func rawMetricsToIM(input []float64, rule AggregationRule) []IntermediateMetric {
	imInput := make([]IntermediateMetric, 0)
	makeIRFunc := MakeIR[rule.Func]
	for _, value := range input {
		im := IntermediateMetric{
			Metadata: MetricMetadata{
				Name:   rule.Output.Name,
				Labels: maps.Clone(rule.Output.Labels),
			},
			Result: makeIRFunc(value),
		}
		imInput = append(imInput, im)
	}
	return imInput
}

func imToOpenMetrics(ims []IntermediateMetric) (string, error) {
	mfs := make([]*dto.MetricFamily, 0)
	for _, im := range ims {
		value := im.Result.ComputeFinal()
		labels := make([]*dto.LabelPair, 0)
		for name, value := range im.Metadata.Labels {
			labels = append(labels, &dto.LabelPair{
				Name:  &name,
				Value: &value,
			})
		}
		mf := &dto.MetricFamily{
			Name: &im.Metadata.Name,
			Type: dto.MetricType_GAUGE.Enum(),
			Metric: []*dto.Metric{
				{
					Label: labels,
					Gauge: &dto.Gauge{
						Value: &value,
					},
				},
			},
		}
		mfs = append(mfs, mf)
	}
	return toOpenMetrics(mfs)
}

func (n *Node) setMetricsHandler(w http.ResponseWriter, r *http.Request) {
	newMetrics, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()
	n.lock.Lock()
	wasSet = true
	metrics = string(newMetrics)
	log.Println("metrics set")
	log.Println(metrics)
	n.lock.Unlock()
	w.WriteHeader(http.StatusOK)
}
