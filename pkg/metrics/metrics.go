package metrics

import (
	k8smetrics "k8s.io/component-base/metrics"
)

const (
	ProbeDurationName                = "csi_volume_health_probe_duration_seconds"
	ProbeTotalName                   = "csi_volume_health_probe_total"
	ControllerVolumeHealthStatusName = "csi_controller_volume_health_status"
	StatusWritesDroppedName          = "csi_volume_health_status_writes_dropped_total"
	UnknownConditionTotalName        = "csi_volume_health_unknown_condition_total"
)

const (
	MethodList = "ControllerListVolumeHealth"
	MethodGet  = "ControllerGetVolumeHealth"
)

const (
	ResultSuccess = "success"
	ResultError   = "error"
)

const (
	labelMethod    = "method"
	labelResult    = "result"
	labelNamespace = "namespace"
	labelPVC       = "persistentvolumeclaim"
	labelStatus    = "status"
)

type Metrics struct {
	probeDuration         *k8smetrics.HistogramVec
	probeTotal            *k8smetrics.CounterVec
	volumeHealthVec       *k8smetrics.GaugeVec
	statusWritesDropped   *k8smetrics.Counter
	unknownConditionTotal *k8smetrics.CounterVec
}

func New() *Metrics {
	return &Metrics{
		probeDuration: k8smetrics.NewHistogramVec(
			&k8smetrics.HistogramOpts{
				Name:           ProbeDurationName,
				Help:           "Histogram of controller-side volume health probe RPC latency in seconds, broken down by CSI method.",
				Buckets:        []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 15, 30},
				StabilityLevel: k8smetrics.ALPHA,
			},
			[]string{labelMethod},
		),
		probeTotal: k8smetrics.NewCounterVec(
			&k8smetrics.CounterOpts{
				Name:           ProbeTotalName,
				Help:           "Cumulative count of controller-side volume health probes, broken down by CSI method and result.",
				StabilityLevel: k8smetrics.ALPHA,
			},
			[]string{labelMethod, labelResult},
		),
		volumeHealthVec: k8smetrics.NewGaugeVec(
			&k8smetrics.GaugeOpts{
				Name:           ControllerVolumeHealthStatusName,
				Help:           "Controller-reported health for every unhealthy volume. Value is 1 while a condition with the given status is present on the PVC.",
				StabilityLevel: k8smetrics.ALPHA,
			},
			[]string{labelNamespace, labelPVC, labelStatus},
		),
		statusWritesDropped: k8smetrics.NewCounter(
			&k8smetrics.CounterOpts{
				Name:           StatusWritesDroppedName,
				Help:           "Cumulative count of PVC status patches whose healthStatus was dropped by the API server, which indicates the CSIVolumeHealth feature gate is disabled.",
				StabilityLevel: k8smetrics.ALPHA,
			},
		),
		unknownConditionTotal: k8smetrics.NewCounterVec(
			&k8smetrics.CounterOpts{
				Name:           UnknownConditionTotalName,
				Help:           "Cumulative count of volume health entries observed with a CSI error type unknown to this monitor, broken down by the raw CSI status value.",
				StabilityLevel: k8smetrics.ALPHA,
			},
			[]string{labelStatus},
		),
	}
}

func (m *Metrics) Register(registry k8smetrics.KubeRegistry) {
	registry.MustRegister(m.probeDuration)
	registry.MustRegister(m.probeTotal)
	registry.MustRegister(m.volumeHealthVec)
	registry.MustRegister(m.statusWritesDropped)
	registry.MustRegister(m.unknownConditionTotal)
}

func (m *Metrics) RecordDroppedStatusWrite() {
	m.statusWritesDropped.Inc()
}

// Cardinality is bounded by the CSI enum values drivers report.
func (m *Metrics) RecordUnknownCondition(csiStatus string) {
	m.unknownConditionTotal.WithLabelValues(csiStatus).Inc()
}

func (m *Metrics) ObserveProbe(method string, durationSeconds float64, err error) {
	result := ResultSuccess
	if err != nil {
		result = ResultError
	}
	m.probeDuration.WithLabelValues(method).Observe(durationSeconds)
	m.probeTotal.WithLabelValues(method, result).Inc()
}

// Replaces all of the PVC's series with exactly the given statuses; an empty set clears it.
func (m *Metrics) SetVolumeHealth(namespace, pvc string, statuses []string) {
	m.deletePVCSeries(namespace, pvc)
	for _, s := range statuses {
		m.volumeHealthVec.WithLabelValues(namespace, pvc, s).Set(1)
	}
}

func (m *Metrics) ClearVolumeHealth(namespace, pvc string) {
	m.deletePVCSeries(namespace, pvc)
}

func (m *Metrics) deletePVCSeries(namespace, pvc string) {
	if !m.volumeHealthVec.IsCreated() {
		return
	}
	m.volumeHealthVec.DeletePartialMatch(map[string]string{
		labelNamespace: namespace,
		labelPVC:       pvc,
	})
}
