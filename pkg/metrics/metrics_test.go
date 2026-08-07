package metrics

import (
	"errors"
	"strings"
	"testing"

	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/testutil"
)

func newRegistered(t *testing.T) (*Metrics, k8smetrics.KubeRegistry) {
	t.Helper()
	m := New()
	reg := k8smetrics.NewKubeRegistry()
	m.Register(reg)
	return m, reg
}

func TestObserveProbe_TotalCountsByResult(t *testing.T) {
	m, reg := newRegistered(t)

	m.ObserveProbe(MethodList, 0.5, nil)
	m.ObserveProbe(MethodList, 0.7, nil)
	m.ObserveProbe(MethodGet, 0.2, errors.New("boom"))

	expected := `
# HELP csi_volume_health_probe_total [ALPHA] Cumulative count of controller-side volume health probes, broken down by CSI method and result.
# TYPE csi_volume_health_probe_total counter
csi_volume_health_probe_total{method="ControllerListVolumeHealth",result="success"} 2
csi_volume_health_probe_total{method="ControllerGetVolumeHealth",result="error"} 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(expected), ProbeTotalName); err != nil {
		t.Errorf("probe_total mismatch: %v", err)
	}
}

func TestObserveProbe_DurationObserved(t *testing.T) {
	m, reg := newRegistered(t)

	m.ObserveProbe(MethodList, 0.5, nil)

	// The histogram for the list method must have observed exactly 1 sample.
	if got := histogramSampleCount(t, reg, ProbeDurationName); got != 1 {
		t.Errorf("expected duration histogram count 1, got %d", got)
	}
}

func TestVolumeHealth_SetAndClear(t *testing.T) {
	m, reg := newRegistered(t)

	// Repeated statuses, which may come from different reasons, share one series.
	m.SetVolumeHealth("ns", "pvc1", []string{"Inaccessible", "Degraded", "Degraded"})
	if got := mustCount(t, reg, ControllerVolumeHealthStatusName); got != 2 {
		t.Errorf("after Set: expected 2 gauge series, got %d", got)
	}

	expected := `
# HELP csi_controller_volume_health_status [ALPHA] Controller-reported health for every unhealthy volume. Value is 1 while a condition with the given status is present on the PVC.
# TYPE csi_controller_volume_health_status gauge
csi_controller_volume_health_status{namespace="ns",persistentvolumeclaim="pvc1",status="Degraded"} 1
csi_controller_volume_health_status{namespace="ns",persistentvolumeclaim="pvc1",status="Inaccessible"} 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(expected), ControllerVolumeHealthStatusName); err != nil {
		t.Errorf("gauge mismatch: %v", err)
	}

	// Reducing to a single status must drop the stale series.
	m.SetVolumeHealth("ns", "pvc1", []string{"Degraded"})
	if got := mustCount(t, reg, ControllerVolumeHealthStatusName); got != 1 {
		t.Errorf("after reduce: expected 1 gauge series, got %d", got)
	}

	// Clearing (recovery) must remove all series for the PVC.
	m.ClearVolumeHealth("ns", "pvc1")
	if got := mustCount(t, reg, ControllerVolumeHealthStatusName); got != 0 {
		t.Errorf("after clear: expected 0 gauge series, got %d", got)
	}
}

func TestVolumeHealth_ClearIsScopedToPVC(t *testing.T) {
	m, reg := newRegistered(t)

	m.SetVolumeHealth("ns", "pvc1", []string{"Degraded"})
	m.SetVolumeHealth("ns", "pvc2", []string{"Inaccessible"})
	if got := mustCount(t, reg, ControllerVolumeHealthStatusName); got != 2 {
		t.Fatalf("setup: expected 2 series, got %d", got)
	}

	// Clearing pvc1 must leave pvc2's series intact.
	m.ClearVolumeHealth("ns", "pvc1")
	if got := mustCount(t, reg, ControllerVolumeHealthStatusName); got != 1 {
		t.Errorf("expected 1 series after clearing pvc1, got %d", got)
	}
}

func TestDeletePVCSeries_UnregisteredNoPanic(t *testing.T) {
	m := New() // not registered
	m.ClearVolumeHealth("ns", "pvc1")
	m.SetVolumeHealth("ns", "pvc1", []string{"Degraded"})
}

func mustCount(t *testing.T, reg k8smetrics.KubeRegistry, name string) int {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather %s: %v", name, err)
	}
	for _, mf := range mfs {
		if mf.GetName() == name {
			return len(mf.GetMetric())
		}
	}
	return 0
}

func histogramSampleCount(t *testing.T, reg k8smetrics.KubeRegistry, name string) uint64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather %s: %v", name, err)
	}
	var total uint64
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			total += m.GetHistogram().GetSampleCount()
		}
	}
	return total
}

func TestRecordDroppedStatusWrite(t *testing.T) {
	m, reg := newRegistered(t)

	m.RecordDroppedStatusWrite()
	m.RecordDroppedStatusWrite()

	expected := `
# HELP csi_volume_health_status_writes_dropped_total [ALPHA] Cumulative count of PVC status patches whose healthStatus was dropped by the API server, which indicates the CSIVolumeHealth feature gate is disabled.
# TYPE csi_volume_health_status_writes_dropped_total counter
csi_volume_health_status_writes_dropped_total 2
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(expected), StatusWritesDroppedName); err != nil {
		t.Errorf("dropped writes counter mismatch: %v", err)
	}
}

func TestRecordUnknownCondition(t *testing.T) {
	m, reg := newRegistered(t)

	m.RecordUnknownCondition("MULTIPATH_LOSS")
	m.RecordUnknownCondition("MULTIPATH_LOSS")
	m.RecordUnknownCondition("99")

	expected := `
# HELP csi_volume_health_unknown_condition_total [ALPHA] Cumulative count of volume health entries observed with a CSI error type unknown to this monitor, broken down by the raw CSI status value.
# TYPE csi_volume_health_unknown_condition_total counter
csi_volume_health_unknown_condition_total{status="99"} 1
csi_volume_health_unknown_condition_total{status="MULTIPATH_LOSS"} 2
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(expected), UnknownConditionTotalName); err != nil {
		t.Errorf("unknown condition counter mismatch: %v", err)
	}
}
