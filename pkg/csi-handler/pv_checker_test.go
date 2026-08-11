package csi_handler

import (
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	informerV1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/klog/v2/ktesting"
	_ "k8s.io/klog/v2/ktesting/init"

	"github.com/container-storage-interface/spec/lib/go/csi"
	healthmetrics "github.com/kubernetes-csi/external-health-monitor/pkg/metrics"
	"github.com/kubernetes-csi/external-health-monitor/pkg/mock"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type MockPVHealthConditionChecker struct {
	pvHealthConditionChecker *PVHealthConditionChecker
	pvcInformer              informerV1.PersistentVolumeClaimInformer
	pvInformer               informerV1.PersistentVolumeInformer
	fakeClient               *fake.Clientset
	csiControllerServer      *mock.FakeControllerServer
	eventRecorder            *record.FakeRecorder
}

func createMockPVHealthConditionChecker(t *testing.T) *MockPVHealthConditionChecker {
	k8sClient, informer := mock.FakeK8s()
	drv, csiConn := mock.StartFakeDriver(t)
	recorder := record.NewFakeRecorder(100)

	handler := NewCSIHealthClient(csiConn, 15*time.Second)
	return &MockPVHealthConditionChecker{
		pvHealthConditionChecker: &PVHealthConditionChecker{
			driverName:      mock.DriverName,
			timeout:         15 * time.Second,
			k8sClient:       k8sClient,
			pvcLister:       informer.Core().V1().PersistentVolumeClaims().Lister(),
			pvLister:        informer.Core().V1().PersistentVolumes().Lister(),
			csiHealthClient: handler,
			eventRecorder:   recorder,
			volumes:         map[string]volumeState{},
		},
		pvcInformer:         informer.Core().V1().PersistentVolumeClaims(),
		pvInformer:          informer.Core().V1().PersistentVolumes(),
		fakeClient:          k8sClient.(*fake.Clientset),
		csiControllerServer: drv.Controller,
		eventRecorder:       recorder,
	}
}

func drainEvents(rec *record.FakeRecorder) []string {
	var events []string
	for {
		select {
		case e := <-rec.Events:
			events = append(events, e)
		default:
			return events
		}
	}
}

// Seeds both the informer (for the lister) and the clientset tracker (for the status patch).
func (m *MockPVHealthConditionChecker) seedPVC(t *testing.T, pvc *v1.PersistentVolumeClaim) {
	t.Helper()
	if err := m.pvcInformer.Informer().GetStore().Add(pvc); err != nil {
		t.Fatal(err)
	}
	if err := m.fakeClient.Tracker().Add(pvc.DeepCopy()); err != nil {
		t.Fatal(err)
	}
}

func healthStatusPatched(actions []k8stesting.Action) (patched bool, abnormal bool) {
	for _, action := range actions {
		patchAction, ok := action.(k8stesting.PatchAction)
		if !ok {
			continue
		}
		if patchAction.GetResource().Resource != "persistentvolumeclaims" {
			continue
		}
		if patchAction.GetSubresource() != "status" {
			continue
		}
		patched = true
		body := string(patchAction.GetPatch())
		if strings.Contains(body, "\"healthConditions\"") {
			abnormal = true
		}
	}
	return patched, abnormal
}

func setPVCHealthStatus(pvc *v1.PersistentVolumeClaim, health *csi.VolumeHealth) {
	pvc.Status.HealthStatus = buildHealthStatus(volumeHealthToResult(health).Conditions)
}

// trackedUnhealthy reports whether the checker currently remembers the PVC's
// volume as unhealthy.
func trackedUnhealthy(checker *PVHealthConditionChecker, namespace, name string) bool {
	checker.volumeStateMu.Lock()
	defer checker.volumeStateMu.Unlock()
	return checker.volumes[namespace+"/"+name].unhealthy
}

func TestPVHealthConditionChecker_CheckControllerListVolumeHealth(t *testing.T) {
	assert := assert.New(t)
	tests := []struct {
		name         string
		pvc          *v1.PersistentVolumeClaim
		pv           *v1.PersistentVolume
		volumeId     string
		health       *csi.VolumeHealth
		wantErr      bool
		wantPatch    bool
		wantAbnormal bool
	}{
		{
			name:         "Abnormal volume gets healthStatus patched",
			pvc:          mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound),
			pv:           mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "1", "uid", &mock.BlockVolumeMode, v1.VolumeBound),
			volumeId:     "1",
			health:       mock.AbnormalVolumeHealth("1"),
			wantPatch:    true,
			wantAbnormal: true,
		},
		{
			name:      "Healthy volume not previously unhealthy: no patch (no-op suppression)",
			pvc:       mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound),
			pv:        mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "2", "uid", &mock.BlockVolumeMode, v1.VolumeBound),
			volumeId:  "2",
			health:    mock.HealthyVolumeHealth("2"),
			wantPatch: false,
		},
		{
			name:      "PV without CSI driver is skipped",
			pvc:       mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound),
			pv:        mock.CreatePVWithoutCSIDriver(2, "pvc", "pv", mock.DefaultNS, "1", "uid", v1.VolumeBound, &mock.BlockVolumeMode),
			volumeId:  "1",
			health:    mock.AbnormalVolumeHealth("1"),
			wantPatch: false,
		},
		{
			name:      "Bound PV without claimRef is skipped",
			pvc:       mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound),
			pv:        mock.CreatePV(2, "", "pv", mock.DefaultNS, "1", "uid", &mock.BlockVolumeMode, v1.VolumeBound),
			volumeId:  "1",
			health:    mock.AbnormalVolumeHealth("1"),
			wantPatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checker := createMockPVHealthConditionChecker(t)
			if err := checker.pvInformer.Informer().GetStore().Add(tt.pv); err != nil {
				t.Fatal(err)
			}
			checker.seedPVC(t, tt.pvc)

			out := &csi.ControllerListVolumeHealthResponse{
				Entries:   []*csi.VolumeHealth{tt.health},
				NextToken: "",
			}
			checker.csiControllerServer.SetListVolumeHealth(out, nil)

			_, ctx := ktesting.NewTestContext(t)
			if err := checker.pvHealthConditionChecker.CheckControllerListVolumeHealth(ctx); (err != nil) != tt.wantErr {
				t.Errorf("CheckControllerListVolumeHealth() error = %v, wantErr %v", err, tt.wantErr)
			}

			listReqs := checker.csiControllerServer.ListVolumeHealthRequests()
			assert.Len(listReqs, 1, "exactly one list RPC per cycle")
			assert.Equal("", listReqs[0].GetStartingToken())
			assert.Empty(checker.csiControllerServer.GetVolumeHealthRequests(), "no Get expected when the volume is present in the list")

			patched, abnormal := healthStatusPatched(checker.fakeClient.Actions())
			assert.Equal(tt.wantPatch, patched, "patch issued?")
			if tt.wantPatch {
				assert.Equal(tt.wantAbnormal, abnormal, "patch marks abnormal?")
			}
		})
	}
}

func Test_ListRecoveryUsesGetForMissingUnhealthyVolume(t *testing.T) {
	assert := assert.New(t)
	checker := createMockPVHealthConditionChecker(t)

	pv := mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "1", "uid", &mock.BlockVolumeMode, v1.VolumeBound)
	pvc := mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound)
	assert.Nil(checker.pvInformer.Informer().GetStore().Add(pv))
	checker.seedPVC(t, pvc)

	_, ctx := ktesting.NewTestContext(t)

	// Cycle 1: abnormal via list -> tracked unhealthy.
	abnormalOut := &csi.ControllerListVolumeHealthResponse{Entries: []*csi.VolumeHealth{mock.AbnormalVolumeHealth("1")}}
	checker.csiControllerServer.SetListVolumeHealth(abnormalOut, nil)
	assert.Nil(checker.pvHealthConditionChecker.CheckControllerListVolumeHealth(ctx))
	assert.True(trackedUnhealthy(checker.pvHealthConditionChecker, mock.DefaultNS, "pvc"))
	assert.Empty(checker.csiControllerServer.GetVolumeHealthRequests(), "no Get confirm needed while the volume is present in the list")

	// Cycle 2: absent from list, but a single Get confirms healthy -> cleared immediately.
	checker.fakeClient.ClearActions()
	emptyList := &csi.ControllerListVolumeHealthResponse{Entries: []*csi.VolumeHealth{}}
	checker.csiControllerServer.SetListVolumeHealth(emptyList, nil)
	getEmpty := &csi.ControllerGetVolumeHealthResponse{VolumeHealth: mock.HealthyVolumeHealth("1")}
	checker.csiControllerServer.SetGetVolumeHealth("1", getEmpty, nil)

	assert.Nil(checker.pvHealthConditionChecker.CheckControllerListVolumeHealth(ctx))

	getReqs := checker.csiControllerServer.GetVolumeHealthRequests()
	assert.Len(getReqs, 1, "exactly one Get confirmation for the absent volume")
	assert.Equal("1", getReqs[0].GetVolumeId())

	patched, abnormal := healthStatusPatched(checker.fakeClient.Actions())
	assert.True(patched, "should clear after a single Get confirmation")
	assert.False(abnormal, "clearing patch must not carry conditions")
	assert.False(trackedUnhealthy(checker.pvHealthConditionChecker, mock.DefaultNS, "pvc"), "no longer tracked unhealthy after Get confirm")
}

func Test_ListAbsentVolumeWithStalePVCHealthStatusUsesGetAfterRestart(t *testing.T) {
	assert := assert.New(t)
	checker := createMockPVHealthConditionChecker(t)

	pv := mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "1", "uid", &mock.BlockVolumeMode, v1.VolumeBound)
	pvc := mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound)
	setPVCHealthStatus(pvc, mock.AbnormalVolumeHealth("1"))
	assert.Nil(checker.pvInformer.Informer().GetStore().Add(pv))
	checker.seedPVC(t, pvc)

	_, ctx := ktesting.NewTestContext(t)
	emptyList := &csi.ControllerListVolumeHealthResponse{Entries: []*csi.VolumeHealth{}}
	checker.csiControllerServer.SetListVolumeHealth(emptyList, nil)
	getEmpty := &csi.ControllerGetVolumeHealthResponse{VolumeHealth: mock.HealthyVolumeHealth("1")}
	checker.csiControllerServer.SetGetVolumeHealth("1", getEmpty, nil)

	assert.Nil(checker.pvHealthConditionChecker.CheckControllerListVolumeHealth(ctx))

	assert.Len(checker.csiControllerServer.ListVolumeHealthRequests(), 1)
	getReqs := checker.csiControllerServer.GetVolumeHealthRequests()
	assert.Len(getReqs, 1, "stale PVC status must trigger exactly one Get confirmation")
	assert.Equal("1", getReqs[0].GetVolumeId())

	patched, abnormal := healthStatusPatched(checker.fakeClient.Actions())
	assert.True(patched, "stale healthStatus from before restart must be cleared")
	assert.False(abnormal, "clearing patch must not carry conditions")
	assert.False(trackedUnhealthy(checker.pvHealthConditionChecker, mock.DefaultNS, "pvc"), "healthy Get result must not leave volume tracked unhealthy")
}

func Test_FailedRPCIsNotRecovery(t *testing.T) {
	assert := assert.New(t)
	checker := createMockPVHealthConditionChecker(t)

	pv := mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "1", "uid", &mock.BlockVolumeMode, v1.VolumeBound)
	pvc := mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound)
	assert.Nil(checker.pvInformer.Informer().GetStore().Add(pv))
	checker.seedPVC(t, pvc)

	_, ctx := ktesting.NewTestContext(t)

	// First establish unhealthy state.
	abnormal := &csi.ControllerGetVolumeHealthResponse{VolumeHealth: mock.AbnormalVolumeHealth("1")}
	checker.csiControllerServer.SetGetVolumeHealth("1", abnormal, nil)
	assert.Nil(checker.pvHealthConditionChecker.CheckControllerVolumeHealth(ctx, pv))
	assert.True(trackedUnhealthy(checker.pvHealthConditionChecker, mock.DefaultNS, "pvc"))

	// Now the RPC fails. The volume must remain tracked unhealthy and no patch issued.
	checker.fakeClient.ClearActions()
	checker.csiControllerServer.SetGetVolumeHealth("1", nil, status.Error(codes.Unavailable, "driver down"))

	err := checker.pvHealthConditionChecker.CheckControllerVolumeHealth(ctx, pv)
	assert.Error(err, "failed RPC should surface an error")

	assert.Len(checker.csiControllerServer.GetVolumeHealthRequests(), 2, "one Get per cycle")

	patched, _ := healthStatusPatched(checker.fakeClient.Actions())
	assert.False(patched, "failed RPC must not issue a recovery patch")
	assert.True(trackedUnhealthy(checker.pvHealthConditionChecker, mock.DefaultNS, "pvc"), "failed RPC must not clear unhealthy state")
}

func Test_ConditionTransition(t *testing.T) {
	assert := assert.New(t)
	checker := createMockPVHealthConditionChecker(t)

	pv := mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "1", "uid", &mock.BlockVolumeMode, v1.VolumeBound)
	pvc := mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound)
	assert.Nil(checker.pvInformer.Informer().GetStore().Add(pv))
	checker.seedPVC(t, pvc)

	_, ctx := ktesting.NewTestContext(t)

	// Condition A: Inaccessible/VolumeNotFound.
	condA := &csi.ControllerGetVolumeHealthResponse{VolumeHealth: mock.AbnormalVolumeHealth("1")}
	checker.csiControllerServer.SetGetVolumeHealth("1", condA, nil)
	assert.Nil(checker.pvHealthConditionChecker.CheckControllerVolumeHealth(ctx, pv))

	// Condition B: Degraded/SlowIO (a different (status,reason)). Must patch again.
	checker.fakeClient.ClearActions()
	condB := &csi.ControllerGetVolumeHealthResponse{
		VolumeHealth: &csi.VolumeHealth{
			VolumeId: "1",
			HealthStatuses: []*csi.VolumeHealth_VolumeHealthEntry{
				{Status: csi.VolumeHealthErrorType_DEGRADED, Reason: "SlowIO", Message: "slow"},
			},
		},
	}
	checker.csiControllerServer.SetGetVolumeHealth("1", condB, nil)
	assert.Nil(checker.pvHealthConditionChecker.CheckControllerVolumeHealth(ctx, pv))

	patched, abnormal := healthStatusPatched(checker.fakeClient.Actions())
	assert.True(patched, "a condition transition A->B must issue a patch")
	assert.True(abnormal, "the transition patch must carry the new condition")
	assert.True(trackedUnhealthy(checker.pvHealthConditionChecker, mock.DefaultNS, "pvc"), "still unhealthy after transition")
}

func Test_GetClearsStalePVCHealthStatusAfterRestart(t *testing.T) {
	assert := assert.New(t)
	checker := createMockPVHealthConditionChecker(t)

	pv := mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "1", "uid", &mock.BlockVolumeMode, v1.VolumeBound)
	pvc := mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound)
	setPVCHealthStatus(pvc, mock.AbnormalVolumeHealth("1"))
	assert.Nil(checker.pvInformer.Informer().GetStore().Add(pv))
	checker.seedPVC(t, pvc)

	_, ctx := ktesting.NewTestContext(t)
	getEmpty := &csi.ControllerGetVolumeHealthResponse{VolumeHealth: mock.HealthyVolumeHealth("1")}
	checker.csiControllerServer.SetGetVolumeHealth("1", getEmpty, nil)

	assert.Nil(checker.pvHealthConditionChecker.CheckControllerVolumeHealth(ctx, pv))

	getReqs := checker.csiControllerServer.GetVolumeHealthRequests()
	assert.Len(getReqs, 1)
	assert.Equal("1", getReqs[0].GetVolumeId())

	patched, abnormal := healthStatusPatched(checker.fakeClient.Actions())
	assert.True(patched, "stale healthStatus from before restart must be cleared")
	assert.False(abnormal, "clearing patch must not carry conditions")
	assert.False(trackedUnhealthy(checker.pvHealthConditionChecker, mock.DefaultNS, "pvc"), "healthy Get result must not leave volume tracked unhealthy")
}

func TestPVHealthConditionChecker_GetVolumeHandle(t *testing.T) {
	tests := []struct {
		name    string
		pv      *v1.PersistentVolume
		wantErr bool
		want    string
	}{
		{
			name:    "Normal Case",
			pv:      mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "2", "uid", &mock.BlockVolumeMode, v1.VolumeBound),
			wantErr: false,
			want:    "2",
		},
		{
			name:    "PV without CSI driver Case",
			pv:      mock.CreatePVWithoutCSIDriver(2, "pvc", "pv", mock.DefaultNS, "1", "uid", v1.VolumeBound, &mock.BlockVolumeMode),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checker := createMockPVHealthConditionChecker(t)
			got, err := checker.pvHealthConditionChecker.GetVolumeHandle(tt.pv)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetVolumeHandle() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetVolumeHandle() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPVHealthConditionChecker_CheckControllerVolumeHealth(t *testing.T) {
	assert := assert.New(t)
	tests := []struct {
		name         string
		pv           *v1.PersistentVolume
		pvc          *v1.PersistentVolumeClaim
		volumeId     string
		health       *csi.VolumeHealth
		expectRPC    bool
		wantErr      bool
		wantPatch    bool
		wantAbnormal bool
	}{
		{
			name:         "Abnormal volume gets healthStatus patched",
			pvc:          mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound),
			pv:           mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "1", "uid", &mock.BlockVolumeMode, v1.VolumeBound),
			volumeId:     "1",
			health:       mock.AbnormalVolumeHealth("1"),
			expectRPC:    true,
			wantPatch:    true,
			wantAbnormal: true,
		},
		{
			name:      "Healthy volume: empty report, no patch",
			pvc:       mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound),
			pv:        mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "2", "uid", &mock.BlockVolumeMode, v1.VolumeBound),
			volumeId:  "2",
			health:    mock.HealthyVolumeHealth("2"),
			expectRPC: true,
			wantPatch: false,
		},
		{
			name:      "PV without CSI driver: error, no RPC",
			pvc:       mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound),
			pv:        mock.CreatePVWithoutCSIDriver(2, "pvc", "pv", mock.DefaultNS, "1", "uid", v1.VolumeBound, &mock.BlockVolumeMode),
			volumeId:  "1",
			expectRPC: false,
			wantErr:   true,
		},
		{
			name:      "PV not bound: error, no RPC",
			pvc:       mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound),
			pv:        mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "1", "uid", &mock.BlockVolumeMode, v1.VolumePending),
			volumeId:  "1",
			expectRPC: false,
			wantErr:   true,
		},
		{
			name:      "PV with empty VolumeHandle: error, no RPC",
			pvc:       mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound),
			pv:        mock.CreatePVWithNilVolumeHandle(2, "pvc", "pv", mock.DefaultNS, "1", "uid", v1.VolumeBound, &mock.BlockVolumeMode),
			volumeId:  "1",
			expectRPC: false,
			wantErr:   true,
		},
		{
			name:      "Bound PV without claimRef: error, no RPC",
			pvc:       mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound),
			pv:        mock.CreatePV(2, "", "pv", mock.DefaultNS, "1", "uid", &mock.BlockVolumeMode, v1.VolumeBound),
			volumeId:  "1",
			expectRPC: false,
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			checker := createMockPVHealthConditionChecker(t)
			if err := checker.pvInformer.Informer().GetStore().Add(tt.pv); err != nil {
				t.Fatal(err)
			}
			checker.seedPVC(t, tt.pvc)

			if tt.expectRPC {
				out := &csi.ControllerGetVolumeHealthResponse{VolumeHealth: tt.health}
				checker.csiControllerServer.SetGetVolumeHealth(tt.volumeId, out, nil)
			}

			_, ctx := ktesting.NewTestContext(t)
			if err := checker.pvHealthConditionChecker.CheckControllerVolumeHealth(ctx, tt.pv); (err != nil) != tt.wantErr {
				t.Errorf("CheckControllerVolumeHealth() error = %v, wantErr %v", err, tt.wantErr)
			}

			getReqs := checker.csiControllerServer.GetVolumeHealthRequests()
			if tt.expectRPC {
				assert.Len(getReqs, 1, "exactly one Get RPC")
				assert.Equal(tt.volumeId, getReqs[0].GetVolumeId())
			} else {
				assert.Empty(getReqs, "no RPC expected")
			}

			patched, abnormal := healthStatusPatched(checker.fakeClient.Actions())
			assert.Equal(tt.wantPatch, patched, "patch issued?")
			if tt.wantPatch {
				assert.Equal(tt.wantAbnormal, abnormal, "patch marks abnormal?")
			}
		})
	}
}

func Test_DroppedHealthStatusWritesRetryAndRecoverImmediately(t *testing.T) {
	assert := assert.New(t)
	checker := createMockPVHealthConditionChecker(t)

	m := healthmetrics.New()
	reg := k8smetrics.NewKubeRegistry()
	m.Register(reg)
	checker.pvHealthConditionChecker.metrics = m

	pv := mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "1", "uid", &mock.BlockVolumeMode, v1.VolumeBound)
	pvc := mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound)
	assert.Nil(checker.pvInformer.Informer().GetStore().Add(pv))
	checker.seedPVC(t, pvc)

	// While dropField is true the "API server" accepts status patches but
	// returns the PVC without healthStatus, like a disabled CSIVolumeHealth gate.
	dropField := true
	checker.fakeClient.PrependReactor("patch", "persistentvolumeclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if !dropField {
			return false, nil, nil
		}
		return true, pvc.DeepCopy(), nil
	})

	_, ctx := ktesting.NewTestContext(t)
	abnormal := &csi.ControllerGetVolumeHealthResponse{VolumeHealth: mock.AbnormalVolumeHealth("1")}
	checker.csiControllerServer.SetGetVolumeHealth("1", abnormal, nil)

	// Cycle 1: the write is attempted and comes back without the field.
	// It must not be recorded as applied.
	assert.Nil(checker.pvHealthConditionChecker.CheckControllerVolumeHealth(ctx, pv))
	patched, _ := healthStatusPatched(checker.fakeClient.Actions())
	assert.True(patched, "first cycle must attempt the write")
	assert.False(trackedUnhealthy(checker.pvHealthConditionChecker, mock.DefaultNS, "pvc"), "a dropped write must not be recorded as applied")
	assert.True(checker.pvHealthConditionChecker.fieldDropped.Load(), "dropped state must be tracked for logging")
	assert.Equal(1, gaugeSeriesCount(t, reg), "the gauge must reflect the driver's report even while writes are dropped")

	// Cycle 2: still dropped. The write is simply retried at normal cadence.
	checker.fakeClient.ClearActions()
	assert.Nil(checker.pvHealthConditionChecker.CheckControllerVolumeHealth(ctx, pv))
	patched, _ = healthStatusPatched(checker.fakeClient.Actions())
	assert.True(patched, "dropped writes keep being retried")
	assert.False(trackedUnhealthy(checker.pvHealthConditionChecker, mock.DefaultNS, "pvc"), "still not recorded while dropped")
	assert.Len(checker.csiControllerServer.GetVolumeHealthRequests(), 2, "probing continues throughout")

	// Cycle 3: the gate is enabled. The very next write persists and is
	// recorded; recovery is immediate.
	dropField = false
	checker.fakeClient.ClearActions()
	assert.Nil(checker.pvHealthConditionChecker.CheckControllerVolumeHealth(ctx, pv))
	patched, abnormalPatch := healthStatusPatched(checker.fakeClient.Actions())
	assert.True(patched, "write must go out as soon as the gate is enabled")
	assert.True(abnormalPatch, "the write must carry the conditions")
	assert.True(trackedUnhealthy(checker.pvHealthConditionChecker, mock.DefaultNS, "pvc"), "persisted write must be recorded")
	assert.False(checker.pvHealthConditionChecker.fieldDropped.Load(), "recovery must clear the dropped state")
}

func Test_ForgetPVCDropsCheckerState(t *testing.T) {
	assert := assert.New(t)
	checker := createMockPVHealthConditionChecker(t)

	m := healthmetrics.New()
	reg := k8smetrics.NewKubeRegistry()
	m.Register(reg)
	checker.pvHealthConditionChecker.metrics = m

	pv := mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "1", "uid", &mock.BlockVolumeMode, v1.VolumeBound)
	pvc := mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound)
	assert.Nil(checker.pvInformer.Informer().GetStore().Add(pv))
	checker.seedPVC(t, pvc)

	_, ctx := ktesting.NewTestContext(t)
	abnormal := &csi.ControllerGetVolumeHealthResponse{VolumeHealth: mock.AbnormalVolumeHealth("1")}
	checker.csiControllerServer.SetGetVolumeHealth("1", abnormal, nil)
	assert.Nil(checker.pvHealthConditionChecker.CheckControllerVolumeHealth(ctx, pv))

	assert.True(trackedUnhealthy(checker.pvHealthConditionChecker, mock.DefaultNS, "pvc"))
	assert.Len(checker.pvHealthConditionChecker.volumes, 1)
	assert.Equal(1, gaugeSeriesCount(t, reg))

	checker.pvHealthConditionChecker.ForgetPVC(mock.DefaultNS, "pvc")

	assert.Empty(checker.pvHealthConditionChecker.volumes, "tracked volume state must be dropped")
	assert.Equal(0, gaugeSeriesCount(t, reg), "the PVC's metric series must be removed")
}

func gaugeSeriesCount(t *testing.T, reg k8smetrics.KubeRegistry) int {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() == healthmetrics.ControllerVolumeHealthStatusName {
			return len(mf.GetMetric())
		}
	}
	return 0
}

func Test_UnknownConditionsSurfacedAsEventNotStatus(t *testing.T) {
	assert := assert.New(t)
	checker := createMockPVHealthConditionChecker(t)

	m := healthmetrics.New()
	reg := k8smetrics.NewKubeRegistry()
	m.Register(reg)
	checker.pvHealthConditionChecker.metrics = m

	pv := mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "1", "uid", &mock.BlockVolumeMode, v1.VolumeBound)
	pvc := mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound)
	assert.Nil(checker.pvInformer.Informer().GetStore().Add(pv))
	checker.seedPVC(t, pvc)

	_, ctx := ktesting.NewTestContext(t)

	// Cycle 1: one recognized and one unrecognized entry.
	mixed := &csi.ControllerGetVolumeHealthResponse{
		VolumeHealth: &csi.VolumeHealth{
			VolumeId: "1",
			HealthStatuses: []*csi.VolumeHealth_VolumeHealthEntry{
				{Status: csi.VolumeHealthErrorType_DEGRADED, Reason: "SlowIO", Message: "slow"},
				{Status: csi.VolumeHealthErrorType(99), Reason: "FromTheFuture", Message: "flux capacitor"},
			},
		},
	}
	checker.csiControllerServer.SetGetVolumeHealth("1", mixed, nil)
	assert.Nil(checker.pvHealthConditionChecker.CheckControllerVolumeHealth(ctx, pv))

	// The status patch carries only the recognized condition.
	patched, abnormal := healthStatusPatched(checker.fakeClient.Actions())
	assert.True(patched, "recognized condition must still be patched")
	assert.True(abnormal)
	for _, action := range checker.fakeClient.Actions() {
		if patchAction, ok := action.(k8stesting.PatchAction); ok {
			assert.NotContains(string(patchAction.GetPatch()), "FromTheFuture", "unknown entries must never reach the status field")
		}
	}

	events := drainEvents(checker.eventRecorder)
	if assert.Len(events, 1, "one warning event per cycle with unknown entries") {
		assert.Contains(events[0], v1.EventTypeWarning)
		assert.Contains(events[0], "UnknownVolumeHealthCondition")
		assert.Contains(events[0], "99 (reason: FromTheFuture): flux capacitor")
	}

	// Cycle 2: only the unknown entry remains. The recognized condition
	// clears (unknown entries never block recovery), the event repeats.
	checker.fakeClient.ClearActions()
	unknownOnly := &csi.ControllerGetVolumeHealthResponse{
		VolumeHealth: &csi.VolumeHealth{
			VolumeId: "1",
			HealthStatuses: []*csi.VolumeHealth_VolumeHealthEntry{
				{Status: csi.VolumeHealthErrorType(99), Reason: "FromTheFuture", Message: "flux capacitor"},
			},
		},
	}
	checker.csiControllerServer.SetGetVolumeHealth("1", unknownOnly, nil)
	assert.Nil(checker.pvHealthConditionChecker.CheckControllerVolumeHealth(ctx, pv))

	patched, abnormal = healthStatusPatched(checker.fakeClient.Actions())
	assert.True(patched, "recognized condition must clear when only unknown entries remain")
	assert.False(abnormal, "the clearing patch must not carry conditions")
	assert.False(trackedUnhealthy(checker.pvHealthConditionChecker, mock.DefaultNS, "pvc"))
	assert.Len(drainEvents(checker.eventRecorder), 1, "the event repeats while the driver keeps reporting the entry")

	// Both observations were counted.
	if got := mustCounterValue(t, reg, healthmetrics.UnknownConditionTotalName, "99"); got != 2 {
		t.Errorf("unknown condition counter = %v, want 2", got)
	}
}

func mustCounterValue(t *testing.T, reg k8smetrics.KubeRegistry, name, status string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "status" && l.GetValue() == status {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

func Test_NoEventsWithoutUnknownConditions(t *testing.T) {
	assert := assert.New(t)
	checker := createMockPVHealthConditionChecker(t)

	pv := mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "1", "uid", &mock.BlockVolumeMode, v1.VolumeBound)
	pvc := mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound)
	assert.Nil(checker.pvInformer.Informer().GetStore().Add(pv))
	checker.seedPVC(t, pvc)

	_, ctx := ktesting.NewTestContext(t)
	abnormal := &csi.ControllerGetVolumeHealthResponse{VolumeHealth: mock.AbnormalVolumeHealth("1")}
	checker.csiControllerServer.SetGetVolumeHealth("1", abnormal, nil)
	assert.Nil(checker.pvHealthConditionChecker.CheckControllerVolumeHealth(ctx, pv))

	assert.Empty(drainEvents(checker.eventRecorder), "recognized conditions are reported via status, not events")
}

// A stale PV can reference a PVC that was deleted and recreated under the
// same name; that PVC must not receive this volume's health.
func Test_MismatchedPVCIsNotPatched(t *testing.T) {
	for _, tc := range []struct {
		name string
		pvc  *v1.PersistentVolumeClaim
	}{
		{
			name: "recreated claim with a new UID",
			pvc:  mock.CreatePVC(1, 2, "pvc", "uid-new", mock.DefaultNS, "pv", v1.ClaimBound),
		},
		{
			name: "claim bound to a different PV",
			pvc:  mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "other-pv", v1.ClaimBound),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert := assert.New(t)
			checker := createMockPVHealthConditionChecker(t)

			pv := mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "1", "uid", &mock.BlockVolumeMode, v1.VolumeBound)
			assert.Nil(checker.pvInformer.Informer().GetStore().Add(pv))
			checker.seedPVC(t, tc.pvc)

			_, ctx := ktesting.NewTestContext(t)
			abnormal := &csi.ControllerGetVolumeHealthResponse{VolumeHealth: mock.AbnormalVolumeHealth("1")}
			checker.csiControllerServer.SetGetVolumeHealth("1", abnormal, nil)

			assert.Error(checker.pvHealthConditionChecker.CheckControllerVolumeHealth(ctx, pv))

			patched, _ := healthStatusPatched(checker.fakeClient.Actions())
			assert.False(patched, "a mismatched PVC must not be patched")
		})
	}
}

func Test_PVDeletionClearsStalePVCHealthStatus(t *testing.T) {
	assert := assert.New(t)
	checker := createMockPVHealthConditionChecker(t)

	pv := mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "1", "uid", &mock.BlockVolumeMode, v1.VolumeBound)
	pvc := mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound)
	setPVCHealthStatus(pvc, mock.AbnormalVolumeHealth("1"))
	assert.Nil(checker.pvInformer.Informer().GetStore().Add(pv))
	checker.seedPVC(t, pvc)

	_, ctx := ktesting.NewTestContext(t)
	checker.pvHealthConditionChecker.ClearForDeletedPV(ctx, pv)

	patched, abnormal := healthStatusPatched(checker.fakeClient.Actions())
	assert.True(patched, "the stale health status must be cleared")
	assert.False(abnormal, "the clearing patch must not carry conditions")
	assert.Empty(checker.pvHealthConditionChecker.volumes)
}

// A PVC recreated under the same name belongs to another volume and must be
// left alone when the old PV goes away.
func Test_PVDeletionDoesNotClearRecreatedPVC(t *testing.T) {
	assert := assert.New(t)
	checker := createMockPVHealthConditionChecker(t)

	pv := mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "1", "uid-old", &mock.BlockVolumeMode, v1.VolumeBound)
	pvc := mock.CreatePVC(1, 2, "pvc", "uid-new", mock.DefaultNS, "other-pv", v1.ClaimBound)
	setPVCHealthStatus(pvc, mock.AbnormalVolumeHealth("2"))
	assert.Nil(checker.pvInformer.Informer().GetStore().Add(pv))
	checker.seedPVC(t, pvc)

	_, ctx := ktesting.NewTestContext(t)
	checker.pvHealthConditionChecker.ClearForDeletedPV(ctx, pv)

	patched, _ := healthStatusPatched(checker.fakeClient.Actions())
	assert.False(patched, "a recreated PVC must not be touched")
	assert.Empty(checker.pvHealthConditionChecker.volumes, "the old claim's state must still be dropped")
}

// Matches Get mode, which also stops monitoring a PV once it is deleting.
func Test_ListSkipsDeletingPV(t *testing.T) {
	assert := assert.New(t)
	checker := createMockPVHealthConditionChecker(t)

	pv := mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "1", "uid", &mock.BlockVolumeMode, v1.VolumeBound)
	now := metav1.Now()
	pv.DeletionTimestamp = &now
	pvc := mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound)
	assert.Nil(checker.pvInformer.Informer().GetStore().Add(pv))
	checker.seedPVC(t, pvc)

	_, ctx := ktesting.NewTestContext(t)
	abnormalOut := &csi.ControllerListVolumeHealthResponse{Entries: []*csi.VolumeHealth{mock.AbnormalVolumeHealth("1")}}
	checker.csiControllerServer.SetListVolumeHealth(abnormalOut, nil)
	assert.Nil(checker.pvHealthConditionChecker.CheckControllerListVolumeHealth(ctx))

	patched, _ := healthStatusPatched(checker.fakeClient.Actions())
	assert.False(patched, "a deleting PV must not be reconciled")
}
