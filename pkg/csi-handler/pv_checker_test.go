package csi_handler

import (
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	informerV1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/klog/v2/ktesting"
	_ "k8s.io/klog/v2/ktesting/init"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/csi-test/v5/driver"
	"github.com/kubernetes-csi/csi-test/v5/utils"
	"github.com/kubernetes-csi/external-health-monitor/pkg/mock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type MockPVHealthConditionChecker struct {
	pvHealthConditionChecker *PVHealthConditionChecker
	pvcInformer              informerV1.PersistentVolumeClaimInformer
	pvInformer               informerV1.PersistentVolumeInformer
	fakeClient               *fake.Clientset
	csiControllerServer      *driver.MockControllerServer
	csiNodeServer            *driver.MockNodeServer
}

func createMockPVHealthConditionChecker(t *testing.T) *MockPVHealthConditionChecker {
	k8sClient, informer := mock.FakeK8s()
	_, _, _, controllerServer, nodeServer, csiConn, err := mock.CreateMockServer(t)
	if err != nil {
		t.Fatal(err)
	}

	handler := NewCSIPVHandler(csiConn)
	return &MockPVHealthConditionChecker{
		pvHealthConditionChecker: &PVHealthConditionChecker{
			driverName:     mock.DriverName,
			timeout:        15 * time.Second,
			k8sClient:      k8sClient,
			pvcLister:      informer.Core().V1().PersistentVolumeClaims().Lister(),
			pvLister:       informer.Core().V1().PersistentVolumes().Lister(),
			csiPVHandler:   handler,
			knownUnhealthy: map[string]bool{},
			lastApplied:    map[string][]v1.VolumeHealthCondition{},
		},
		pvcInformer:         informer.Core().V1().PersistentVolumeClaims(),
		pvInformer:          informer.Core().V1().PersistentVolumes(),
		fakeClient:          k8sClient.(*fake.Clientset),
		csiControllerServer: controllerServer,
		csiNodeServer:       nodeServer,
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
			pv:           mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "1", "uid", &mock.FSVolumeMode, v1.VolumeBound),
			volumeId:     "1",
			health:       mock.AbnormalVolumeHealth("1"),
			wantPatch:    true,
			wantAbnormal: true,
		},
		{
			name:      "Healthy volume not previously unhealthy: no patch (no-op suppression)",
			pvc:       mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound),
			pv:        mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "2", "uid", &mock.FSVolumeMode, v1.VolumeBound),
			volumeId:  "2",
			health:    mock.HealthyVolumeHealth("2"),
			wantPatch: false,
		},
		{
			name:      "PV without CSI driver is skipped",
			pvc:       mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound),
			pv:        mock.CreatePVWithoutCSIDriver(2, "pvc", "pv", mock.DefaultNS, "1", "uid", v1.VolumeBound, &mock.FSVolumeMode),
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

			in := &csi.ControllerListVolumeHealthRequest{StartingToken: ""}
			out := &csi.ControllerListVolumeHealthResponse{
				Entries:   []*csi.VolumeHealth{tt.health},
				NextToken: "",
			}

			_, ctx := ktesting.NewTestContext(t)
			checker.csiControllerServer.EXPECT().ControllerListVolumeHealth(gomock.Any(), utils.Protobuf(in)).Return(out, nil).Times(1)
			if err := checker.pvHealthConditionChecker.CheckControllerListVolumeHealth(ctx); (err != nil) != tt.wantErr {
				t.Errorf("CheckControllerListVolumeHealth() error = %v, wantErr %v", err, tt.wantErr)
			}

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

	pv := mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "1", "uid", &mock.FSVolumeMode, v1.VolumeBound)
	pvc := mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound)
	assert.Nil(checker.pvInformer.Informer().GetStore().Add(pv))
	checker.seedPVC(t, pvc)

	_, ctx := ktesting.NewTestContext(t)

	// Cycle 1: abnormal via list -> tracked unhealthy.
	abnormalOut := &csi.ControllerListVolumeHealthResponse{Entries: []*csi.VolumeHealth{mock.AbnormalVolumeHealth("1")}}
	checker.csiControllerServer.EXPECT().ControllerListVolumeHealth(gomock.Any(), gomock.Any()).Return(abnormalOut, nil).Times(1)
	assert.Nil(checker.pvHealthConditionChecker.CheckControllerListVolumeHealth(ctx))
	assert.True(checker.pvHealthConditionChecker.knownUnhealthy["1"])

	// Cycle 2: absent from list, but a single Get confirms healthy -> cleared immediately.
	checker.fakeClient.ClearActions()
	emptyList := &csi.ControllerListVolumeHealthResponse{Entries: []*csi.VolumeHealth{}}
	checker.csiControllerServer.EXPECT().ControllerListVolumeHealth(gomock.Any(), gomock.Any()).Return(emptyList, nil).Times(1)
	getEmpty := &csi.ControllerGetVolumeHealthResponse{VolumeHealth: mock.HealthyVolumeHealth("1")}
	checker.csiControllerServer.EXPECT().ControllerGetVolumeHealth(gomock.Any(), gomock.Any()).Return(getEmpty, nil).Times(1)

	assert.Nil(checker.pvHealthConditionChecker.CheckControllerListVolumeHealth(ctx))

	patched, abnormal := healthStatusPatched(checker.fakeClient.Actions())
	assert.True(patched, "should clear after a single Get confirmation")
	assert.False(abnormal, "clearing patch must not carry conditions")
	assert.False(checker.pvHealthConditionChecker.knownUnhealthy["1"], "no longer tracked unhealthy after Get confirm")
}

func Test_FailedRPCIsNotRecovery(t *testing.T) {
	assert := assert.New(t)
	checker := createMockPVHealthConditionChecker(t)

	pv := mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "1", "uid", &mock.FSVolumeMode, v1.VolumeBound)
	pvc := mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound)
	assert.Nil(checker.pvInformer.Informer().GetStore().Add(pv))
	checker.seedPVC(t, pvc)

	_, ctx := ktesting.NewTestContext(t)

	// First establish unhealthy state.
	abnormal := &csi.ControllerGetVolumeHealthResponse{VolumeHealth: mock.AbnormalVolumeHealth("1")}
	checker.csiControllerServer.EXPECT().ControllerGetVolumeHealth(gomock.Any(), gomock.Any()).Return(abnormal, nil).Times(1)
	assert.Nil(checker.pvHealthConditionChecker.CheckControllerVolumeHealth(ctx, pv))
	assert.True(checker.pvHealthConditionChecker.knownUnhealthy["1"])

	// Now the RPC fails. The volume must remain tracked unhealthy and no patch issued.
	checker.fakeClient.ClearActions()
	checker.csiControllerServer.EXPECT().ControllerGetVolumeHealth(gomock.Any(), gomock.Any()).Return(nil, status.Error(codes.Unavailable, "driver down")).Times(1)

	err := checker.pvHealthConditionChecker.CheckControllerVolumeHealth(ctx, pv)
	assert.Error(err, "failed RPC should surface an error")

	patched, _ := healthStatusPatched(checker.fakeClient.Actions())
	assert.False(patched, "failed RPC must not issue a recovery patch")
	assert.True(checker.pvHealthConditionChecker.knownUnhealthy["1"], "failed RPC must not clear unhealthy state")
}

func Test_ConditionTransition(t *testing.T) {
	assert := assert.New(t)
	checker := createMockPVHealthConditionChecker(t)

	pv := mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "1", "uid", &mock.FSVolumeMode, v1.VolumeBound)
	pvc := mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound)
	assert.Nil(checker.pvInformer.Informer().GetStore().Add(pv))
	checker.seedPVC(t, pvc)

	_, ctx := ktesting.NewTestContext(t)

	// Condition A: Inaccessible/VolumeNotFound.
	condA := &csi.ControllerGetVolumeHealthResponse{VolumeHealth: mock.AbnormalVolumeHealth("1")}
	checker.csiControllerServer.EXPECT().ControllerGetVolumeHealth(gomock.Any(), gomock.Any()).Return(condA, nil).Times(1)
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
	checker.csiControllerServer.EXPECT().ControllerGetVolumeHealth(gomock.Any(), gomock.Any()).Return(condB, nil).Times(1)
	assert.Nil(checker.pvHealthConditionChecker.CheckControllerVolumeHealth(ctx, pv))

	patched, abnormal := healthStatusPatched(checker.fakeClient.Actions())
	assert.True(patched, "a condition transition A->B must issue a patch")
	assert.True(abnormal, "the transition patch must carry the new condition")
	assert.True(checker.pvHealthConditionChecker.knownUnhealthy["1"], "still unhealthy after transition")
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
			pv:      mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "2", "uid", &mock.FSVolumeMode, v1.VolumeBound),
			wantErr: false,
			want:    "2",
		},
		{
			name:    "PV without CSI driver Case",
			pv:      mock.CreatePVWithoutCSIDriver(2, "pvc", "pv", mock.DefaultNS, "1", "uid", v1.VolumeBound, &mock.FSVolumeMode),
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
			pv:           mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "1", "uid", &mock.FSVolumeMode, v1.VolumeBound),
			volumeId:     "1",
			health:       mock.AbnormalVolumeHealth("1"),
			expectRPC:    true,
			wantPatch:    true,
			wantAbnormal: true,
		},
		{
			name:      "Healthy volume: empty report, no patch",
			pvc:       mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound),
			pv:        mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "2", "uid", &mock.FSVolumeMode, v1.VolumeBound),
			volumeId:  "2",
			health:    mock.HealthyVolumeHealth("2"),
			expectRPC: true,
			wantPatch: false,
		},
		{
			name:      "PV without CSI driver: error, no RPC",
			pvc:       mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound),
			pv:        mock.CreatePVWithoutCSIDriver(2, "pvc", "pv", mock.DefaultNS, "1", "uid", v1.VolumeBound, &mock.FSVolumeMode),
			volumeId:  "1",
			expectRPC: false,
			wantErr:   true,
		},
		{
			name:      "PV not bound: error, no RPC",
			pvc:       mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound),
			pv:        mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "1", "uid", &mock.FSVolumeMode, v1.VolumePending),
			volumeId:  "1",
			expectRPC: false,
			wantErr:   true,
		},
		{
			name:      "PV with empty VolumeHandle: error, no RPC",
			pvc:       mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound),
			pv:        mock.CreatePVWithNilVolumeHandle(2, "pvc", "pv", mock.DefaultNS, "1", "uid", v1.VolumeBound, &mock.FSVolumeMode),
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
				in := &csi.ControllerGetVolumeHealthRequest{VolumeId: tt.volumeId}
				out := &csi.ControllerGetVolumeHealthResponse{VolumeHealth: tt.health}
				checker.csiControllerServer.EXPECT().ControllerGetVolumeHealth(gomock.Any(), utils.Protobuf(in)).Return(out, nil).Times(1)
			}

			_, ctx := ktesting.NewTestContext(t)
			if err := checker.pvHealthConditionChecker.CheckControllerVolumeHealth(ctx, tt.pv); (err != nil) != tt.wantErr {
				t.Errorf("CheckControllerVolumeHealth() error = %v, wantErr %v", err, tt.wantErr)
			}

			patched, abnormal := healthStatusPatched(checker.fakeClient.Actions())
			assert.Equal(tt.wantPatch, patched, "patch issued?")
			if tt.wantPatch {
				assert.Equal(tt.wantAbnormal, abnormal, "patch marks abnormal?")
			}
		})
	}
}
