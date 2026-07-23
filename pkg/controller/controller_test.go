package pv_monitor_controller

import (
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2/ktesting"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/external-health-monitor/pkg/metrics"
	"github.com/kubernetes-csi/external-health-monitor/pkg/mock"
	"github.com/stretchr/testify/assert"
)

func csiVolume(id string) *csi.Volume {
	return &csi.Volume{VolumeId: id}
}

func abnormalMockVolume() *mock.MockVolume {
	return &mock.MockVolume{
		CSIVolume: &mock.CSIVolume{
			Volume: csiVolume("abnormalVolume1"),
			Health: mock.AbnormalVolumeHealth("abnormalVolume1"),
		},
		NativeVolume:      mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "abnormalVolume1", "pvcuid", &mock.FSVolumeMode, v1.VolumeBound),
		NativeVolumeClaim: mock.CreatePVC(1, 2, "pvc", "pvcuid", mock.DefaultNS, "pv", v1.ClaimBound),
	}
}

func healthyMockVolume() *mock.MockVolume {
	return &mock.MockVolume{
		CSIVolume: &mock.CSIVolume{
			Volume: csiVolume("normalVolume1"),
			Health: mock.HealthyVolumeHealth("normalVolume1"),
		},
		NativeVolume:      mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "normalVolume1", "pvcuid", &mock.FSVolumeMode, v1.VolumeBound),
		NativeVolumeClaim: mock.CreatePVC(1, 2, "pvc", "pvcuid", mock.DefaultNS, "pv", v1.ClaimBound),
	}
}

func Test_AbnormalVolumeWithListVolumeHealth(t *testing.T) {
	runTest(t, &testCase{
		name:                    "abnormal_volume_list",
		supportListVolumeHealth: true,
		fakeNativeObjects:       &fakeNativeObjects{MockVolume: abnormalMockVolume()},
		wantAbnormalPatch:       true,
	})
}

func Test_NormalVolumeWithListVolumeHealth(t *testing.T) {
	runTest(t, &testCase{
		name:                    "normal_volume_list",
		supportListVolumeHealth: true,
		fakeNativeObjects:       &fakeNativeObjects{MockVolume: healthyMockVolume()},
		wantAbnormalPatch:       false,
	})
}

func Test_AbnormalVolumeWithGetVolumeHealth(t *testing.T) {
	runTest(t, &testCase{
		name:                    "abnormal_volume_get",
		supportListVolumeHealth: false,
		fakeNativeObjects:       &fakeNativeObjects{MockVolume: abnormalMockVolume()},
		wantAbnormalPatch:       true,
	})
}

func Test_NormalVolumeWithGetVolumeHealth(t *testing.T) {
	runTest(t, &testCase{
		name:                    "normal_volume_get",
		supportListVolumeHealth: false,
		fakeNativeObjects:       &fakeNativeObjects{MockVolume: healthyMockVolume()},
		wantAbnormalPatch:       false,
	})
}

// newGetModeFixture builds a Get-mode controller whose queue and workers are
// driven manually by the test; the informer factory is never started.
func newGetModeFixture(t *testing.T, pv *v1.PersistentVolume, pvc *v1.PersistentVolumeClaim) (*PVMonitorController, *mock.FakeCSIDriver, cache.Store) {
	t.Helper()

	client := fake.NewSimpleClientset(pv, pvc)
	factory := informers.NewSharedInformerFactory(client, 0)
	pvStore := factory.Core().V1().PersistentVolumes().Informer().GetStore()
	pvcStore := factory.Core().V1().PersistentVolumeClaims().Informer().GetStore()

	drv, csiConn := mock.StartFakeDriver(t)
	option := &PVMonitorOptions{
		DriverName:               mock.DriverName,
		ContextTimeout:           15 * time.Second,
		ListVolumesInterval:      5 * time.Minute,
		PVWorkerExecuteInterval:  1 * time.Minute,
		VolumeListAndAddInterval: 5 * time.Minute,
		SupportListVolumeHealth:  false,
	}
	ctrl := NewPVMonitorController(client, csiConn, factory, metrics.New(), option)

	if err := pvStore.Add(pv); err != nil {
		t.Fatal(err)
	}
	if err := pvcStore.Add(pvc); err != nil {
		t.Fatal(err)
	}
	return ctrl, drv, pvStore
}

func Test_PendingPVIsMonitoredOnceBound(t *testing.T) {
	assert := assert.New(t)

	pv := mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "vol-1", "pvcuid", &mock.FSVolumeMode, v1.VolumePending)
	pvc := mock.CreatePVC(1, 2, "pvc", "pvcuid", mock.DefaultNS, "pv", v1.ClaimBound)
	ctrl, drv, pvStore := newGetModeFixture(t, pv, pvc)
	drv.Controller.SetGetVolumeHealth("vol-1", &csi.ControllerGetVolumeHealthResponse{VolumeHealth: mock.AbnormalVolumeHealth("vol-1")}, nil)

	// While pending, the periodic pass must leave the PV alone.
	assert.Nil(ctrl.AddPVsToQueue())
	assert.Equal(0, ctrl.pvQueue.Len())
	assert.False(ctrl.pvEnqueued["pv"])

	bound := pv.DeepCopy()
	bound.Status.Phase = v1.VolumeBound
	assert.Nil(pvStore.Update(bound))

	assert.Nil(ctrl.AddPVsToQueue())
	assert.Equal(1, ctrl.pvQueue.Len())

	_, ctx := ktesting.NewTestContext(t)
	ctrl.checkPVWorker(ctx)

	assert.Len(drv.Controller.GetVolumeHealthRequests(), 1, "bound PV must be health-checked")
}

func Test_PVUnboundAtPopIsForgottenAndPickedUpAgain(t *testing.T) {
	assert := assert.New(t)

	pv := mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "vol-1", "pvcuid", &mock.FSVolumeMode, v1.VolumeBound)
	pvc := mock.CreatePVC(1, 2, "pvc", "pvcuid", mock.DefaultNS, "pv", v1.ClaimBound)
	ctrl, drv, pvStore := newGetModeFixture(t, pv, pvc)
	drv.Controller.SetGetVolumeHealth("vol-1", &csi.ControllerGetVolumeHealthResponse{VolumeHealth: mock.AbnormalVolumeHealth("vol-1")}, nil)

	// Enqueued while bound, but released again before the worker pops it.
	ctrl.pvAdded(pv)
	released := pv.DeepCopy()
	released.Status.Phase = v1.VolumeReleased
	assert.Nil(pvStore.Update(released))

	_, ctx := ktesting.NewTestContext(t)
	ctrl.checkPVWorker(ctx)

	assert.Empty(drv.Controller.GetVolumeHealthRequests(), "unbound PV must not be health-checked")
	assert.False(ctrl.pvEnqueued["pv"], "dropped PV must be forgotten so it can be re-added")

	// Once bound again, the periodic pass picks it up and it gets checked.
	assert.Nil(pvStore.Update(pv.DeepCopy()))
	assert.Nil(ctrl.AddPVsToQueue())
	assert.Equal(1, ctrl.pvQueue.Len())

	ctrl.checkPVWorker(ctx)
	assert.Len(drv.Controller.GetVolumeHealthRequests(), 1, "re-bound PV must be health-checked")
}

func Test_DeletingPVIsForgotten(t *testing.T) {
	assert := assert.New(t)

	pv := mock.CreatePV(2, "pvc", "pv", mock.DefaultNS, "vol-1", "pvcuid", &mock.FSVolumeMode, v1.VolumeBound)
	pvc := mock.CreatePVC(1, 2, "pvc", "pvcuid", mock.DefaultNS, "pv", v1.ClaimBound)
	ctrl, drv, pvStore := newGetModeFixture(t, pv, pvc)

	ctrl.pvAdded(pv)
	deleting := pv.DeepCopy()
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	assert.Nil(pvStore.Update(deleting))

	_, ctx := ktesting.NewTestContext(t)
	ctrl.checkPVWorker(ctx)

	assert.Empty(drv.Controller.GetVolumeHealthRequests(), "deleting PV must not be health-checked")
	assert.False(ctrl.pvEnqueued["pv"], "deleting PV must not stay tracked")
	assert.Nil(ctrl.AddPVsToQueue())
	assert.Equal(0, ctrl.pvQueue.Len(), "deleting PV must not be re-enqueued by the periodic pass")
}
