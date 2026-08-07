package pv_monitor_controller

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2/ktesting"
	_ "k8s.io/klog/v2/ktesting/init"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/external-health-monitor/pkg/metrics"
	"github.com/kubernetes-csi/external-health-monitor/pkg/mock"
	"github.com/stretchr/testify/assert"
)

type fakeNativeObjects struct {
	MockVolume *mock.MockVolume
}

type testCase struct {
	fakeNativeObjects       *fakeNativeObjects
	supportListVolumeHealth bool
	wantAbnormalPatch       bool
}

func waitForHealthStatusPatch(client *fake.Clientset, timeout time.Duration) (seen bool, abnormal bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, action := range client.Actions() {
			patchAction, ok := action.(k8stesting.PatchAction)
			if !ok {
				continue
			}
			if patchAction.GetResource().Resource != "persistentvolumeclaims" || patchAction.GetSubresource() != "status" {
				continue
			}
			seen = true
			if strings.Contains(string(patchAction.GetPatch()), "\"healthConditions\"") {
				abnormal = true
			}
			return seen, abnormal
		}
		time.Sleep(20 * time.Millisecond)
	}
	return seen, abnormal
}

func runTest(t *testing.T, tc *testCase) {
	assert := assert.New(t)
	nativeObjects := []runtime.Object{
		tc.fakeNativeObjects.MockVolume.NativeVolume,
		tc.fakeNativeObjects.MockVolume.NativeVolumeClaim,
	}
	client := fake.NewSimpleClientset(nativeObjects...)
	informers := informers.NewSharedInformerFactory(client, 0)
	pvInformer := informers.Core().V1().PersistentVolumes()
	pvcInformer := informers.Core().V1().PersistentVolumeClaims()
	option := &PVMonitorOptions{
		DriverName:               "fake.csi.driver.io",
		ContextTimeout:           15 * time.Second,
		ListVolumesInterval:      5 * time.Minute,
		PVWorkerExecuteInterval:  1 * time.Minute,
		VolumeListAndAddInterval: 5 * time.Minute,
		SupportListVolumeHealth:  tc.supportListVolumeHealth,
	}

	drv, csiConn := mock.StartFakeDriver(t)

	var volumes []*mock.CSIVolume
	volumes = append(volumes, tc.fakeNativeObjects.MockVolume.CSIVolume)
	err := pvInformer.Informer().GetStore().Add(tc.fakeNativeObjects.MockVolume.NativeVolume)
	assert.Nil(err)
	err = pvcInformer.Informer().GetStore().Add(tc.fakeNativeObjects.MockVolume.NativeVolumeClaim)
	assert.Nil(err)

	_, ctx := ktesting.NewTestContext(t)
	programFakeControllerServer(drv.Controller, tc.supportListVolumeHealth, volumes)
	pvMonitorController := NewPVMonitorController(client, csiConn, informers, metrics.New(), record.NewFakeRecorder(100), option)
	assert.NotNil(pvMonitorController)

	ctx, cancel := context.WithCancel(ctx)
	stopCh := ctx.Done()
	informers.Start(stopCh)
	var wg sync.WaitGroup
	go pvMonitorController.Run(ctx, 1, &wg)

	seen, abnormal := waitForHealthStatusPatch(client, 5*time.Second)
	if tc.wantAbnormalPatch {
		assert.True(seen, "expected a healthStatus patch")
		assert.True(abnormal, "expected the patch to carry abnormal conditions")
	} else {
		assert.False(seen, "expected no healthStatus patch for a healthy volume")
	}

	if tc.supportListVolumeHealth {
		assert.Equal(0, pvMonitorController.pvQueue.Len(), "List mode must not accumulate PV queue entries")
	}

	cancel()
}

func programFakeControllerServer(ctrl *mock.FakeControllerServer, supportListVolumeHealth bool, objects []*mock.CSIVolume) {
	if supportListVolumeHealth {
		entries := make([]*csi.VolumeHealth, len(objects))
		for index, volume := range objects {
			entries[index] = volume.Health
		}
		ctrl.SetListVolumeHealth(&csi.ControllerListVolumeHealthResponse{Entries: entries}, nil)
	} else {
		for _, volume := range objects {
			ctrl.SetGetVolumeHealth(volume.Volume.VolumeId, &csi.ControllerGetVolumeHealthResponse{VolumeHealth: volume.Health}, nil)
		}
	}
}
