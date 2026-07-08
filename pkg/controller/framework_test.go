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
	"github.com/kubernetes-csi/csi-test/v5/driver"
	"github.com/kubernetes-csi/csi-test/v5/utils"
	"github.com/kubernetes-csi/external-health-monitor/pkg/metrics"
	"github.com/kubernetes-csi/external-health-monitor/pkg/mock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
)

type fakeNativeObjects struct {
	MockVolume *mock.MockVolume
	MockNode   *mock.MockNode
}

type testCase struct {
	name                    string
	enableNodeWatcher       bool
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
	if tc.enableNodeWatcher {
		nativeObjects = append(nativeObjects, tc.fakeNativeObjects.MockNode.NativeNode)
	}

	client := fake.NewSimpleClientset(nativeObjects...)
	informers := informers.NewSharedInformerFactory(client, 0)
	pvInformer := informers.Core().V1().PersistentVolumes()
	pvcInformer := informers.Core().V1().PersistentVolumeClaims()
	nodeInformer := informers.Core().V1().Nodes()
	option := &PVMonitorOptions{
		DriverName:                "fake.csi.driver.io",
		ContextTimeout:            15 * time.Second,
		EnableNodeWatcher:         tc.enableNodeWatcher,
		ListVolumesInterval:       5 * time.Minute,
		PVWorkerExecuteInterval:   1 * time.Minute,
		VolumeListAndAddInterval:  5 * time.Minute,
		NodeWorkerExecuteInterval: 1 * time.Minute,
		NodeListAndAddInterval:    5 * time.Minute,
		SupportListVolumeHealth:   tc.supportListVolumeHealth,
	}

	_, _, _, controllerServer, _, csiConn, err := mock.CreateMockServer(t)
	assert.Nil(err)

	eventStore := make(chan string, 1)
	eventRecorder := record.FakeRecorder{
		Events: eventStore,
	}

	var volumes []*mock.CSIVolume
	volumes = append(volumes, tc.fakeNativeObjects.MockVolume.CSIVolume)
	err = pvInformer.Informer().GetStore().Add(tc.fakeNativeObjects.MockVolume.NativeVolume)
	assert.Nil(err)
	err = pvcInformer.Informer().GetStore().Add(tc.fakeNativeObjects.MockVolume.NativeVolumeClaim)
	assert.Nil(err)

	if tc.enableNodeWatcher {
		err = nodeInformer.Informer().GetStore().Add(tc.fakeNativeObjects.MockNode.NativeNode)
		assert.Nil(err)
	}

	logger, ctx := ktesting.NewTestContext(t)
	mockCSIControllerServer(controllerServer, tc.supportListVolumeHealth, volumes)
	pvMonitorController := NewPVMonitorController(logger, client, csiConn, informers, &eventRecorder, metrics.New(), option)
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

	cancel()
}

func mockCSIControllerServer(csiControllerServer *driver.MockControllerServer, supportListVolumeHealth bool, objects []*mock.CSIVolume) {
	if supportListVolumeHealth {
		entries := make([]*csi.VolumeHealth, len(objects))
		for index, volume := range objects {
			entries[index] = volume.Health
		}
		in := &csi.ControllerListVolumeHealthRequest{StartingToken: ""}
		out := &csi.ControllerListVolumeHealthResponse{
			Entries:   entries,
			NextToken: "",
		}
		csiControllerServer.EXPECT().ControllerListVolumeHealth(gomock.Any(), utils.Protobuf(in)).Return(out, nil).Times(100000)
	} else {
		for _, volume := range objects {
			in := &csi.ControllerGetVolumeHealthRequest{VolumeId: volume.Volume.VolumeId}
			out := &csi.ControllerGetVolumeHealthResponse{VolumeHealth: volume.Health}
			csiControllerServer.EXPECT().ControllerGetVolumeHealth(gomock.Any(), utils.Protobuf(in)).Return(out, nil).Times(100000)
		}
	}
}
