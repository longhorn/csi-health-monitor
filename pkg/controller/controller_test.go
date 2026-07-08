package pv_monitor_controller

import (
	"testing"

	v1 "k8s.io/api/core/v1"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/external-health-monitor/pkg/mock"
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
