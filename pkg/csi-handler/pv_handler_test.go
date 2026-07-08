package csi_handler

import (
	"context"
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/csi-test/v5/utils"
	"github.com/kubernetes-csi/external-health-monitor/pkg/mock"
	"go.uber.org/mock/gomock"
)

var (
	volume1 = &csi.Volume{
		VolumeId: "1",
	}

	volume2 = &csi.Volume{
		VolumeId: "2",
	}

	abnormalVolumeHealth = &csi.VolumeHealth{
		VolumeId: "1",
		HealthStatuses: []*csi.VolumeHealth_VolumeHealthEntry{
			{
				Status:  csi.VolumeHealthErrorType_INACCESSIBLE,
				Reason:  "VolumeNotFound",
				Message: "Volume not found",
			},
		},
	}

	healthyVolumeHealth = &csi.VolumeHealth{
		VolumeId:       "2",
		HealthStatuses: nil,
	}

	volumeMap = map[string]VolumeSample{
		"1": {
			Volume: volume1,
			Health: abnormalVolumeHealth,
		},
		"2": {
			Volume: volume2,
			Health: healthyVolumeHealth,
		},
	}

	abnormalConditions = []v1.VolumeHealthCondition{
		{
			Status:  v1.VolumeHealthInaccessible,
			Reason:  "VolumeNotFound",
			Message: "Volume not found",
		},
	}
)

type VolumeSample struct {
	Volume *csi.Volume
	Health *csi.VolumeHealth
}

func Test_csiPVHandler_ControllerListVolumeHealth(t *testing.T) {
	mockController, driver, _, controllerServer, _, csiConn, err := mock.CreateMockServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer mockController.Finish()
	defer driver.Stop()

	handler := NewCSIPVHandler(csiConn)
	in := &csi.ControllerListVolumeHealthRequest{
		StartingToken: "",
	}
	out := &csi.ControllerListVolumeHealthResponse{
		Entries: []*csi.VolumeHealth{
			abnormalVolumeHealth,
			healthyVolumeHealth,
		},
		NextToken: "",
	}

	controllerServer.EXPECT().ControllerListVolumeHealth(gomock.Any(), utils.Protobuf(in)).Return(out, nil).Times(1)
	tests := []struct {
		name    string
		want    map[string]*VolumeHealthResult
		wantErr bool
	}{
		{
			name: "case1",
			want: map[string]*VolumeHealthResult{
				"1": {Conditions: abnormalConditions},
				"2": {Conditions: nil},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := handler.ControllerListVolumeHealth(context.Background())
			if (err != nil) != tt.wantErr {
				t.Errorf("csiPVHandler.ControllerListVolumeHealth() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("csiPVHandler.ControllerListVolumeHealth() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_csiPVHandler_ControllerGetVolumeHealth(t *testing.T) {
	mockController, driver, _, controllerServer, _, csiConn, err := mock.CreateMockServer(t)
	if err != nil {
		t.Fatal(err)
	}
	defer mockController.Finish()
	defer driver.Stop()

	handler := NewCSIPVHandler(csiConn)
	tests := []struct {
		name     string
		want     *VolumeHealthResult
		volumeId string
		wantErr  bool
	}{
		{
			name:     "AbnormalCase",
			volumeId: "1",
			want:     &VolumeHealthResult{Conditions: abnormalConditions},
			wantErr:  false,
		},
		{
			name:     "HealthyCase",
			volumeId: "2",
			want:     &VolumeHealthResult{Conditions: nil},
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := &csi.ControllerGetVolumeHealthRequest{
				VolumeId: tt.volumeId,
			}
			out := &csi.ControllerGetVolumeHealthResponse{
				VolumeHealth: volumeMap[tt.volumeId].Health,
			}
			controllerServer.EXPECT().ControllerGetVolumeHealth(gomock.Any(), utils.Protobuf(in)).Return(out, nil).Times(1)
			got, err := handler.ControllerGetVolumeHealth(context.Background(), tt.volumeId)
			if (err != nil) != tt.wantErr {
				t.Errorf("csiPVHandler.ControllerGetVolumeHealth() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("csiPVHandler.ControllerGetVolumeHealth() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_mapVolumeHealthErrorType(t *testing.T) {
	tests := []struct {
		name string
		in   csi.VolumeHealthErrorType
		want v1.VolumeHealthStatusType
	}{
		{"inaccessible", csi.VolumeHealthErrorType_INACCESSIBLE, v1.VolumeHealthInaccessible},
		{"dataloss", csi.VolumeHealthErrorType_DATA_LOSS, v1.VolumeHealthDataLoss},
		{"degraded", csi.VolumeHealthErrorType_DEGRADED, v1.VolumeHealthDegraded},
		// Unknown / future enum values must not be treated as healthy; they surface as Degraded.
		{"unknown", csi.VolumeHealthErrorType_UNKNOWN_VOLUME_HEALTH_TYPE, v1.VolumeHealthDegraded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mapVolumeHealthErrorType(tt.in); got != tt.want {
				t.Errorf("mapVolumeHealthErrorType(%v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}
