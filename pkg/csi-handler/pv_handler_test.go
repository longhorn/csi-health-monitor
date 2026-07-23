package csi_handler

import (
	"context"
	"reflect"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/external-health-monitor/pkg/mock"
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
	drv, csiConn := mock.StartFakeDriver(t)

	handler := NewCSIPVHandler(csiConn, 15*time.Second)
	out := &csi.ControllerListVolumeHealthResponse{
		Entries: []*csi.VolumeHealth{
			abnormalVolumeHealth,
			healthyVolumeHealth,
		},
		NextToken: "",
	}
	drv.Controller.SetListVolumeHealth(out, nil)

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
			reqs := drv.Controller.ListVolumeHealthRequests()
			if len(reqs) != 1 {
				t.Errorf("expected exactly 1 list RPC, got %d", len(reqs))
			} else if reqs[0].GetStartingToken() != "" {
				t.Errorf("expected empty starting token, got %q", reqs[0].GetStartingToken())
			}
		})
	}
}

func Test_csiPVHandler_ControllerGetVolumeHealth(t *testing.T) {
	drv, csiConn := mock.StartFakeDriver(t)

	handler := NewCSIPVHandler(csiConn, 15*time.Second)
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
			out := &csi.ControllerGetVolumeHealthResponse{
				VolumeHealth: volumeMap[tt.volumeId].Health,
			}
			drv.Controller.SetGetVolumeHealth(tt.volumeId, out, nil)
			reqsBefore := len(drv.Controller.GetVolumeHealthRequests())

			got, err := handler.ControllerGetVolumeHealth(context.Background(), tt.volumeId)
			if (err != nil) != tt.wantErr {
				t.Errorf("csiPVHandler.ControllerGetVolumeHealth() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("csiPVHandler.ControllerGetVolumeHealth() = %v, want %v", got, tt.want)
			}
			reqs := drv.Controller.GetVolumeHealthRequests()
			if len(reqs) != reqsBefore+1 {
				t.Errorf("expected exactly 1 additional Get RPC, got %d", len(reqs)-reqsBefore)
			} else if reqs[len(reqs)-1].GetVolumeId() != tt.volumeId {
				t.Errorf("expected request for volume %q, got %q", tt.volumeId, reqs[len(reqs)-1].GetVolumeId())
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

// Guards against reintroducing a single deadline around a whole listing
// cycle: every page and every get must get a fresh per-RPC deadline.
func Test_csiPVHandler_PerRPCDeadlines(t *testing.T) {
	drv, csiConn := mock.StartFakeDriver(t)
	handler := NewCSIPVHandler(csiConn, 15*time.Second)

	page1 := &csi.ControllerListVolumeHealthResponse{
		Entries:   []*csi.VolumeHealth{abnormalVolumeHealth},
		NextToken: "page-2",
	}
	page2 := &csi.ControllerListVolumeHealthResponse{
		Entries: []*csi.VolumeHealth{healthyVolumeHealth},
	}
	drv.Controller.SetListVolumeHealthPages(page1, page2)
	drv.Controller.SetGetVolumeHealth("1", &csi.ControllerGetVolumeHealthResponse{VolumeHealth: abnormalVolumeHealth}, nil)

	got, err := handler.ControllerListVolumeHealth(context.Background())
	if err != nil {
		t.Fatalf("ControllerListVolumeHealth() error = %v", err)
	}
	want := map[string]*VolumeHealthResult{
		"1": {Conditions: abnormalConditions},
		"2": {Conditions: nil},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ControllerListVolumeHealth() = %v, want %v", got, want)
	}

	if _, err := handler.ControllerGetVolumeHealth(context.Background(), "1"); err != nil {
		t.Fatalf("ControllerGetVolumeHealth() error = %v", err)
	}

	listDeadlines := drv.Controller.ListVolumeHealthDeadlines()
	if len(listDeadlines) != 2 {
		t.Fatalf("expected 2 list pages, got %d", len(listDeadlines))
	}
	getDeadlines := drv.Controller.GetVolumeHealthDeadlines()
	if len(getDeadlines) != 1 {
		t.Fatalf("expected 1 get request, got %d", len(getDeadlines))
	}
	for i, d := range append(listDeadlines, getDeadlines...) {
		if d.IsZero() {
			t.Errorf("request %d carried no deadline", i)
		}
	}
	if !listDeadlines[1].After(listDeadlines[0]) {
		t.Errorf("second page must get a fresh deadline, got %v then %v", listDeadlines[0], listDeadlines[1])
	}
	if !getDeadlines[0].After(listDeadlines[1]) {
		t.Errorf("get must not inherit the list deadline, got %v after %v", getDeadlines[0], listDeadlines[1])
	}
}
