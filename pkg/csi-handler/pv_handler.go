/*
Copyright 2020 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package csi_handler

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"

	"google.golang.org/grpc"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

var _ CSIHandler = &csiPVHandler{}

type csiPVHandler struct {
	controllerClient csi.ControllerClient
}

func NewCSIPVHandler(conn *grpc.ClientConn) CSIHandler {
	return &csiPVHandler{
		controllerClient: csi.NewControllerClient(conn),
	}
}

type VolumeHealthResult struct {
	Conditions []v1.VolumeHealthCondition
}

// A volume absent from the returned map was not reported in this list cycle (distinct from
// present-but-empty, which means healthy); the caller can resolve absence with ControllerGetVolumeHealth.
func (handler *csiPVHandler) ControllerListVolumeHealth(ctx context.Context) (map[string]*VolumeHealthResult, error) {
	p := map[string]*VolumeHealthResult{}

	token := ""
	for {
		rsp, err := handler.controllerClient.ControllerListVolumeHealth(ctx, &csi.ControllerListVolumeHealthRequest{
			StartingToken: token,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to list volume health: %v", err)
		}

		for _, vh := range rsp.GetEntries() {
			p[vh.GetVolumeId()] = volumeHealthToResult(vh)
		}

		token = rsp.GetNextToken()
		if len(token) == 0 {
			break
		}
	}
	return p, nil
}

// A non-error response with no health statuses is the explicit recovery signal and yields
// an empty (healthy) result.
func (handler *csiPVHandler) ControllerGetVolumeHealth(ctx context.Context, volumeID string) (*VolumeHealthResult, error) {
	res, err := handler.controllerClient.ControllerGetVolumeHealth(ctx, &csi.ControllerGetVolumeHealthRequest{
		VolumeId: volumeID,
	})
	if err != nil {
		// A failed RPC is not a recovery; let the caller leave stored conditions in place.
		return nil, err
	}

	return volumeHealthToResult(res.GetVolumeHealth()), nil
}

func volumeHealthToResult(vh *csi.VolumeHealth) *VolumeHealthResult {
	result := &VolumeHealthResult{}
	if vh == nil {
		return result
	}
	for _, entry := range vh.GetHealthStatuses() {
		if entry == nil {
			continue
		}
		result.Conditions = append(result.Conditions, v1.VolumeHealthCondition{
			Status:  mapVolumeHealthErrorType(entry.GetStatus()),
			Reason:  entry.GetReason(),
			Message: entry.GetMessage(),
		})
	}
	return result
}

func mapVolumeHealthErrorType(t csi.VolumeHealthErrorType) v1.VolumeHealthStatusType {
	switch t {
	case csi.VolumeHealthErrorType_INACCESSIBLE:
		return v1.VolumeHealthInaccessible
	case csi.VolumeHealthErrorType_DATA_LOSS:
		return v1.VolumeHealthDataLoss
	case csi.VolumeHealthErrorType_DEGRADED:
		return v1.VolumeHealthDegraded
	default:
		return v1.VolumeHealthDegraded
	}
}
