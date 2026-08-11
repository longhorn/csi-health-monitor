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
	"time"

	v1 "k8s.io/api/core/v1"

	"google.golang.org/grpc"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

var _ CSIHealthClient = &csiHealthClient{}

type csiHealthClient struct {
	controllerClient csi.ControllerClient
	// timeout bounds each individual CSI RPC, not a whole reconciliation cycle.
	timeout time.Duration
}

func NewCSIHealthClient(conn *grpc.ClientConn, timeout time.Duration) CSIHealthClient {
	return &csiHealthClient{
		controllerClient: csi.NewControllerClient(conn),
		timeout:          timeout,
	}
}

type VolumeHealthResult struct {
	Conditions []v1.VolumeHealthCondition
	Unknown    []UnknownCondition
}

// UnknownCondition is kept as raw strings because it deliberately never
// becomes a v1.VolumeHealthCondition.
type UnknownCondition struct {
	Status  string
	Reason  string
	Message string
}

// A volume absent from the returned map was not reported in this list cycle (distinct from
// present-but-empty, which means healthy); the caller can resolve absence with ControllerGetVolumeHealth.
func (client *csiHealthClient) ControllerListVolumeHealth(ctx context.Context) (map[string]*VolumeHealthResult, error) {
	p := map[string]*VolumeHealthResult{}

	token := ""
	for {
		rsp, err := client.listVolumeHealthPage(ctx, token)
		if err != nil {
			return nil, fmt.Errorf("list volume health: %w", err)
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

// Each page gets its own deadline so a large paginated listing is not
// squeezed into a single timeout.
func (client *csiHealthClient) listVolumeHealthPage(ctx context.Context, token string) (*csi.ControllerListVolumeHealthResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	return client.controllerClient.ControllerListVolumeHealth(ctx, &csi.ControllerListVolumeHealthRequest{
		StartingToken: token,
	})
}

// A non-error response with no health statuses is the explicit recovery signal and yields
// an empty (healthy) result.
func (client *csiHealthClient) ControllerGetVolumeHealth(ctx context.Context, volumeID string) (*VolumeHealthResult, error) {
	ctx, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	res, err := client.controllerClient.ControllerGetVolumeHealth(ctx, &csi.ControllerGetVolumeHealthRequest{
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
		status, ok := mapVolumeHealthErrorType(entry.GetStatus())
		if !ok {
			result.Unknown = append(result.Unknown, UnknownCondition{
				Status:  entry.GetStatus().String(),
				Reason:  entry.GetReason(),
				Message: entry.GetMessage(),
			})
			continue
		}
		result.Conditions = append(result.Conditions, v1.VolumeHealthCondition{
			Status:  status,
			Reason:  entry.GetReason(),
			Message: entry.GetMessage(),
		})
	}
	return result
}

func mapVolumeHealthErrorType(t csi.VolumeHealthErrorType) (v1.VolumeHealthStatusType, bool) {
	switch t {
	case csi.VolumeHealthErrorType_INACCESSIBLE:
		return v1.VolumeHealthInaccessible, true
	case csi.VolumeHealthErrorType_DATA_LOSS:
		return v1.VolumeHealthDataLoss, true
	case csi.VolumeHealthErrorType_DEGRADED:
		return v1.VolumeHealthDegraded, true
	default:
		return "", false
	}
}
