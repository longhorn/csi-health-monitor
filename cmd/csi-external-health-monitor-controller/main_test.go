/*
Copyright 2026 The Kubernetes Authors.

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

package main

import (
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/csi-lib-utils/rpc"
)

func TestHealthMode(t *testing.T) {
	tests := []struct {
		name        string
		list, get   bool
		wantUseList bool
		wantErr     bool
	}{
		{name: "get only", get: true, wantUseList: false},
		{name: "list and get", list: true, get: true, wantUseList: true},
		{name: "neither", wantErr: true},
		{name: "list without required get", list: true, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			caps := rpc.ControllerCapabilitySet{
				csi.ControllerServiceCapability_RPC_LIST_VOLUME_HEALTH: tt.list,
				csi.ControllerServiceCapability_RPC_GET_VOLUME_HEALTH:  tt.get,
			}
			useList, err := healthMode(caps)
			if (err != nil) != tt.wantErr {
				t.Fatalf("healthMode() error = %v, wantErr %v", err, tt.wantErr)
			}
			if useList != tt.wantUseList {
				t.Errorf("healthMode() = %v, want %v", useList, tt.wantUseList)
			}
		})
	}
}
