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

package csi_handler

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/kubernetes-csi/external-health-monitor/pkg/mock"
	"github.com/stretchr/testify/assert"
)

func TestPatchPVCHealthStatusRetriesWithLatestResourceVersion(t *testing.T) {
	assert := assert.New(t)
	pvc := mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound)
	pvc.ResourceVersion = "1"
	current := pvc.DeepCopy()

	client := fake.NewSimpleClientset()
	checker := &PVHealthConditionChecker{
		k8sClient: client,
		timeout:   time.Second,
	}

	getAttempts := 0
	client.PrependReactor("get", "persistentvolumeclaims", func(k8stesting.Action) (bool, runtime.Object, error) {
		getAttempts++
		return true, current.DeepCopy(), nil
	})

	var patchResourceVersions []string
	var patchUIDs []types.UID
	status := buildHealthStatus([]v1.VolumeHealthCondition{{
		Status:  v1.VolumeHealthDegraded,
		Reason:  "SlowIO",
		Message: "slow",
	}})
	client.PrependReactor("patch", "persistentvolumeclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
		patchAction := action.(k8stesting.PatchAction)
		var patch struct {
			Metadata struct {
				ResourceVersion string    `json:"resourceVersion"`
				UID             types.UID `json:"uid"`
			} `json:"metadata"`
		}
		if err := json.Unmarshal(patchAction.GetPatch(), &patch); err != nil {
			return true, nil, err
		}
		patchResourceVersions = append(patchResourceVersions, patch.Metadata.ResourceVersion)
		patchUIDs = append(patchUIDs, patch.Metadata.UID)

		if len(patchResourceVersions) == 1 {
			current = current.DeepCopy()
			current.ResourceVersion = "2"
			return true, nil, apierrors.NewConflict(v1.Resource("persistentvolumeclaims"), pvc.Name, errors.New("test conflict"))
		}

		updated := current.DeepCopy()
		updated.ResourceVersion = "3"
		updated.Status.HealthStatus = status
		return true, updated, nil
	})

	persisted, err := checker.patchPVCHealthStatus(context.Background(), pvc, status)

	assert.NoError(err)
	assert.True(persisted)
	assert.Equal(2, getAttempts)
	assert.Equal([]string{"1", "2"}, patchResourceVersions)
	assert.Equal([]types.UID{"uid", "uid"}, patchUIDs)
}

func TestPatchPVCHealthStatusDoesNotRetryOntoRecreatedPVC(t *testing.T) {
	assert := assert.New(t)
	pvc := mock.CreatePVC(1, 2, "pvc", "uid-old", mock.DefaultNS, "pv", v1.ClaimBound)
	pvc.ResourceVersion = "1"
	current := pvc.DeepCopy()

	client := fake.NewSimpleClientset()
	checker := &PVHealthConditionChecker{
		k8sClient: client,
		timeout:   time.Second,
	}

	getAttempts := 0
	client.PrependReactor("get", "persistentvolumeclaims", func(k8stesting.Action) (bool, runtime.Object, error) {
		getAttempts++
		return true, current.DeepCopy(), nil
	})

	patchAttempts := 0
	client.PrependReactor("patch", "persistentvolumeclaims", func(k8stesting.Action) (bool, runtime.Object, error) {
		patchAttempts++
		current = current.DeepCopy()
		current.UID = "uid-new"
		current.ResourceVersion = "2"
		return true, nil, apierrors.NewConflict(v1.Resource("persistentvolumeclaims"), pvc.Name, errors.New("test conflict"))
	})

	status := buildHealthStatus([]v1.VolumeHealthCondition{{
		Status: v1.VolumeHealthDegraded,
		Reason: "SlowIO",
	}})
	persisted, err := checker.patchPVCHealthStatus(context.Background(), pvc, status)

	assert.False(persisted)
	assert.ErrorContains(err, "changed UID")
	assert.Equal(2, getAttempts)
	assert.Equal(1, patchAttempts, "the recreated PVC must not receive a retry")
}

func TestPatchPVCHealthStatusSkipsPatchWhenLiveStatusMatches(t *testing.T) {
	assert := assert.New(t)
	pvc := mock.CreatePVC(1, 2, "pvc", "uid", mock.DefaultNS, "pv", v1.ClaimBound)
	pvc.ResourceVersion = "1"
	status := buildHealthStatus([]v1.VolumeHealthCondition{{
		Status: v1.VolumeHealthDegraded,
		Reason: "SlowIO",
	}})

	current := pvc.DeepCopy()
	current.ResourceVersion = "2"
	current.Status.HealthStatus = status
	client := fake.NewSimpleClientset(current)
	checker := &PVHealthConditionChecker{
		k8sClient: client,
		timeout:   time.Second,
	}

	persisted, err := checker.patchPVCHealthStatus(context.Background(), pvc, status)

	assert.NoError(err)
	assert.True(persisted)
	for _, action := range client.Actions() {
		_, isPatch := action.(k8stesting.PatchAction)
		assert.False(isPatch, "matching live status must not be patched again")
	}
}
