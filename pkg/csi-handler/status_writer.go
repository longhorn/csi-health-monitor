package csi_handler

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

const fieldManager = "csi-external-health-monitor-controller"

// Called only when the condition set changed, so the transition time is always fresh.
func buildHealthStatus(desired []v1.VolumeHealthCondition) *v1.VolumeHealthStatus {
	now := metav1.Now()
	return &v1.VolumeHealthStatus{
		HealthConditions:   desired,
		LastTransitionTime: now,
	}
}

// TODO replace with clientset.CoreV1().PersistentVolumeClaims(ns).ApplyStatus
//
// The returned bool reports whether the write actually persisted: the API
// server accepts the patch but drops healthStatus when the CSIVolumeHealth
// feature gate is disabled. A dropped write must not be recorded as applied,
// or the checker would believe the conditions were written and go silent for
// this PVC even after the gate is enabled.
func (checker *PVHealthConditionChecker) patchPVCHealthStatus(
	ctx context.Context,
	pvc *v1.PersistentVolumeClaim,
	status *v1.VolumeHealthStatus,
) (bool, error) {
	// A nil/empty status serializes to null, clearing the field server-side.
	var healthValue interface{}
	if status != nil && len(status.HealthConditions) > 0 {
		healthValue = status
	}

	patch := map[string]interface{}{
		"status": map[string]interface{}{
			"healthStatus": healthValue,
		},
	}
	data, err := json.Marshal(patch)
	if err != nil {
		return false, fmt.Errorf("failed to marshal health status patch for PVC %s/%s: %w", pvc.Namespace, pvc.Name, err)
	}

	ctx, cancel := context.WithTimeout(ctx, checker.timeout)
	defer cancel()
	updated, err := checker.k8sClient.CoreV1().PersistentVolumeClaims(pvc.Namespace).Patch(
		ctx,
		pvc.Name,
		types.MergePatchType,
		data,
		metav1.PatchOptions{FieldManager: fieldManager},
		"status",
	)
	if err != nil {
		return false, fmt.Errorf("failed to patch health status for PVC %s/%s: %w", pvc.Namespace, pvc.Name, err)
	}

	if healthValue == nil {
		// A clearing patch cannot probe the feature gate.
		return true, nil
	}
	if updated.Status.HealthStatus == nil {
		checker.recordDroppedStatusWrite()
		if checker.fieldDropped.CompareAndSwap(false, true) {
			klog.FromContext(ctx).Info("The API server dropped the healthStatus field, the CSIVolumeHealth feature gate appears to be disabled; health status will not be visible on PVCs", "pvc", klog.KObj(pvc))
		}
		return false, nil
	}
	if checker.fieldDropped.CompareAndSwap(true, false) {
		klog.FromContext(ctx).Info("The API server is persisting the healthStatus field again", "pvc", klog.KObj(pvc))
	}
	return true, nil
}

func pvcHealthConditions(pvc *v1.PersistentVolumeClaim) []v1.VolumeHealthCondition {
	if pvc == nil || pvc.Status.HealthStatus == nil {
		return nil
	}
	return normalizeConditions(pvc.Status.HealthStatus.HealthConditions)
}

// normalizeConditions returns a stable-sorted copy so equality comparison and patch output are deterministic.
func normalizeConditions(in []v1.VolumeHealthCondition) []v1.VolumeHealthCondition {
	if len(in) == 0 {
		return nil
	}
	out := make([]v1.VolumeHealthCondition, len(in))
	copy(out, in)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Status != out[j].Status {
			return out[i].Status < out[j].Status
		}
		return out[i].Reason < out[j].Reason
	})
	return out
}

// Compares status, reason, and message. A message-only change still triggers a patch.
// Inputs must be normalized first.
func conditionsEqual(a, b []v1.VolumeHealthCondition) bool {
	return reflect.DeepEqual(a, b)
}
