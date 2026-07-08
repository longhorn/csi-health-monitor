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
	"sync"
	"time"

	"google.golang.org/grpc"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"

	"github.com/kubernetes-csi/external-health-monitor/pkg/metrics"
)

// PVHealthConditionChecker probes controller-side volume health and reconciles it onto
// pvc.status.healthStatus.
type PVHealthConditionChecker struct {
	driverName string

	timeout   time.Duration
	k8sClient kubernetes.Interface

	pvcLister corelisters.PersistentVolumeClaimLister
	pvLister  corelisters.PersistentVolumeLister

	csiPVHandler CSIHandler

	metrics *metrics.Metrics

	// recoveryStateMu guards recovery state mutated from the GetVolume workers and the
	// ControllerListVolumeHealth goroutine.
	recoveryStateMu sync.Mutex
	// knownUnhealthy tracks volume handles reported unhealthy during this process;
	// PVC status covers unhealthy state that predates this process.
	knownUnhealthy map[string]bool
	// lastApplied is a short-lived write-through cache used while the informer has
	// not observed our status patch yet. The PVC status remains the source of truth:
	// entries only apply to the resourceVersion they were computed from.
	lastApplied map[string]appliedHealthStatus
}

type appliedHealthStatus struct {
	resourceVersion string
	conditions      []v1.VolumeHealthCondition
}

func NewPVHealthConditionChecker(
	name string,
	conn *grpc.ClientConn,
	kClient kubernetes.Interface,
	timeout time.Duration,
	pvcLister corelisters.PersistentVolumeClaimLister,
	pvLister corelisters.PersistentVolumeLister,
	healthMetrics *metrics.Metrics,
) *PVHealthConditionChecker {
	return &PVHealthConditionChecker{
		driverName:     name,
		k8sClient:      kClient,
		pvcLister:      pvcLister,
		pvLister:       pvLister,
		timeout:        timeout,
		csiPVHandler:   NewCSIPVHandler(conn),
		metrics:        healthMetrics,
		knownUnhealthy: map[string]bool{},
		lastApplied:    map[string]appliedHealthStatus{},
	}
}

// A previously-unhealthy volume absent from a complete list cycle is resolved with
// ControllerGetVolumeHealth. The CSI spec requires Get when List is supported.
func (checker *PVHealthConditionChecker) CheckControllerListVolumeHealth(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, checker.timeout)
	defer cancel()

	// A failed list RPC is not a recovery: leave stored conditions
	// untouched and try again next cycle.
	start := time.Now()
	result, err := checker.csiPVHandler.ControllerListVolumeHealth(ctx)
	checker.observeProbe(metrics.MethodList, start, err)
	if err != nil {
		return err
	}

	pvs, err := checker.pvLister.List(labels.Everything())
	if err != nil {
		return err
	}

	logger := klog.FromContext(ctx)
	for _, pv := range pvs {
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != checker.driverName {
			continue
		}
		if pv.Status.Phase != v1.VolumeBound {
			continue
		}

		volumeHandle, err := checker.GetVolumeHandle(pv)
		if err != nil {
			logger.Error(err, "Get volume handle error")
			continue
		}

		pvc, err := checker.pvcLister.PersistentVolumeClaims(pv.Spec.ClaimRef.Namespace).Get(pv.Spec.ClaimRef.Name)
		if err != nil {
			logger.Error(err, "Get PVC error")
			continue
		}

		health, present := result[volumeHandle]
		if present {
			if err := checker.reconcileAndTrack(ctx, pvc, volumeHandle, health.Conditions); err != nil {
				logger.Error(err, "Reconcile PVC health status error", "pvc", pvc.Name)
			}
			continue
		}

		if err := checker.handleAbsentInListCycle(ctx, pvc, volumeHandle); err != nil {
			logger.Error(err, "Recover PVC health status error", "pvc", pvc.Name)
		}
	}

	return nil
}

// No-op unless the volume is currently believed unhealthy or PVC status already
// has stored health conditions from a previous controller run.
func (checker *PVHealthConditionChecker) handleAbsentInListCycle(ctx context.Context, pvc *v1.PersistentVolumeClaim, volumeHandle string) error {
	if !checker.needsAbsentVolumeConfirmation(pvc, volumeHandle) {
		return nil
	}

	start := time.Now()
	health, err := checker.csiPVHandler.ControllerGetVolumeHealth(ctx, volumeHandle)
	checker.observeProbe(metrics.MethodGet, start, err)
	if err != nil {
		// Failed RPC is not a recovery; wait for the next list cycle.
		return err
	}
	return checker.reconcileAndTrack(ctx, pvc, volumeHandle, health.Conditions)
}

func (checker *PVHealthConditionChecker) GetVolumeHandle(pv *v1.PersistentVolume) (string, error) {
	if pv.Spec.CSI == nil {
		return "", fmt.Errorf("csi source is nil")
	}

	return pv.Spec.CSI.VolumeHandle, nil
}

// In Get mode an empty health report clears the stored conditions immediately; the Get
// RPC is authoritative.
func (checker *PVHealthConditionChecker) CheckControllerVolumeHealth(ctx context.Context, pv *v1.PersistentVolume) error {
	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != checker.driverName {
		return fmt.Errorf("csi source is nil or the volume is not managed by this checker/monitor")
	}

	if pv.Status.Phase != v1.VolumeBound {
		return fmt.Errorf("PV: %s status is not bound", pv.Name)
	}

	ctx, cancel := context.WithTimeout(ctx, checker.timeout)
	defer cancel()

	logger := klog.FromContext(ctx)
	volumeHandle, err := checker.GetVolumeHandle(pv)
	if err != nil {
		logger.Error(err, "Get volume handle error")
		return err
	}

	if len(volumeHandle) == 0 {
		return fmt.Errorf("volume handle in csi source is empty")
	}

	start := time.Now()
	health, err := checker.csiPVHandler.ControllerGetVolumeHealth(ctx, volumeHandle)
	checker.observeProbe(metrics.MethodGet, start, err)
	if err != nil {
		// Failed RPC is not a recovery.
		return err
	}

	pvc, err := checker.pvcLister.PersistentVolumeClaims(pv.Spec.ClaimRef.Namespace).Get(pv.Spec.ClaimRef.Name)
	if err != nil {
		return err
	}

	return checker.reconcileAndTrack(ctx, pvc, volumeHandle, health.Conditions)
}

// The driver's report is authoritative (overwrite, not merge), and a patch is issued only
// when it differs from PVC status or a same-resourceVersion patch not yet seen by the informer.
func (checker *PVHealthConditionChecker) reconcileAndTrack(ctx context.Context, pvc *v1.PersistentVolumeClaim, volumeHandle string, conditions []v1.VolumeHealthCondition) error {
	pvcKey := pvc.Namespace + "/" + pvc.Name
	desired := normalizeConditions(conditions)
	prev := checker.currentHealthConditions(pvcKey, pvc)

	if !conditionsEqual(prev, desired) {
		if err := checker.patchPVCHealthStatus(ctx, pvc, buildHealthStatus(desired)); err != nil {
			// Leave bookkeeping untouched so the next cycle retries the same transition.
			return err
		}
	}

	checker.recoveryStateMu.Lock()
	defer checker.recoveryStateMu.Unlock()
	if len(desired) > 0 {
		checker.knownUnhealthy[volumeHandle] = true
	} else {
		delete(checker.knownUnhealthy, volumeHandle)
	}

	if conditionsEqual(pvcHealthConditions(pvc), desired) {
		delete(checker.lastApplied, pvcKey)
	} else {
		checker.lastApplied[pvcKey] = appliedHealthStatus{
			resourceVersion: pvc.ResourceVersion,
			conditions:      desired,
		}
	}

	checker.updateVolumeHealthGauge(pvc.Namespace, pvc.Name, desired)
	return nil
}

func (checker *PVHealthConditionChecker) needsAbsentVolumeConfirmation(pvc *v1.PersistentVolumeClaim, volumeHandle string) bool {
	pvcKey := pvc.Namespace + "/" + pvc.Name

	checker.recoveryStateMu.Lock()
	if checker.knownUnhealthy[volumeHandle] {
		checker.recoveryStateMu.Unlock()
		return true
	}
	applied, ok := checker.lastApplied[pvcKey]
	checker.recoveryStateMu.Unlock()

	if ok && applied.resourceVersion == pvc.ResourceVersion {
		return len(applied.conditions) > 0
	}
	return len(pvcHealthConditions(pvc)) > 0
}

func (checker *PVHealthConditionChecker) currentHealthConditions(pvcKey string, pvc *v1.PersistentVolumeClaim) []v1.VolumeHealthCondition {
	current := pvcHealthConditions(pvc)

	checker.recoveryStateMu.Lock()
	defer checker.recoveryStateMu.Unlock()
	applied, ok := checker.lastApplied[pvcKey]
	if !ok {
		return current
	}
	if applied.resourceVersion != pvc.ResourceVersion {
		delete(checker.lastApplied, pvcKey)
		return current
	}
	return applied.conditions
}

// Safe when metrics is nil.
func (checker *PVHealthConditionChecker) observeProbe(method string, start time.Time, err error) {
	if checker.metrics == nil {
		return
	}
	checker.metrics.ObserveProbe(method, time.Since(start).Seconds(), err)
}

// One gauge series per (status, reason) while unhealthy; all removed on recovery. Safe when
// metrics is nil.
func (checker *PVHealthConditionChecker) updateVolumeHealthGauge(namespace, name string, desired []v1.VolumeHealthCondition) {
	if checker.metrics == nil {
		return
	}
	if len(desired) == 0 {
		checker.metrics.ClearVolumeHealth(namespace, name)
		return
	}
	pairs := make([][2]string, 0, len(desired))
	for _, c := range desired {
		pairs = append(pairs, [2]string{string(c.Status), c.Reason})
	}
	checker.metrics.SetVolumeHealth(namespace, name, pairs)
}
