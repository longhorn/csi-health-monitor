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
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"

	"github.com/kubernetes-csi/external-health-monitor/pkg/metrics"
)

const eventReasonUnknownHealthCondition = "UnknownVolumeHealthCondition"

// PVHealthConditionChecker probes controller-side volume health and reconciles it onto
// pvc.status.healthStatus.
type PVHealthConditionChecker struct {
	driverName string

	timeout   time.Duration
	k8sClient kubernetes.Interface

	pvcLister corelisters.PersistentVolumeClaimLister
	pvLister  corelisters.PersistentVolumeLister

	csiHealthClient CSIHealthClient

	metrics *metrics.Metrics

	// eventRecorder is only used for unknown-type conditions, which cannot
	// be represented in pvc.status.healthStatus.
	eventRecorder record.EventRecorder

	// volumeStateMu guards volumes, which is mutated from the Get-mode workers
	// and the List-mode goroutine.
	volumeStateMu sync.Mutex
	// volumes holds reconciliation state per bound PVC (namespace/name). It
	// suppresses duplicate patches while the informer lags behind our own
	// write and flags absent volumes that need a recovery check. PVC status
	// remains the durable source of truth.
	volumes map[string]volumeState

	// fieldDropped tracks whether the API server is currently dropping
	// healthStatus writes (CSIVolumeHealth disabled), so the condition is
	// logged on transitions instead of on every write.
	fieldDropped atomic.Bool
}

// volumeState carries the in-process knowledge about one volume.
type volumeState struct {
	// unhealthy is true while the driver's latest report carried conditions.
	unhealthy bool
	// pending mirrors the last status write until the informer observes it.
	pending *pendingWrite
}

// pendingWrite only applies while the PVC's resourceVersion still equals the
// resourceVersion it was computed from.
type pendingWrite struct {
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
	eventRecorder record.EventRecorder,
) *PVHealthConditionChecker {
	return &PVHealthConditionChecker{
		driverName:      name,
		k8sClient:       kClient,
		pvcLister:       pvcLister,
		pvLister:        pvLister,
		timeout:         timeout,
		csiHealthClient: NewCSIHealthClient(conn, timeout),
		metrics:         healthMetrics,
		eventRecorder:   eventRecorder,
		volumes:         map[string]volumeState{},
	}
}

// A previously-unhealthy volume absent from a complete list cycle is resolved with
// ControllerGetVolumeHealth. The CSI spec requires Get when List is supported.
func (checker *PVHealthConditionChecker) CheckControllerListVolumeHealth(ctx context.Context) error {
	// A failed list RPC is not a recovery: leave stored conditions
	// untouched and try again next cycle.
	start := time.Now()
	result, err := checker.csiHealthClient.ControllerListVolumeHealth(ctx)
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
		if pv.Spec.ClaimRef == nil {
			logger.V(4).Info("Skipping bound PV without claimRef", "pv", pv.Name)
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
			if err := checker.reconcileAndTrack(ctx, pvc, health); err != nil {
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
	if !checker.needsAbsentVolumeConfirmation(pvc) {
		return nil
	}

	start := time.Now()
	health, err := checker.csiHealthClient.ControllerGetVolumeHealth(ctx, volumeHandle)
	checker.observeProbe(metrics.MethodGet, start, err)
	if err != nil {
		// Failed RPC is not a recovery; wait for the next list cycle.
		return err
	}
	return checker.reconcileAndTrack(ctx, pvc, health)
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

	if pv.Spec.ClaimRef == nil {
		return fmt.Errorf("PV %s is bound but has no claimRef", pv.Name)
	}

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
	health, err := checker.csiHealthClient.ControllerGetVolumeHealth(ctx, volumeHandle)
	checker.observeProbe(metrics.MethodGet, start, err)
	if err != nil {
		// Failed RPC is not a recovery.
		return err
	}

	pvc, err := checker.pvcLister.PersistentVolumeClaims(pv.Spec.ClaimRef.Namespace).Get(pv.Spec.ClaimRef.Name)
	if err != nil {
		return err
	}

	return checker.reconcileAndTrack(ctx, pvc, health)
}

// The driver's report is authoritative (overwrite, not merge), and a patch is issued only
// when it differs from PVC status or a same-resourceVersion patch not yet seen by the informer.
func (checker *PVHealthConditionChecker) reconcileAndTrack(ctx context.Context, pvc *v1.PersistentVolumeClaim, health *VolumeHealthResult) error {
	checker.surfaceUnknownConditions(ctx, pvc, health.Unknown)

	pvcKey := pvc.Namespace + "/" + pvc.Name
	desired := normalizeConditions(health.Conditions)
	prev := checker.currentHealthConditions(pvcKey, pvc)

	if !conditionsEqual(prev, desired) {
		persisted, err := checker.patchPVCHealthStatus(ctx, pvc, buildHealthStatus(desired))
		if err != nil {
			// Leave bookkeeping untouched so the next cycle retries the same transition.
			return err
		}
		if !persisted {
			// The API server dropped the field; treat like a failed write.
			return nil
		}
	}

	checker.volumeStateMu.Lock()
	state := volumeState{unhealthy: len(desired) > 0}
	if !conditionsEqual(pvcHealthConditions(pvc), desired) {
		// The informer has not observed the write yet; remember it until it does.
		state.pending = &pendingWrite{resourceVersion: pvc.ResourceVersion, conditions: desired}
	}
	if state.unhealthy || state.pending != nil {
		checker.volumes[pvcKey] = state
	} else {
		delete(checker.volumes, pvcKey)
	}
	checker.volumeStateMu.Unlock()

	checker.updateVolumeHealthGauge(pvc.Namespace, pvc.Name, desired)
	return nil
}

// ForgetPVC drops the state held for a PVC once it or its PV is deleted.
func (checker *PVHealthConditionChecker) ForgetPVC(namespace, name string) {
	checker.volumeStateMu.Lock()
	delete(checker.volumes, namespace+"/"+name)
	checker.volumeStateMu.Unlock()

	if checker.metrics != nil {
		checker.metrics.ClearVolumeHealth(namespace, name)
	}
}

func (checker *PVHealthConditionChecker) needsAbsentVolumeConfirmation(pvc *v1.PersistentVolumeClaim) bool {
	pvcKey := pvc.Namespace + "/" + pvc.Name

	checker.volumeStateMu.Lock()
	state, ok := checker.volumes[pvcKey]
	checker.volumeStateMu.Unlock()

	if ok && state.unhealthy {
		return true
	}
	if ok && state.pending != nil && state.pending.resourceVersion == pvc.ResourceVersion {
		return len(state.pending.conditions) > 0
	}
	return len(pvcHealthConditions(pvc)) > 0
}

func (checker *PVHealthConditionChecker) currentHealthConditions(pvcKey string, pvc *v1.PersistentVolumeClaim) []v1.VolumeHealthCondition {
	current := pvcHealthConditions(pvc)

	checker.volumeStateMu.Lock()
	defer checker.volumeStateMu.Unlock()
	state, ok := checker.volumes[pvcKey]
	if !ok || state.pending == nil {
		return current
	}
	if state.pending.resourceVersion != pvc.ResourceVersion {
		// The informer caught up; PVC status takes over as the baseline again.
		state.pending = nil
		if state.unhealthy {
			checker.volumes[pvcKey] = state
		} else {
			delete(checker.volumes, pvcKey)
		}
		return current
	}
	return state.pending.conditions
}

// Safe when metrics is nil.
func (checker *PVHealthConditionChecker) observeProbe(method string, start time.Time, err error) {
	if checker.metrics == nil {
		return
	}
	checker.metrics.ObserveProbe(method, time.Since(start).Seconds(), err)
}

// Safe when metrics is nil.
func (checker *PVHealthConditionChecker) recordDroppedStatusWrite() {
	if checker.metrics == nil {
		return
	}
	checker.metrics.RecordDroppedStatusWrite()
}

// The CSI spec forbids acting on unrecognized error types, and the status enum
// cannot represent them, so they are surfaced as a PVC warning event instead.
func (checker *PVHealthConditionChecker) surfaceUnknownConditions(ctx context.Context, pvc *v1.PersistentVolumeClaim, unknown []UnknownCondition) {
	if len(unknown) == 0 {
		return
	}
	details := unknownConditionsDetails(unknown)
	klog.FromContext(ctx).Info("CSI driver reported volume health conditions of a type unknown to this monitor", "pvc", klog.KObj(pvc), "conditions", details)
	if checker.metrics != nil {
		for _, u := range unknown {
			checker.metrics.RecordUnknownCondition(u.Status)
		}
	}
	if checker.eventRecorder == nil {
		return
	}
	checker.eventRecorder.Event(pvc, v1.EventTypeWarning, eventReasonUnknownHealthCondition,
		"The CSI driver reported volume health conditions of a type unknown to this monitor: "+details)
}

// Deterministic (sorted) so the event correlator can aggregate repeats.
func unknownConditionsDetails(unknown []UnknownCondition) string {
	entries := make([]string, 0, len(unknown))
	for _, u := range unknown {
		e := u.Status
		if u.Reason != "" {
			e += " (reason: " + u.Reason + ")"
		}
		if u.Message != "" {
			e += ": " + u.Message
		}
		entries = append(entries, e)
	}
	sort.Strings(entries)
	return strings.Join(entries, "; ")
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
