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

package pv_monitor_controller

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"

	v1 "k8s.io/api/core/v1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	handler "github.com/kubernetes-csi/external-health-monitor/pkg/csi-handler"
	"github.com/kubernetes-csi/external-health-monitor/pkg/metrics"
)

// PVMonitorController is the struct of pv monitor controller containing all information to perform volumes health condition checking
type PVMonitorController struct {
	driverName string

	supportListVolumeHealth bool

	pvChecker *handler.PVHealthConditionChecker

	pvLister       corelisters.PersistentVolumeLister
	pvListerSynced cache.InformerSynced

	pvcLister       corelisters.PersistentVolumeClaimLister
	pvcListerSynced cache.InformerSynced

	// used for updating the pvEnqueued map
	sync.Mutex
	// pvEnqueued stores all CSI PVs which are enqueued
	pvEnqueued map[string]bool
	// we get PVs from pvQueue to check their health conditions
	pvQueue workqueue.Interface

	// Time interval for calling ControllerListVolumeHealth RPC to check volumes' health condition
	ListVolumesInterval time.Duration
	// Time interval for executing pv worker goroutines
	PVWorkerExecuteInterval time.Duration
	// Time interval for listing volumes and add them to queue
	VolumeListAndAddInterval time.Duration
}

// PVMonitorOptions configures PV monitor
type PVMonitorOptions struct {
	ContextTimeout          time.Duration
	DriverName              string
	SupportListVolumeHealth bool

	ListVolumesInterval      time.Duration
	PVWorkerExecuteInterval  time.Duration
	VolumeListAndAddInterval time.Duration
}

// NewPVMonitorController creates PV monitor controller
func NewPVMonitorController(
	client kubernetes.Interface,
	conn *grpc.ClientConn,
	factory informers.SharedInformerFactory,
	healthMetrics *metrics.Metrics,
	option *PVMonitorOptions,
) *PVMonitorController {
	ctrl := &PVMonitorController{
		supportListVolumeHealth: option.SupportListVolumeHealth,
		driverName:              option.DriverName,
		pvQueue:                 workqueue.NewNamed("csi-monitor-pv-queue"),

		pvEnqueued: make(map[string]bool),

		ListVolumesInterval:      option.ListVolumesInterval,
		PVWorkerExecuteInterval:  option.PVWorkerExecuteInterval,
		VolumeListAndAddInterval: option.VolumeListAndAddInterval,
	}
	ctrl.setupPVInformer(factory)
	ctrl.setupPVCInformer(factory)
	ctrl.setupPVChecker(factory, client, conn, healthMetrics, option)
	return ctrl
}

func (ctrl *PVMonitorController) setupPVInformer(factory informers.SharedInformerFactory) {
	informer := factory.Core().V1().PersistentVolumes()
	informer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: ctrl.pvAdded,
		// we do not care about PV changes, so do not need UpdateFunc here.
		// deleted PVs will not be readded to the queue, so do not need DeleteFunc here
	})
	ctrl.pvLister = informer.Lister()
	ctrl.pvListerSynced = informer.Informer().HasSynced
}

func (ctrl *PVMonitorController) setupPVCInformer(factory informers.SharedInformerFactory) {
	informer := factory.Core().V1().PersistentVolumeClaims()
	ctrl.pvcLister = informer.Lister()
	ctrl.pvcListerSynced = informer.Informer().HasSynced
}

func (ctrl *PVMonitorController) setupPVChecker(
	factory informers.SharedInformerFactory,
	client kubernetes.Interface,
	conn *grpc.ClientConn,
	healthMetrics *metrics.Metrics,
	option *PVMonitorOptions,
) {
	ctrl.pvChecker = handler.NewPVHealthConditionChecker(
		option.DriverName,
		conn,
		client,
		option.ContextTimeout,
		ctrl.pvcLister,
		ctrl.pvLister,
		healthMetrics,
	)
}

// Run runs the volume health condition checking method
func (ctrl *PVMonitorController) Run(ctx context.Context, workers int, wg *sync.WaitGroup) {
	defer ctrl.pvQueue.ShutDown()

	logger := klog.FromContext(ctx)
	logger.Info("Starting CSI External PV Health Monitor Controller")
	defer logger.Info("Shutting down CSI External PV Health Monitor Controller")

	if !waitForCacheSyncSucceed(ctx, ctrl) {
		logger.Error(nil, "Cannot sync cache")
		return
	}

	// if the driver supports ControllerListVolumeHealth, it is preferred for performance reasons
	if ctrl.supportListVolumeHealth {
		goTrack(wg, func() {
			wait.UntilWithContext(ctx, ctrl.checkPVsHealthConditionByListVolumeHealth, ctrl.ListVolumesInterval)
		})
		<-ctx.Done()
		return
	}

	for i := 0; i < workers; i++ {
		goTrack(wg, func() {
			wait.UntilWithContext(ctx, ctrl.checkPVWorker, ctrl.PVWorkerExecuteInterval)
		})
	}
	goTrack(wg, func() {
		wait.UntilWithContext(ctx, func(ctx context.Context) {
			logger := klog.FromContext(ctx)
			err := ctrl.AddPVsToQueue()
			if err != nil {
				logger.Error(err, "Failed to reconcile volumes")
			}
		}, ctrl.VolumeListAndAddInterval)
	})

	<-ctx.Done()
}

func goTrack(wg *sync.WaitGroup, f func()) {
	if wg != nil {
		wg.Go(f)
	} else {
		go f()
	}
}

func waitForCacheSyncSucceed(ctx context.Context, ctrl *PVMonitorController) bool {
	return cache.WaitForCacheSync(ctx.Done(), ctrl.pvListerSynced, ctrl.pvcListerSynced)
}

func (ctrl *PVMonitorController) checkPVsHealthConditionByListVolumeHealth(ctx context.Context) {
	logger := klog.FromContext(ctx)
	err := ctrl.pvChecker.CheckControllerListVolumeHealth(ctx)
	if err != nil {
		logger.Error(err, "Check controller volume health error")
	}
}

// AddPVsToQueue adds PVs to queue periodically
func (ctrl *PVMonitorController) AddPVsToQueue() error {
	pvs, err := ctrl.pvLister.List(labels.Everything())
	if err != nil {
		return err
	}

	for _, pv := range pvs {
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != ctrl.driverName {
			continue
		}
		if pv.Status.Phase != v1.VolumeBound || pv.DeletionTimestamp != nil {
			continue
		}
		ctrl.Lock()
		if !ctrl.pvEnqueued[pv.Name] {
			ctrl.pvEnqueued[pv.Name] = true
			ctrl.pvQueue.Add(pv.Name)
		}
		ctrl.Unlock()
	}

	return nil
}

// forgetPV takes a PV out of the monitoring loop so that a later
// AddPVsToQueue pass can pick it up again, e.g. once a pending PV
// becomes bound.
func (ctrl *PVMonitorController) forgetPV(pvName string) {
	ctrl.Lock()
	delete(ctrl.pvEnqueued, pvName)
	ctrl.Unlock()
}

func (ctrl *PVMonitorController) checkPVWorker(ctx context.Context) {
	key, quit := ctrl.pvQueue.Get()
	if quit {
		return
	}
	defer ctrl.pvQueue.Done(key)

	logger := klog.FromContext(ctx)
	pvName := key.(string)
	logger.V(4).Info("Started PV processing", "pv", pvName)

	pv, err := ctrl.pvLister.Get(pvName)
	if err != nil {
		if apierrs.IsNotFound(err) {
			// PV was deleted in the meantime, ignore.
			ctrl.forgetPV(pvName)
			logger.V(3).Info("PV deleted, ignoring", "pv", pvName)
			return
		}
		logger.Error(err, "Error getting PersistentVolume", "pv", pvName)
		ctrl.pvQueue.Add(pvName)
		return
	}

	if pv.DeletionTimestamp != nil {
		logger.Info("PV is being deleted now, skip checking health condition", "pv", pv.Name)
		ctrl.forgetPV(pvName)
		return
	}

	if pv.Status.Phase != v1.VolumeBound {
		logger.Info("PV status is not bound, remove it from the queue", "pv", pv.Name)
		ctrl.forgetPV(pvName)
		return
	}

	err = ctrl.pvChecker.CheckControllerVolumeHealth(ctx, pv)
	if err != nil {
		logger.Error(err, "Check controller volume health error")
	}

	// re-enqueue anyway
	ctrl.pvQueue.Add(pvName)
}
