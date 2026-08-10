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

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

func (ctrl *PVMonitorController) pvAdded(obj interface{}) {
	pv := obj.(*v1.PersistentVolume)
	if pv.Status.Phase != v1.VolumeBound || pv.Spec.CSI == nil || pv.Spec.CSI.Driver != ctrl.driverName {
		return
	}

	ctrl.enqueuePV(pv.Name)
}

func (ctrl *PVMonitorController) pvDeleted(obj interface{}) {
	pv, ok := obj.(*v1.PersistentVolume)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		pv, ok = tombstone.Obj.(*v1.PersistentVolume)
		if !ok {
			return
		}
	}
	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != ctrl.driverName {
		return
	}

	ctrl.forgetPV(pv.Name)
	ctrl.pvChecker.ClearForDeletedPV(context.Background(), pv)
}

func (ctrl *PVMonitorController) pvcDeleted(obj interface{}) {
	pvc, ok := obj.(*v1.PersistentVolumeClaim)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		pvc, ok = tombstone.Obj.(*v1.PersistentVolumeClaim)
		if !ok {
			return
		}
	}

	// Forgetting state for a PVC the checker never tracked is a no-op, so no
	// driver filtering is needed here.
	ctrl.pvChecker.ForgetPVC(pvc.Namespace, pvc.Name)
}
