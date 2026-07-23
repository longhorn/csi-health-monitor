package mock

import (
	"context"
	"errors"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/kubernetes-csi/csi-lib-utils/connection"
	"github.com/kubernetes-csi/csi-lib-utils/metrics"
	"google.golang.org/grpc"
)

var (
	DefaultNS          = "test"
	DriverName         = "fake.csi.driver.io"
	DefaultKubeletPath = "/var/lib/kubelet"
	FSVolumeMode       = v1.PersistentVolumeBlock
	ErrorWatchTimeout  = errors.New("watch event timeout")
)

type CSIVolume struct {
	Volume  *csi.Volume
	Health  *csi.VolumeHealth
	IsBlock bool
}

func AbnormalVolumeHealth(volumeID string) *csi.VolumeHealth {
	return &csi.VolumeHealth{
		VolumeId: volumeID,
		HealthStatuses: []*csi.VolumeHealth_VolumeHealthEntry{
			{
				Status:  csi.VolumeHealthErrorType_INACCESSIBLE,
				Reason:  "VolumeNotFound",
				Message: "Volume not found",
			},
		},
	}
}

func HealthyVolumeHealth(volumeID string) *csi.VolumeHealth {
	return &csi.VolumeHealth{
		VolumeId:       volumeID,
		HealthStatuses: nil,
	}
}

type MockNode struct {
	NativeNode *v1.Node
}

type MockPod struct {
	NativePod *v1.Pod
}

type MockEvent struct {
	NativeEvent *v1.Event
}

type MockVolume struct {
	CSIVolume         *CSIVolume
	NativeVolume      *v1.PersistentVolume
	NativeVolumeClaim *v1.PersistentVolumeClaim
}

func FakeK8s() (kubernetes.Interface, informers.SharedInformerFactory) {
	client := fake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactory(client, 0)
	return client, informerFactory
}

func QuantityGB(i int) resource.Quantity {
	q := resource.NewQuantity(int64(i*1024*1024), resource.BinarySI)
	return *q
}

func New(ctx context.Context, address string) (*grpc.ClientConn, error) {
	metricsManager := metrics.NewCSIMetricsManager("fake.csi.driver.io" /* driverName */)
	conn, err := connection.Connect(ctx, address, metricsManager)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func createNode(name, namespace string) *v1.Node {
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}
}

func createPod(name, namespace, volumeName, pvcName, nodeName, uid string, pvcReadOnly bool) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(uid),
		},
		Spec: v1.PodSpec{
			NodeName: nodeName,
			Volumes: []v1.Volume{
				{
					Name: volumeName,
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvcName,
							ReadOnly:  pvcReadOnly,
						},
					},
				},
			},
		},
	}
}

func createPVC(requestGB, capacityGB int, name, uid, namespace, volumeName string, volumePhase v1.PersistentVolumeClaimPhase) *v1.PersistentVolumeClaim {
	request := QuantityGB(requestGB)
	capacity := QuantityGB(capacityGB)

	return &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(uid),
		},
		Spec: v1.PersistentVolumeClaimSpec{
			Resources: v1.VolumeResourceRequirements{
				Requests: map[v1.ResourceName]resource.Quantity{
					v1.ResourceStorage: request,
				},
			},
			VolumeName: volumeName,
		},
		Status: v1.PersistentVolumeClaimStatus{
			Phase: volumePhase,
			Capacity: map[v1.ResourceName]resource.Quantity{
				v1.ResourceStorage: capacity,
			},
		},
	}
}

func createPV(capacityGB int, pvcName, name, pvcNamespace string, pvcUID types.UID, volumeId string, volumePhase v1.PersistentVolumePhase, volumeMode *v1.PersistentVolumeMode) *v1.PersistentVolume {
	capacity := QuantityGB(capacityGB)

	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.PersistentVolumeSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Capacity: map[v1.ResourceName]resource.Quantity{
				v1.ResourceStorage: capacity,
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:       DriverName,
					VolumeHandle: volumeId,
				},
			},
			VolumeMode: volumeMode,
		},
		Status: v1.PersistentVolumeStatus{
			Phase: volumePhase,
		},
	}
	if len(pvcName) > 0 {
		pv.Spec.ClaimRef = &v1.ObjectReference{
			Namespace: pvcNamespace,
			Name:      pvcName,
			UID:       pvcUID,
		}
	}
	return pv
}

func createEvent(name, namespace, uid, eventType, eventReason string) *v1.Event {
	return &v1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		InvolvedObject: v1.ObjectReference{
			UID: types.UID(uid),
		},
		Type:   eventType,
		Reason: eventReason,
	}
}

func CreatePVWithoutCSIDriver(capacityGB int, pvcName, name, pvcNamespace string, pvcUID types.UID, volumeId string, volumePhase v1.PersistentVolumePhase, volumeMode *v1.PersistentVolumeMode) *v1.PersistentVolume {
	pv := createPV(capacityGB, pvcName, name, pvcNamespace, pvcUID, volumeId, volumePhase, volumeMode)
	pv.Spec.CSI = nil
	return pv
}

func CreatePVWithNilVolumeHandle(capacityGB int, pvcName, name, pvcNamespace string, pvcUID types.UID, volumeId string, volumePhase v1.PersistentVolumePhase, volumeMode *v1.PersistentVolumeMode) *v1.PersistentVolume {
	pv := createPV(capacityGB, pvcName, name, pvcNamespace, pvcUID, volumeId, volumePhase, volumeMode)
	pv.Spec.CSI.VolumeHandle = ""
	return pv
}

func CreatePVC(requestGB, capacityGB int, name, uid, namespace, volumeName string, volumePhase v1.PersistentVolumeClaimPhase) *v1.PersistentVolumeClaim {
	return createPVC(requestGB, capacityGB, name, uid, namespace, volumeName, volumePhase)
}

func CreatePV(capacityGB int, pvcName, name, pvcNamespace, volumeId string, pvcUID types.UID, volumeMode *v1.PersistentVolumeMode, volumePhase v1.PersistentVolumePhase) *v1.PersistentVolume {
	return createPV(capacityGB, pvcName, name, pvcNamespace, pvcUID, volumeId, volumePhase, volumeMode)
}

func CreateNode(name, namespace string) *v1.Node {
	return createNode(name, namespace)
}

func CreatePod(name, namespace, volumeName, pvcName, nodeName, uid string, pvcReadOnly bool) *v1.Pod {
	return createPod(name, namespace, volumeName, pvcName, nodeName, uid, pvcReadOnly)
}

func CreateEvent(name, namespace, uid, eventType, eventReason string) *v1.Event {
	return createEvent(name, namespace, uid, eventType, eventReason)
}

func WatchEvent(want bool, eventChan <-chan string) (string, error) {
	select {
	case event := <-eventChan:
		return event, nil
	case <-time.After(5 * time.Second):
		return "", ErrorWatchTimeout
	}
}
