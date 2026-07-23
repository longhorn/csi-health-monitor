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

package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"k8s.io/apiserver/pkg/server"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/component-base/featuregate"
	"k8s.io/component-base/logs"
	logsapi "k8s.io/component-base/logs/api/v1"
	_ "k8s.io/component-base/logs/json/register"
	"k8s.io/klog/v2"

	"github.com/container-storage-interface/spec/lib/go/csi"
	libconfig "github.com/kubernetes-csi/csi-lib-utils/config"
	"github.com/kubernetes-csi/csi-lib-utils/connection"
	"github.com/kubernetes-csi/csi-lib-utils/leaderelection"
	"github.com/kubernetes-csi/csi-lib-utils/metrics"
	"github.com/kubernetes-csi/csi-lib-utils/rpc"
	"github.com/kubernetes-csi/csi-lib-utils/standardflags"

	monitorcontroller "github.com/kubernetes-csi/external-health-monitor/pkg/controller"
	"github.com/kubernetes-csi/external-health-monitor/pkg/features"
	healthmetrics "github.com/kubernetes-csi/external-health-monitor/pkg/metrics"
)

const (

	// Default timeout of short CSI calls like GetPluginInfo
	csiTimeout = time.Second
)

// Command line flags
var (
	monitorInterval = flag.Duration("monitor-interval", 1*time.Minute, "Interval for controller to check volumes health condition.")

	resync                   = flag.Duration("resync", 10*time.Minute, "Resync interval of the controller.")
	timeout                  = flag.Duration("timeout", 15*time.Second, "Timeout of each call to the CSI driver and of each PVC status patch.")
	listVolumesInterval      = flag.Duration("list-volumes-interval", 5*time.Minute, "Time interval for calling ControllerListVolumeHealth RPC to check volumes' health condition")
	volumeListAndAddInterval = flag.Duration("volume-list-add-interval", 5*time.Minute, "Time interval for listing volumes and add them to queue")
	workerThreads            = flag.Uint("worker-threads", 10, "Number of pv monitor worker threads")
)

var (
	version = "unknown"
)

func main() {
	fg := featuregate.NewFeatureGate()
	logsapi.AddFeatureGates(fg)
	c := logsapi.NewLoggingConfiguration()
	logsapi.AddGoFlags(c, flag.CommandLine)
	logs.InitLogs()
	standardflags.RegisterCommonFlags(flag.CommandLine)
	standardflags.AddAutomaxprocs(klog.Infof)
	flag.Parse()
	logger := klog.Background()
	if err := logsapi.ValidateAndApply(c, fg); err != nil {
		logger.Error(err, "LoggingConfiguration is invalid")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	if standardflags.Configuration.ShowVersion {
		fmt.Println(os.Args[0], version)
		return
	}
	logger.Info("Version", "version", version)

	if standardflags.Configuration.MetricsAddress != "" && standardflags.Configuration.HttpEndpoint != "" {
		logger.Error(nil, "Only one of `--metrics-address` and `--http-endpoint` can be set.")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}
	addr := standardflags.Configuration.MetricsAddress
	if addr == "" {
		addr = standardflags.Configuration.HttpEndpoint
	}

	// Create the client config. Use kubeconfig if given, otherwise assume in-cluster.
	config, err := libconfig.BuildConfig(standardflags.Configuration.KubeConfig, standardflags.Configuration)
	if err != nil {
		logger.Error(err, "Failed to build a Kubernetes config")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	if *workerThreads == 0 {
		logger.Error(nil, "Option --worker-threads must be greater than zero")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		logger.Error(err, "Failed to create a Clientset")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	factory := informers.NewSharedInformerFactory(clientset, *resync)

	metricsManager := metrics.NewCSIMetricsManager("" /* driverName */)

	// Connect to CSI.
	ctx := context.Background()
	csiConn, err := connection.Connect(ctx, standardflags.Configuration.CSIAddress, metricsManager, connection.OnConnectionLoss(connection.ExitOnConnectionLoss()))
	if err != nil {
		logger.Error(err, "Failed to connect to the CSI driver")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	err = rpc.ProbeForever(ctx, csiConn, *timeout)
	if err != nil {
		logger.Error(err, "Failed to probe the CSI driver")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	// Find driver name.
	cancelationCtx, cancel := context.WithTimeout(context.Background(), csiTimeout)
	cancelationCtx = klog.NewContext(cancelationCtx, logger)
	defer cancel()
	storageDriver, err := rpc.GetDriverName(cancelationCtx, csiConn)
	if err != nil {
		logger.Error(err, "Failed to get the CSI driver name")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}
	logger.V(2).Info("CSI driver name", "driver", storageDriver)
	metricsManager.SetDriverName(storageDriver)

	healthMetrics := healthmetrics.New()
	healthMetrics.Register(metricsManager.GetRegistry())

	// Prepare HTTP endpoint for metrics + leader election healthz
	mux := http.NewServeMux()
	if addr != "" {
		metricsManager.RegisterToServer(mux, standardflags.Configuration.MetricsPath)
		go func() {
			logger.Info("ServeMux listening", "address", addr)
			err := http.ListenAndServe(addr, mux)
			if err != nil {
				logger.Error(err, "Failed to start HTTP server at specified address and metrics path", "address", addr, "path", standardflags.Configuration.MetricsPath)
				klog.FlushAndExit(klog.ExitFlushTimeout, 1)
			}
		}()
	}

	pluginCapabilities, err := rpc.GetPluginCapabilities(cancelationCtx, csiConn)
	if err != nil {
		logger.Error(err, "Failed to check whether the CSI driver supports the Plugin Controller Service")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}
	if !pluginCapabilities[csi.PluginCapability_Service_CONTROLLER_SERVICE] {
		logger.V(2).Info("CSI driver does not support Plugin Controller Service, exiting")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	caps, err := rpc.GetControllerCapabilities(cancelationCtx, csiConn)
	if err != nil {
		logger.Error(err, "Failed to get the CSI driver's controller capabilities")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	supportListVolumeHealth, err := healthMode(caps)
	if err != nil {
		logger.Error(err, "The CSI driver cannot be monitored")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	option := monitorcontroller.PVMonitorOptions{
		DriverName:              storageDriver,
		ContextTimeout:          *timeout,
		SupportListVolumeHealth: supportListVolumeHealth,

		ListVolumesInterval:      *listVolumesInterval,
		PVWorkerExecuteInterval:  *monitorInterval,
		VolumeListAndAddInterval: *volumeListAndAddInterval,
	}

	monitorController := monitorcontroller.NewPVMonitorController(
		clientset,
		csiConn,
		factory,
		healthMetrics,
		&option,
	)

	// handle SIGTERM and SIGINT by cancelling the context.

	var (
		terminate       func()          // called when all controllers are finished
		controllerCtx   context.Context // shuts down all controllers on a signal
		shutdownHandler <-chan struct{} // called when the signal is received
	)

	if utilfeature.DefaultFeatureGate.Enabled(features.ReleaseLeaderElectionOnExit) {
		// ctx waits for all controllers to finish, then shuts down the whole process, incl. leader election
		ctx, terminate = context.WithCancel(ctx)
		var cancelControllerCtx context.CancelFunc
		controllerCtx, cancelControllerCtx = context.WithCancel(ctx)
		shutdownHandler = server.SetupSignalHandler()

		defer terminate()

		go func() {
			defer cancelControllerCtx()
			<-shutdownHandler
			logger.Info("Received SIGTERM or SIGINT signal, shutting down controller.")
		}()
	}

	run := func(ctx context.Context) {
		if utilfeature.DefaultFeatureGate.Enabled(features.ReleaseLeaderElectionOnExit) {
			var wg sync.WaitGroup
			factory.Start(ctx.Done())
			monitorController.Run(controllerCtx, int(*workerThreads), &wg)
			<-controllerCtx.Done()
			wg.Wait()
			terminate()
		} else {
			factory.Start(ctx.Done())
			monitorController.Run(ctx, int(*workerThreads), nil)
		}
	}
	leaderelection.RunWithLeaderElection(
		ctx,
		config,
		standardflags.Configuration,
		run,
		"external-health-monitor-leader-"+storageDriver,
		mux,
		utilfeature.DefaultFeatureGate.Enabled(features.ReleaseLeaderElectionOnExit),
	)
}

// healthMode decides how the driver is monitored based on its advertised
// controller capabilities, returning whether ControllerListVolumeHealth is
// used over per-volume ControllerGetVolumeHealth calls.
func healthMode(caps rpc.ControllerCapabilitySet) (useListVolumeHealth bool, err error) {
	supportsList := caps[csi.ControllerServiceCapability_RPC_LIST_VOLUME_HEALTH]
	supportsGet := caps[csi.ControllerServiceCapability_RPC_GET_VOLUME_HEALTH]

	if !supportsList && !supportsGet {
		return false, fmt.Errorf("the CSI driver supports neither ControllerListVolumeHealth nor ControllerGetVolumeHealth")
	}
	if supportsList && !supportsGet {
		return false, fmt.Errorf("the CSI driver supports ControllerListVolumeHealth without ControllerGetVolumeHealth, which the CSI specification requires alongside it")
	}
	return supportsList, nil
}
