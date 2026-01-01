/*
Copyright 2025.

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

package runtimeextension

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimehooksv1 "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cpv1beta1 "github.com/k0sproject/k0smotron/api/controlplane/v1beta1"
	"github.com/k0sproject/k0smotron/internal/controller/util"
)

const (
	// planNamePrefix is the prefix for per-machine autopilot plan names.
	planNamePrefix = "autopilot-inplace-"

	// defaultRetryAfterSeconds is the default retry interval for in-progress updates.
	defaultRetryAfterSeconds = 30
)

// CanUpdateMachine determines if the extension can handle specific machine changes for in-place updates.
// For k0smotron, we can handle version changes via Autopilot.
func (h *ExtensionHandler) CanUpdateMachine(ctx context.Context, req *runtimehooksv1.CanUpdateMachineRequest, resp *runtimehooksv1.CanUpdateMachineResponse) {
	logger := log.FromContext(ctx).WithValues("machine", req.Current.Machine.Name)
	logger.Info("CanUpdateMachine called")

	resp.SetStatus(runtimehooksv1.ResponseStatusSuccess)

	// Check if this is a k0s machine by looking at the bootstrap config
	if !h.isK0sMachine(ctx, &req.Current.Machine) {
		logger.Info("Not a k0s machine, skipping")
		return
	}

	// Check if only version changed
	currentVersion := req.Current.Machine.Spec.Version
	desiredVersion := req.Desired.Machine.Spec.Version

	if currentVersion == desiredVersion {
		logger.Info("Version unchanged, nothing to patch")
		return
	}

	logger.Info("Version change detected", "current", currentVersion, "desired", desiredVersion)

	// Create a JSONMergePatch for the Machine spec.version field
	patch := map[string]interface{}{
		"spec": map[string]interface{}{
			"version": desiredVersion,
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		logger.Error(err, "Failed to marshal patch")
		resp.SetStatus(runtimehooksv1.ResponseStatusFailure)
		resp.SetMessage(fmt.Sprintf("failed to marshal patch: %v", err))
		return
	}

	resp.MachinePatch = runtimehooksv1.Patch{
		PatchType: runtimehooksv1.JSONMergePatchType,
		Patch:     patchBytes,
	}

	logger.Info("Machine version patch created")
}

// CanUpdateMachineSet determines if the extension can handle specific MachineSet changes for in-place updates.
// For k0smotron worker nodes, we can handle version changes via Autopilot.
func (h *ExtensionHandler) CanUpdateMachineSet(ctx context.Context, req *runtimehooksv1.CanUpdateMachineSetRequest, resp *runtimehooksv1.CanUpdateMachineSetResponse) {
	logger := log.FromContext(ctx).WithValues("machineSet", req.Current.MachineSet.Name)
	logger.Info("CanUpdateMachineSet called")

	resp.SetStatus(runtimehooksv1.ResponseStatusSuccess)

	// Check if only version changed in the MachineSet spec.template.spec
	currentVersion := req.Current.MachineSet.Spec.Template.Spec.Version
	desiredVersion := req.Desired.MachineSet.Spec.Template.Spec.Version

	if currentVersion == desiredVersion {
		logger.Info("Version unchanged, nothing to patch")
		return
	}

	logger.Info("Version change detected", "current", currentVersion, "desired", desiredVersion)

	// Create a JSONMergePatch for the MachineSet spec.template.spec.version field
	patch := map[string]interface{}{
		"spec": map[string]interface{}{
			"template": map[string]interface{}{
				"spec": map[string]interface{}{
					"version": desiredVersion,
				},
			},
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		logger.Error(err, "Failed to marshal patch")
		resp.SetStatus(runtimehooksv1.ResponseStatusFailure)
		resp.SetMessage(fmt.Sprintf("failed to marshal patch: %v", err))
		return
	}

	resp.MachineSetPatch = runtimehooksv1.Patch{
		PatchType: runtimehooksv1.JSONMergePatchType,
		Patch:     patchBytes,
	}

	logger.Info("MachineSet version patch created")
}

// UpdateMachine performs the actual in-place update on a machine using Autopilot.
func (h *ExtensionHandler) UpdateMachine(ctx context.Context, req *runtimehooksv1.UpdateMachineRequest, resp *runtimehooksv1.UpdateMachineResponse) {
	logger := log.FromContext(ctx).WithValues("machine", req.Desired.Machine.Name)
	logger.Info("UpdateMachine called")

	machine := &req.Desired.Machine

	// Get the cluster for this machine
	cluster, err := h.getClusterForMachine(ctx, machine)
	if err != nil {
		logger.Error(err, "Failed to get cluster for machine")
		resp.SetStatus(runtimehooksv1.ResponseStatusFailure)
		resp.SetMessage(fmt.Sprintf("failed to get cluster: %v", err))
		return
	}

	// Get the K0sControlPlane for version and download URL info
	kcp, err := h.getK0sControlPlane(ctx, cluster)
	if err != nil {
		logger.Error(err, "Failed to get K0sControlPlane")
		resp.SetStatus(runtimehooksv1.ResponseStatusFailure)
		resp.SetMessage(fmt.Sprintf("failed to get K0sControlPlane: %v", err))
		return
	}

	// Get the kube client for the child cluster
	kubeClient, err := util.GetKubeClient(ctx, h.Client, cluster)
	if err != nil {
		logger.Error(err, "Failed to get kube client for child cluster")
		resp.SetStatus(runtimehooksv1.ResponseStatusFailure)
		resp.SetMessage(fmt.Sprintf("failed to get kube client: %v", err))
		return
	}

	planName := planNamePrefix + machine.Name

	// Check if plan already exists
	planState, err := h.getPlanState(ctx, kubeClient, planName)
	if err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "Failed to get plan state")
		resp.SetStatus(runtimehooksv1.ResponseStatusFailure)
		resp.SetMessage(fmt.Sprintf("failed to get plan state: %v", err))
		return
	}

	switch planState {
	case PlanStateCompleted:
		// Plan completed successfully, delete the plan and signal completion
		logger.Info("Autopilot plan completed successfully")
		if err := h.deletePlan(ctx, kubeClient, planName); err != nil {
			logger.Error(err, "Failed to delete completed plan")
		}
		resp.SetStatus(runtimehooksv1.ResponseStatusSuccess)
		resp.SetRetryAfterSeconds(0) // Done
		return

	case PlanStateSchedulable, PlanStateSchedulableWait:
		// Plan is in progress, retry later
		logger.Info("Autopilot plan in progress", "state", planState)
		resp.SetStatus(runtimehooksv1.ResponseStatusSuccess)
		resp.SetRetryAfterSeconds(defaultRetryAfterSeconds)
		return

	case PlanStateFailed:
		// Plan failed, delete and retry
		logger.Info("Autopilot plan failed, will retry")
		if err := h.deletePlan(ctx, kubeClient, planName); err != nil {
			logger.Error(err, "Failed to delete failed plan")
		}
		// Fall through to create a new plan

	case PlanStateNotFound:
		// Plan doesn't exist, create one
		logger.Info("Creating new Autopilot plan")
	}

	// Create the per-machine Autopilot plan
	if err := h.createPerMachinePlan(ctx, kubeClient, machine, kcp); err != nil {
		logger.Error(err, "Failed to create Autopilot plan")
		resp.SetStatus(runtimehooksv1.ResponseStatusFailure)
		resp.SetMessage(fmt.Sprintf("failed to create autopilot plan: %v", err))
		return
	}

	logger.Info("Autopilot plan created, will retry to check status")
	resp.SetStatus(runtimehooksv1.ResponseStatusSuccess)
	resp.SetRetryAfterSeconds(defaultRetryAfterSeconds)
}

// isK0sMachine checks if the machine is managed by k0smotron.
func (h *ExtensionHandler) isK0sMachine(ctx context.Context, machine *clusterv1.Machine) bool {
	// Check if the bootstrap config ref points to a k0s config
	// k0s bootstrap configs have kind K0sControllerConfig or K0sWorkerConfig
	kind := machine.Spec.Bootstrap.ConfigRef.Kind
	return kind == "K0sControllerConfig" || kind == "K0sWorkerConfig"
}

// getClusterForMachine retrieves the Cluster object for a given Machine.
func (h *ExtensionHandler) getClusterForMachine(ctx context.Context, machine *clusterv1.Machine) (*clusterv1.Cluster, error) {
	clusterName, ok := machine.Labels[clusterv1.ClusterNameLabel]
	if !ok {
		return nil, fmt.Errorf("machine %s does not have cluster name label", machine.Name)
	}

	cluster := &clusterv1.Cluster{}
	if err := h.Client.Get(ctx, types.NamespacedName{
		Name:      clusterName,
		Namespace: machine.Namespace,
	}, cluster); err != nil {
		return nil, err
	}

	return cluster, nil
}

// getK0sControlPlane retrieves the K0sControlPlane for a cluster.
func (h *ExtensionHandler) getK0sControlPlane(ctx context.Context, cluster *clusterv1.Cluster) (*cpv1beta1.K0sControlPlane, error) {
	// Check if it's a K0sControlPlane
	if cluster.Spec.ControlPlaneRef.Kind != "K0sControlPlane" {
		return nil, fmt.Errorf("cluster %s control plane is not K0sControlPlane (kind: %s)", cluster.Name, cluster.Spec.ControlPlaneRef.Kind)
	}

	kcp := &cpv1beta1.K0sControlPlane{}
	if err := h.Client.Get(ctx, types.NamespacedName{
		Name:      cluster.Spec.ControlPlaneRef.Name,
		Namespace: cluster.Namespace,
	}, kcp); err != nil {
		return nil, err
	}

	return kcp, nil
}

// getControllerConfigDownloadURL retrieves the download URL from the K0sControlPlane.
func getControllerConfigDownloadURL(kcp *cpv1beta1.K0sControlPlane, version string) (amd64URL, arm64URL, armURL string) {
	if kcp.Spec.K0sConfigSpec.DownloadURL != "" {
		return kcp.Spec.K0sConfigSpec.DownloadURL, kcp.Spec.K0sConfigSpec.DownloadURL, kcp.Spec.K0sConfigSpec.DownloadURL
	}
	amd64URL = fmt.Sprintf("https://get.k0sproject.io/%s/k0s-%s-amd64", version, version)
	arm64URL = fmt.Sprintf("https://get.k0sproject.io/%s/k0s-%s-arm64", version, version)
	armURL = fmt.Sprintf("https://get.k0sproject.io/%s/k0s-%s-arm", version, version)
	return
}

// getChildClusterSecret retrieves the kubeconfig secret for the child cluster.
func (h *ExtensionHandler) getChildClusterSecret(ctx context.Context, cluster *clusterv1.Cluster) (*corev1.Secret, error) {
	secret := &corev1.Secret{}
	secretName := fmt.Sprintf("%s-kubeconfig", cluster.Name)
	if err := h.Client.Get(ctx, types.NamespacedName{
		Name:      secretName,
		Namespace: cluster.Namespace,
	}, secret); err != nil {
		return nil, err
	}
	return secret, nil
}
