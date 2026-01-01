//go:build e2e

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

package e2e

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/k0sproject/k0smotron/e2e/util"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
	capiframework "sigs.k8s.io/cluster-api/test/framework"
	"sigs.k8s.io/cluster-api/test/framework/clusterctl"
	capiutil "sigs.k8s.io/cluster-api/util"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestRuntimeExtension(t *testing.T) {
	setupAndRun(t, runtimeExtensionSpec)
}

// runtimeExtensionSpec tests the complete lifecycle of the runtime extension for in-place updates:
// 1. Verify ExtensionConfig is registered and handlers are discovered
// 2. Create a 3-node control plane cluster with InPlace update strategy
// 3. Wait for cluster and control plane readiness
// 4. Trigger a version upgrade on K0sControlPlane
// 5. Verify Autopilot plans are created for each machine
// 6. Verify Autopilot plans complete successfully
// 7. Verify plans are cleaned up
// 8. Verify version upgrade completes successfully
func runtimeExtensionSpec(t *testing.T) {
	testName := "runtime-extension"

	// ================================================
	// Step 1: Verify ExtensionConfig Registration
	// ================================================
	fmt.Println("Step 1: Verifying ExtensionConfig registration")

	// Wait for ExtensionConfig to be discovered with expected handlers
	// Handler names have suffix .<extension-config-name>
	err := util.WaitForExtensionConfigDiscovery(ctx, util.WaitForExtensionConfigDiscoveryInput{
		Getter:        bootstrapClusterProxy.GetClient(),
		ExtensionName: "k0smotron-inplace-update",
		ExpectedHandlers: []string{
			"k0smotron-can-update-machine.k0smotron-inplace-update",
			"k0smotron-can-update-machine-set.k0smotron-inplace-update",
			"k0smotron-update-machine.k0smotron-inplace-update",
		},
	}, util.GetInterval(e2eConfig, testName, "wait-controllers"))
	require.NoError(t, err, "ExtensionConfig should be registered with expected handlers")

	fmt.Println("ExtensionConfig registered successfully")

	// ================================================
	// Step 2: Setup Namespace and Create Cluster
	// ================================================
	fmt.Println("Step 2: Creating test namespace and cluster")

	namespace, _ := util.SetupSpecNamespace(ctx, testName, bootstrapClusterProxy, artifactFolder)

	clusterName := fmt.Sprintf("%s-%s", testName, capiutil.RandomString(6))

	workloadClusterTemplate := clusterctl.ConfigCluster(ctx, clusterctl.ConfigClusterInput{
		ClusterctlConfigPath:    clusterctlConfigPath,
		KubeconfigPath:          bootstrapClusterProxy.GetKubeconfigPath(),
		Flavor:                  "", // default flavor
		Namespace:               namespace.Name,
		ClusterName:             clusterName,
		KubernetesVersion:       e2eConfig.MustGetVariable(KubernetesVersion),
		ControlPlaneMachineCount: ptr.To[int64](3), // 3-node control plane
		InfrastructureProvider:  "docker",
		LogFolder:               filepath.Join(artifactFolder, "clusters", bootstrapClusterProxy.GetName()),
		ClusterctlVariables: map[string]string{
			"CLUSTER_NAME":    clusterName,
			"NAMESPACE":       namespace.Name,
			"UPDATE_STRATEGY": "InPlace",
		},
	})
	require.NotNil(t, workloadClusterTemplate)

	require.Eventually(t, func() bool {
		return bootstrapClusterProxy.CreateOrUpdate(ctx, workloadClusterTemplate) == nil
	}, 10*time.Second, 1*time.Second, "Failed to apply the cluster template")

	// ================================================
	// Step 3: Wait for Cluster and Control Plane
	// ================================================
	fmt.Println("Step 3: Waiting for cluster to provision")

	cluster, err := util.DiscoveryAndWaitForCluster(ctx, capiframework.DiscoveryAndWaitForClusterInput{
		Getter:    bootstrapClusterProxy.GetClient(),
		Namespace: namespace.Name,
		Name:      clusterName,
	}, util.GetInterval(e2eConfig, testName, "wait-cluster"))
	require.NoError(t, err)

	defer func() {
		util.DumpSpecResourcesAndCleanup(
			ctx,
			testName,
			bootstrapClusterProxy,
			artifactFolder,
			namespace,
			cancelWatches,
			cluster,
			util.GetInterval(e2eConfig, testName, "wait-delete-cluster"),
			skipCleanup,
			clusterctlConfigPath,
		)
	}()

	controlPlane, err := util.DiscoveryAndWaitForControlPlaneInitialized(ctx, capiframework.DiscoveryAndWaitForControlPlaneInitializedInput{
		Lister:  bootstrapClusterProxy.GetClient(),
		Cluster: cluster,
	}, util.GetInterval(e2eConfig, testName, "wait-controllers"))
	require.NoError(t, err)

	// Wait for all control plane replicas to be ready
	err = util.WaitForControlPlaneToBeReady(ctx, bootstrapClusterProxy.GetClient(), controlPlane, util.GetInterval(e2eConfig, testName, "wait-control-plane"))
	require.NoError(t, err)

	fmt.Println("Cluster and control plane are ready")

	// ================================================
	// Step 4: Get initial machine names and workload cluster client
	// ================================================
	fmt.Println("Step 4: Recording initial machine names")

	machineNames, err := util.GetControlPlaneMachineNames(ctx, util.GetControlPlaneMachineNamesInput{
		Lister:      bootstrapClusterProxy.GetClient(),
		Namespace:   namespace.Name,
		ClusterName: clusterName,
	})
	require.NoError(t, err)
	require.Len(t, machineNames, 3, "Should have 3 control plane machines")

	fmt.Printf("Initial machines: %v\n", machineNames)

	// Get workload cluster client for Autopilot verification
	workloadCluster := bootstrapClusterProxy.GetWorkloadCluster(ctx, namespace.Name, clusterName)
	workloadClientSet, err := kubernetes.NewForConfig(workloadCluster.GetRESTConfig())
	require.NoError(t, err, "Should get workload cluster clientset")

	// Verify no in-place Autopilot plans exist initially
	hasPlans, err := util.HasInPlaceAutopilotPlans(ctx, workloadClientSet)
	require.NoError(t, err)
	require.False(t, hasPlans, "No in-place Autopilot plans should exist initially")

	// ================================================
	// Step 5: Trigger Version Upgrade
	// ================================================
	fmt.Println("Step 5: Triggering version upgrade via K0sControlPlane")

	upgradeVersion := e2eConfig.MustGetVariable(KubernetesVersionFirstUpgradeTo)

	// Re-fetch controlPlane to get latest resource version (avoid conflict errors)
	err = bootstrapClusterProxy.GetClient().Get(ctx, client.ObjectKey{
		Name:      controlPlane.Name,
		Namespace: controlPlane.Namespace,
	}, controlPlane)
	require.NoError(t, err, "Failed to get K0sControlPlane")

	fmt.Printf("Upgrading from %s to %s\n", controlPlane.Spec.Version, upgradeVersion)

	// Update controlPlane version to trigger upgrade
	controlPlane.Spec.Version = upgradeVersion
	err = bootstrapClusterProxy.GetClient().Update(ctx, controlPlane)
	require.NoError(t, err, "Failed to update K0sControlPlane version")

	// ================================================
	// Step 6: Verify Autopilot Plans are Created
	// ================================================
	fmt.Println("Step 6: Verifying Autopilot plans are created")

	// Wait for Autopilot plans to be created for each machine
	for _, machineName := range machineNames {
		err := util.WaitForAutopilotPlanCreated(ctx, util.WaitForAutopilotPlanCreatedInput{
			WorkloadClientSet: workloadClientSet,
			MachineName:       machineName,
		}, util.GetInterval(e2eConfig, testName, "wait-autopilot-plan"))
		require.NoError(t, err, "Autopilot plan should be created for machine %s", machineName)
		fmt.Printf("Autopilot plan created for machine: %s\n", machineName)
	}

	// ================================================
	// Step 7: Wait for Autopilot Plans to Complete
	// ================================================
	fmt.Println("Step 7: Waiting for Autopilot plans to complete")

	err = util.WaitForAllAutopilotPlansCompleted(ctx, util.WaitForAllAutopilotPlansCompletedInput{
		WorkloadClientSet: workloadClientSet,
		MachineNames:      machineNames,
	}, util.GetInterval(e2eConfig, testName, "wait-kube-proxy-upgrade"))
	require.NoError(t, err, "All Autopilot plans should complete successfully")

	fmt.Println("All Autopilot plans completed successfully")

	// ================================================
	// Step 8: Verify Plans are Cleaned Up
	// ================================================
	fmt.Println("Step 8: Verifying Autopilot plans are cleaned up")

	// Wait for cleanup to occur
	require.Eventually(t, func() bool {
		hasPlans, err := util.HasInPlaceAutopilotPlans(ctx, workloadClientSet)
		if err != nil {
			return false
		}
		return !hasPlans
	}, 2*time.Minute, 10*time.Second, "Autopilot plans should be cleaned up")

	fmt.Println("Autopilot plans cleaned up")

	// ================================================
	// Step 9: Verify Control Plane Version Upgrade Completed
	// ================================================
	fmt.Println("Step 9: Verifying control plane version upgrade")

	err = util.WaitForControlPlaneToBeReady(ctx, bootstrapClusterProxy.GetClient(), controlPlane, util.GetInterval(e2eConfig, testName, "wait-control-plane"))
	require.NoError(t, err)

	// Verify the version is updated
	err = bootstrapClusterProxy.GetClient().Get(ctx, client.ObjectKey{
		Name:      controlPlane.Name,
		Namespace: controlPlane.Namespace,
	}, controlPlane)
	require.NoError(t, err)
	require.Equal(t, upgradeVersion, controlPlane.Status.Version, "Control plane version should be upgraded")

	fmt.Println("Version upgrade completed successfully!")

	// ================================================
	// Step 10: Verify kube-proxy version
	// ================================================
	fmt.Println("Step 10: Verifying kube-proxy version")

	err = util.WaitForKubeProxyUpgrade(ctx, util.WaitForKubeProxyUpgradeInput{
		Getter:            workloadCluster.GetClient(),
		KubernetesVersion: upgradeVersion,
	}, util.GetInterval(e2eConfig, testName, "wait-kube-proxy-upgrade"))
	require.NoError(t, err)

	fmt.Println("Runtime extension e2e test completed successfully!")
}
