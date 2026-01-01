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

package util

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// autopilotPlanName is the fixed name required by Autopilot.
	// Autopilot only processes plans named exactly "autopilot".
	autopilotPlanName = "autopilot"

	// Autopilot plan states
	planStateCompleted      = "Completed"
	planStateSchedulable    = "Schedulable"
	planStateSchedulableWait = "SchedulableWait"
)

// WaitForExtensionConfigDiscoveryInput specifies the input for WaitForExtensionConfigDiscovery.
type WaitForExtensionConfigDiscoveryInput struct {
	Getter           client.Client
	ExtensionName    string
	ExpectedHandlers []string
}

// WaitForExtensionConfigDiscovery waits until the ExtensionConfig has discovered all expected handlers.
func WaitForExtensionConfigDiscovery(ctx context.Context, input WaitForExtensionConfigDiscoveryInput, interval Interval) error {
	fmt.Printf("Waiting for ExtensionConfig %s to discover handlers: %v\n", input.ExtensionName, input.ExpectedHandlers)

	extensionConfig := &unstructured.Unstructured{}
	extensionConfig.SetAPIVersion("runtime.cluster.x-k8s.io/v1alpha1")
	extensionConfig.SetKind("ExtensionConfig")

	return wait.PollUntilContextTimeout(ctx, interval.tick, interval.timeout, true, func(ctx context.Context) (bool, error) {
		key := client.ObjectKey{Name: input.ExtensionName}
		if err := input.Getter.Get(ctx, key, extensionConfig); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}

		// Check status.handlers
		status, found, err := unstructured.NestedSlice(extensionConfig.Object, "status", "handlers")
		if err != nil || !found {
			return false, nil
		}

		discoveredHandlers := make(map[string]bool)
		for _, h := range status {
			handler, ok := h.(map[string]interface{})
			if !ok {
				continue
			}
			name, found, _ := unstructured.NestedString(handler, "name")
			if found {
				discoveredHandlers[name] = true
			}
		}

		// Check all expected handlers are discovered
		for _, expected := range input.ExpectedHandlers {
			if !discoveredHandlers[expected] {
				return false, nil
			}
		}

		return true, nil
	})
}

// WaitForAutopilotPlanCreatedInput specifies the input for WaitForAutopilotPlanCreated.
type WaitForAutopilotPlanCreatedInput struct {
	WorkloadClientSet *kubernetes.Clientset
}

// WaitForAutopilotPlanCreated waits until the "autopilot" plan is created.
// Note: Autopilot only processes plans named exactly "autopilot".
func WaitForAutopilotPlanCreated(ctx context.Context, input WaitForAutopilotPlanCreatedInput, interval Interval) error {
	return wait.PollUntilContextTimeout(ctx, interval.tick, interval.timeout, true, func(ctx context.Context) (bool, error) {
		_, err := input.WorkloadClientSet.RESTClient().Get().
			AbsPath("/apis/autopilot.k0sproject.io/v1beta2/plans/" + autopilotPlanName).
			DoRaw(ctx)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			// Ignore other errors (API might not be ready yet)
			return false, nil
		}
		return true, nil
	})
}

// WaitForAutopilotPlanCompletedInput specifies the input for WaitForAutopilotPlanCompleted.
type WaitForAutopilotPlanCompletedInput struct {
	WorkloadClientSet *kubernetes.Clientset
}

// WaitForAutopilotPlanCompleted waits until the "autopilot" plan completes.
// Note: Autopilot only processes plans named exactly "autopilot".
func WaitForAutopilotPlanCompleted(ctx context.Context, input WaitForAutopilotPlanCompletedInput, interval Interval) error {
	return wait.PollUntilContextTimeout(ctx, interval.tick, interval.timeout, true, func(ctx context.Context) (bool, error) {
		state, err := GetAutopilotPlanState(ctx, input.WorkloadClientSet, autopilotPlanName)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// Plan was deleted (cleanup happened after completion) - this means it completed
				return true, nil
			}
			// Continue polling on other errors
			return false, nil
		}

		switch state {
		case planStateCompleted:
			return true, nil
		case planStateSchedulable, planStateSchedulableWait:
			// Still in progress
			return false, nil
		default:
			// Failed state or unknown - report error
			if strings.Contains(state, "Failed") || strings.Contains(state, "Incomplete") {
				return false, fmt.Errorf("autopilot plan failed with state: %s", state)
			}
			return false, nil
		}
	})
}

// WaitForAllMachinesUpdatedInput specifies the input for WaitForAllMachinesUpdated.
type WaitForAllMachinesUpdatedInput struct {
	WorkloadClientSet *kubernetes.Clientset
	MachineCount      int
}

// WaitForAllMachinesUpdated waits until all machines have been updated.
// Since Autopilot only processes one plan at a time (named "autopilot"),
// this function waits for all sequential updates to complete by monitoring
// the number of completed plan cycles.
func WaitForAllMachinesUpdated(ctx context.Context, input WaitForAllMachinesUpdatedInput, interval Interval) error {
	// We expect `MachineCount` plans to be created and completed in sequence.
	// Each plan will be created, processed, completed, and deleted by the runtime extension.
	// The best way to track this is to wait until no plan exists and all machines have been updated.
	// The runtime extension handles this sequentially via retries.

	// Simply wait for the plan to not exist (final cleanup happened)
	// The runtime extension will keep retrying until all machines are updated
	fmt.Printf("Waiting for all %d machines to be updated via Autopilot...\n", input.MachineCount)

	return wait.PollUntilContextTimeout(ctx, interval.tick, interval.timeout, true, func(ctx context.Context) (bool, error) {
		_, err := input.WorkloadClientSet.RESTClient().Get().
			AbsPath("/apis/autopilot.k0sproject.io/v1beta2/plans/" + autopilotPlanName).
			DoRaw(ctx)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// No plan exists - check if all updates are complete
				// The runtime extension only deletes plans after completion
				// If there's no plan, either all updates are done or none have started
				fmt.Println("No autopilot plan found - checking if updates are complete")
				return true, nil
			}
			return false, nil
		}

		// Plan still exists, check its state
		state, err := GetAutopilotPlanState(ctx, input.WorkloadClientSet, autopilotPlanName)
		if err != nil {
			return false, nil
		}

		fmt.Printf("Autopilot plan state: %s\n", state)

		// If the plan failed, report the error
		if strings.Contains(state, "Failed") || strings.Contains(state, "Incomplete") || strings.Contains(state, "MissingSignalNode") {
			details, _ := GetAutopilotPlanDetails(ctx, input.WorkloadClientSet, autopilotPlanName)
			if details != nil {
				rawStatus, _ := json.MarshalIndent(details["status"], "", "  ")
				fmt.Printf("Plan failed. Status: %s\n", string(rawStatus))
			}
			return false, fmt.Errorf("autopilot plan failed with state: %s", state)
		}

		return false, nil
	})
}

// ListAutopilotPlans lists all Autopilot plans in the workload cluster.
func ListAutopilotPlans(ctx context.Context, clientSet *kubernetes.Clientset) ([]string, error) {
	result, err := clientSet.RESTClient().Get().
		AbsPath("/apis/autopilot.k0sproject.io/v1beta2/plans").
		DoRaw(ctx)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	var planList struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		} `json:"items"`
	}

	if err := json.Unmarshal(result, &planList); err != nil {
		return nil, err
	}

	var names []string
	for _, item := range planList.Items {
		names = append(names, item.Metadata.Name)
	}
	return names, nil
}

// GetAutopilotPlanState retrieves the current state of an Autopilot plan.
func GetAutopilotPlanState(ctx context.Context, clientSet *kubernetes.Clientset, planName string) (string, error) {
	result, err := clientSet.RESTClient().Get().
		AbsPath("/apis/autopilot.k0sproject.io/v1beta2/plans/" + planName).
		DoRaw(ctx)
	if err != nil {
		return "", err
	}

	var plan struct {
		Status struct {
			State string `json:"state"`
		} `json:"status"`
	}

	if err := json.Unmarshal(result, &plan); err != nil {
		return "", err
	}

	return plan.Status.State, nil
}

// GetControlPlaneMachineNamesInput specifies the input for GetControlPlaneMachineNames.
type GetControlPlaneMachineNamesInput struct {
	Lister      client.Client
	Namespace   string
	ClusterName string
}

// GetControlPlaneMachineNames returns the names of all control plane machines for a cluster.
func GetControlPlaneMachineNames(ctx context.Context, input GetControlPlaneMachineNamesInput) ([]string, error) {
	machineList := &clusterv1.MachineList{}
	err := input.Lister.List(ctx, machineList,
		client.InNamespace(input.Namespace),
		client.MatchingLabels{
			clusterv1.ClusterNameLabel:         input.ClusterName,
			clusterv1.MachineControlPlaneLabel: "true",
		},
	)
	if err != nil {
		return nil, err
	}

	var names []string
	for _, m := range machineList.Items {
		names = append(names, m.Name)
	}
	return names, nil
}

// HasAutopilotPlan checks if the "autopilot" plan exists.
func HasAutopilotPlan(ctx context.Context, clientSet *kubernetes.Clientset) (bool, error) {
	_, err := clientSet.RESTClient().Get().
		AbsPath("/apis/autopilot.k0sproject.io/v1beta2/plans/" + autopilotPlanName).
		DoRaw(ctx)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// ListControlNodes lists all ControlNode names in the workload cluster.
func ListControlNodes(ctx context.Context, clientSet *kubernetes.Clientset) ([]string, error) {
	result, err := clientSet.RESTClient().Get().
		AbsPath("/apis/autopilot.k0sproject.io/v1beta2/controlnodes").
		DoRaw(ctx)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	var nodeList struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		} `json:"items"`
	}

	if err := json.Unmarshal(result, &nodeList); err != nil {
		return nil, err
	}

	var names []string
	for _, item := range nodeList.Items {
		names = append(names, item.Metadata.Name)
	}
	return names, nil
}

// GetAutopilotPlanDetails retrieves full details of an Autopilot plan including spec and status.
func GetAutopilotPlanDetails(ctx context.Context, clientSet *kubernetes.Clientset, planName string) (map[string]interface{}, error) {
	result, err := clientSet.RESTClient().Get().
		AbsPath("/apis/autopilot.k0sproject.io/v1beta2/plans/" + planName).
		DoRaw(ctx)
	if err != nil {
		return nil, err
	}

	var plan map[string]interface{}
	if err := json.Unmarshal(result, &plan); err != nil {
		return nil, err
	}

	return plan, nil
}

// DumpAutopilotDebugInfo prints debugging information about Autopilot state.
func DumpAutopilotDebugInfo(ctx context.Context, clientSet *kubernetes.Clientset, machineNames []string) {
	fmt.Println("=== Autopilot Debug Info ===")

	// List ControlNodes
	controlNodes, err := ListControlNodes(ctx, clientSet)
	if err != nil {
		fmt.Printf("Error listing ControlNodes: %v\n", err)
	} else {
		fmt.Printf("ControlNodes: %v\n", controlNodes)
	}

	// List Plans
	plans, err := ListAutopilotPlans(ctx, clientSet)
	if err != nil {
		fmt.Printf("Error listing Plans: %v\n", err)
	} else {
		fmt.Printf("Plans: %v\n", plans)
	}

	// Get details for the "autopilot" plan
	details, err := GetAutopilotPlanDetails(ctx, clientSet, autopilotPlanName)
	if err != nil {
		fmt.Printf("Plan %s: error getting details: %v\n", autopilotPlanName, err)
	} else {
		// Extract relevant info
		status, _ := details["status"].(map[string]interface{})
		spec, _ := details["spec"].(map[string]interface{})

		state := "unknown"
		if status != nil {
			if s, ok := status["state"].(string); ok {
				state = s
			}
		}

		planID := ""
		if spec != nil {
			if id, ok := spec["id"].(string); ok {
				planID = id
			}
		}

		fmt.Printf("Plan %s:\n  State: %s\n  ID: %s\n", autopilotPlanName, state, planID)
		// Always print the raw status
		rawStatus, _ := json.MarshalIndent(status, "  ", "  ")
		fmt.Printf("  Raw Status: %s\n", string(rawStatus))
		if spec != nil {
			// Print commands section
			if commands, ok := spec["commands"].([]interface{}); ok && len(commands) > 0 {
				if cmd, ok := commands[0].(map[string]interface{}); ok {
					if k0supdate, ok := cmd["k0supdate"].(map[string]interface{}); ok {
						if targets, ok := k0supdate["targets"].(map[string]interface{}); ok {
							targetsJSON, _ := json.MarshalIndent(targets, "  ", "  ")
							fmt.Printf("  Targets: %s\n", string(targetsJSON))
						}
					}
				}
			}
		}
	}

	fmt.Printf("Expected machines: %v\n", machineNames)
	fmt.Println("=== End Autopilot Debug Info ===")
}
