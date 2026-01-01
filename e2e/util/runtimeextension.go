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
	// autopilotPlanPrefix is the prefix for per-machine autopilot plan names.
	autopilotPlanPrefix = "autopilot-inplace-"

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
	MachineName       string
}

// WaitForAutopilotPlanCreated waits until an Autopilot plan is created for a specific machine.
func WaitForAutopilotPlanCreated(ctx context.Context, input WaitForAutopilotPlanCreatedInput, interval Interval) error {
	planName := autopilotPlanPrefix + input.MachineName

	return wait.PollUntilContextTimeout(ctx, interval.tick, interval.timeout, true, func(ctx context.Context) (bool, error) {
		_, err := input.WorkloadClientSet.RESTClient().Get().
			AbsPath("/apis/autopilot.k0sproject.io/v1beta2/plans/" + planName).
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
	MachineName       string
}

// WaitForAutopilotPlanCompleted waits until an Autopilot plan completes for a specific machine.
func WaitForAutopilotPlanCompleted(ctx context.Context, input WaitForAutopilotPlanCompletedInput, interval Interval) error {
	planName := autopilotPlanPrefix + input.MachineName

	return wait.PollUntilContextTimeout(ctx, interval.tick, interval.timeout, true, func(ctx context.Context) (bool, error) {
		state, err := GetAutopilotPlanState(ctx, input.WorkloadClientSet, planName)
		if err != nil {
			if apierrors.IsNotFound(err) {
				// Plan was deleted (cleanup happened) - consider as completed
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
				return false, fmt.Errorf("autopilot plan %s failed with state: %s", planName, state)
			}
			return false, nil
		}
	})
}

// WaitForAllAutopilotPlansCompletedInput specifies the input for WaitForAllAutopilotPlansCompleted.
type WaitForAllAutopilotPlansCompletedInput struct {
	WorkloadClientSet *kubernetes.Clientset
	MachineNames      []string
}

// WaitForAllAutopilotPlansCompleted waits until all Autopilot plans complete for a list of machines.
func WaitForAllAutopilotPlansCompleted(ctx context.Context, input WaitForAllAutopilotPlansCompletedInput, interval Interval) error {
	for _, machineName := range input.MachineNames {
		err := WaitForAutopilotPlanCompleted(ctx, WaitForAutopilotPlanCompletedInput{
			WorkloadClientSet: input.WorkloadClientSet,
			MachineName:       machineName,
		}, interval)
		if err != nil {
			return fmt.Errorf("failed waiting for machine %s: %w", machineName, err)
		}
		fmt.Printf("Autopilot plan completed for machine: %s\n", machineName)
	}
	return nil
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

// HasInPlaceAutopilotPlans checks if there are any in-place autopilot plans remaining.
func HasInPlaceAutopilotPlans(ctx context.Context, clientSet *kubernetes.Clientset) (bool, error) {
	plans, err := ListAutopilotPlans(ctx, clientSet)
	if err != nil {
		return false, err
	}

	for _, plan := range plans {
		if strings.HasPrefix(plan, autopilotPlanPrefix) {
			return true, nil
		}
	}
	return false, nil
}
