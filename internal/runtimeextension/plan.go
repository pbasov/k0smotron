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
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/controller-runtime/pkg/log"

	autopilot "github.com/k0sproject/k0s/pkg/apis/autopilot/v1beta2"
	"github.com/k0sproject/k0s/pkg/autopilot/controller/plans/core"
	cpv1beta1 "github.com/k0sproject/k0smotron/api/controlplane/v1beta1"
)

// PlanState represents the state of an Autopilot plan.
type PlanState string

const (
	PlanStateNotFound       PlanState = "NotFound"
	PlanStateSchedulable    PlanState = "Schedulable"
	PlanStateSchedulableWait PlanState = "SchedulableWait"
	PlanStateCompleted      PlanState = "Completed"
	PlanStateFailed         PlanState = "Failed"
)

// getPlanStateAndTarget retrieves the current state of the "autopilot" plan and
// extracts the target machine name from the plan's id field (format: "id-{machineName}-{timestamp}").
func (h *ExtensionHandler) getPlanStateAndTarget(ctx context.Context, kubeClient *kubernetes.Clientset) (PlanState, string, error) {
	logger := log.FromContext(ctx).WithValues("plan", autopilotPlanName)

	result, err := kubeClient.RESTClient().Get().
		AbsPath("/apis/autopilot.k0sproject.io/v1beta2/plans/" + autopilotPlanName).
		DoRaw(ctx)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return PlanStateNotFound, "", nil
		}
		return "", "", err
	}

	var plan autopilot.Plan
	if err := yaml.Unmarshal(result, &plan); err != nil {
		return "", "", fmt.Errorf("failed to unmarshal plan: %w", err)
	}

	logger.Info("Plan state", "state", plan.Status.State, "id", plan.Spec.ID)

	// Extract machine name from plan ID (format: "id-{machineName}-{timestamp}")
	targetMachine := extractMachineNameFromPlanID(plan.Spec.ID)

	var state PlanState
	switch plan.Status.State {
	case core.PlanCompleted:
		state = PlanStateCompleted
	case core.PlanSchedulable:
		state = PlanStateSchedulable
	case core.PlanSchedulableWait:
		state = PlanStateSchedulableWait
	case core.PlanIncompleteTargets, core.PlanInconsistentTargets, core.PlanRestricted, core.PlanApplyFailed, core.PlanMissingSignalNode, core.PlanWarning:
		state = PlanStateFailed
	default:
		// Unknown state, treat as in progress
		state = PlanStateSchedulable
	}

	return state, targetMachine, nil
}

// extractMachineNameFromPlanID extracts the machine name from a plan ID.
// Plan ID format: "id-{machineName}-{timestamp}"
func extractMachineNameFromPlanID(planID string) string {
	// Remove "id-" prefix
	if len(planID) <= 3 || planID[:3] != "id-" {
		return ""
	}
	remainder := planID[3:]

	// Find the last dash (before timestamp)
	lastDash := -1
	for i := len(remainder) - 1; i >= 0; i-- {
		if remainder[i] == '-' {
			lastDash = i
			break
		}
	}

	if lastDash <= 0 {
		return ""
	}

	return remainder[:lastDash]
}

// createAutopilotPlan creates an Autopilot plan for in-place update.
// Note: Autopilot only processes plans named exactly "autopilot".
// This function creates a plan targeting a single machine at a time.
func (h *ExtensionHandler) createAutopilotPlan(ctx context.Context, kubeClient *kubernetes.Clientset, machine *clusterv1.Machine, kcp *cpv1beta1.K0sControlPlane) error {
	logger := log.FromContext(ctx).WithValues("machine", machine.Name)

	// Autopilot requires the plan to be named "autopilot"
	planName := autopilotPlanName
	version := machine.Spec.Version

	// Get download URLs
	amd64URL, arm64URL, armURL := getControllerConfigDownloadURL(kcp, version)

	timestamp := fmt.Sprintf("%d", time.Now().Unix())

	// Determine if this is a controller or worker node
	// Controllers are machines created by K0sControlPlane (have control-plane label)
	isController := false
	if _, ok := machine.Labels[clusterv1.MachineControlPlaneLabel]; ok {
		isController = true
	}

	var targetsSection map[string]interface{}
	if isController {
		targetsSection = map[string]interface{}{
			"controllers": map[string]interface{}{
				"discovery": map[string]interface{}{
					"static": map[string]interface{}{
						"nodes": []string{machine.Name},
					},
				},
			},
		}
	} else {
		targetsSection = map[string]interface{}{
			"workers": map[string]interface{}{
				"discovery": map[string]interface{}{
					"static": map[string]interface{}{
						"nodes": []string{machine.Name},
					},
				},
			},
		}
	}

	plan := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "autopilot.k0sproject.io/v1beta2",
			"kind":       "Plan",
			"metadata": map[string]interface{}{
				"name": planName,
			},
			"spec": map[string]interface{}{
				"id":        fmt.Sprintf("id-%s-%s", machine.Name, timestamp),
				"timestamp": timestamp,
				"commands": []interface{}{
					map[string]interface{}{
						"k0supdate": map[string]interface{}{
							"version": version,
							"platforms": map[string]interface{}{
								"linux-amd64": map[string]interface{}{
									"url": amd64URL,
								},
								"linux-arm64": map[string]interface{}{
									"url": arm64URL,
								},
								"linux-arm": map[string]interface{}{
									"url": armURL,
								},
							},
							"targets": targetsSection,
						},
					},
				},
			},
		},
	}

	planBytes, err := plan.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal plan: %w", err)
	}

	logger.Info("Creating Autopilot plan", "version", version)

	return kubeClient.RESTClient().Post().
		AbsPath("/apis/autopilot.k0sproject.io/v1beta2/plans").
		Body(planBytes).
		Do(ctx).
		Error()
}

// deletePlan deletes an Autopilot plan.
func (h *ExtensionHandler) deletePlan(ctx context.Context, kubeClient *kubernetes.Clientset, planName string) error {
	logger := log.FromContext(ctx).WithValues("plan", planName)
	logger.Info("Deleting Autopilot plan")

	err := kubeClient.RESTClient().Delete().
		AbsPath("/apis/autopilot.k0sproject.io/v1beta2/plans/" + planName).
		Do(ctx).
		Error()
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}
