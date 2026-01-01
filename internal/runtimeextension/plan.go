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

// getPlanState retrieves the current state of an Autopilot plan.
func (h *ExtensionHandler) getPlanState(ctx context.Context, kubeClient *kubernetes.Clientset, planName string) (PlanState, error) {
	logger := log.FromContext(ctx).WithValues("plan", planName)

	result, err := kubeClient.RESTClient().Get().
		AbsPath("/apis/autopilot.k0sproject.io/v1beta2/plans/" + planName).
		DoRaw(ctx)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return PlanStateNotFound, nil
		}
		return "", err
	}

	var plan autopilot.Plan
	if err := yaml.Unmarshal(result, &plan); err != nil {
		return "", fmt.Errorf("failed to unmarshal plan: %w", err)
	}

	logger.Info("Plan state", "state", plan.Status.State)

	switch plan.Status.State {
	case core.PlanCompleted:
		return PlanStateCompleted, nil
	case core.PlanSchedulable:
		return PlanStateSchedulable, nil
	case core.PlanSchedulableWait:
		return PlanStateSchedulableWait, nil
	case core.PlanIncompleteTargets, core.PlanInconsistentTargets, core.PlanRestricted, core.PlanApplyFailed, core.PlanMissingSignalNode, core.PlanWarning:
		return PlanStateFailed, nil
	default:
		// Unknown state, treat as in progress
		return PlanStateSchedulable, nil
	}
}

// createPerMachinePlan creates a per-machine Autopilot plan for in-place update.
func (h *ExtensionHandler) createPerMachinePlan(ctx context.Context, kubeClient *kubernetes.Clientset, machine *clusterv1.Machine, kcp *cpv1beta1.K0sControlPlane) error {
	logger := log.FromContext(ctx).WithValues("machine", machine.Name)

	planName := planNamePrefix + machine.Name
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
