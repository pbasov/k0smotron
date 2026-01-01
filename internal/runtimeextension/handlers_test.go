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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	runtimehooksv1 "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"

	bootstrapv1 "github.com/k0sproject/k0smotron/api/bootstrap/v1beta1"
	cpv1beta1 "github.com/k0sproject/k0smotron/api/controlplane/v1beta1"
)

func TestCanUpdateMachine_NonK0sMachine(t *testing.T) {
	handler := &ExtensionHandler{}

	req := &runtimehooksv1.CanUpdateMachineRequest{
		Current: runtimehooksv1.CanUpdateMachineRequestObjects{
			Machine: clusterv1.Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-machine",
					Namespace: "default",
				},
				Spec: clusterv1.MachineSpec{
					Version: "v1.28.0",
					Bootstrap: clusterv1.Bootstrap{
						ConfigRef: clusterv1.ContractVersionedObjectReference{
							Kind:     "KubeadmConfig",
							Name:     "test-config",
							APIGroup: "bootstrap.cluster.x-k8s.io",
						},
					},
				},
			},
		},
		Desired: runtimehooksv1.CanUpdateMachineRequestObjects{
			Machine: clusterv1.Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-machine",
					Namespace: "default",
				},
				Spec: clusterv1.MachineSpec{
					Version: "v1.29.0",
					Bootstrap: clusterv1.Bootstrap{
						ConfigRef: clusterv1.ContractVersionedObjectReference{
							Kind:     "KubeadmConfig",
							Name:     "test-config",
							APIGroup: "bootstrap.cluster.x-k8s.io",
						},
					},
				},
			},
		},
	}
	resp := &runtimehooksv1.CanUpdateMachineResponse{}

	handler.CanUpdateMachine(context.Background(), req, resp)

	assert.Equal(t, runtimehooksv1.ResponseStatusSuccess, resp.Status)
	assert.False(t, resp.MachinePatch.IsDefined(), "Should not return a patch for non-k0s machine")
}

func TestCanUpdateMachine_K0sControllerConfig_VersionChange(t *testing.T) {
	handler := &ExtensionHandler{}

	req := &runtimehooksv1.CanUpdateMachineRequest{
		Current: runtimehooksv1.CanUpdateMachineRequestObjects{
			Machine: clusterv1.Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-machine",
					Namespace: "default",
				},
				Spec: clusterv1.MachineSpec{
					Version: "v1.28.0+k0s.0",
					Bootstrap: clusterv1.Bootstrap{
						ConfigRef: clusterv1.ContractVersionedObjectReference{
							Kind:     "K0sControllerConfig",
							Name:     "test-config",
							APIGroup: "bootstrap.cluster.x-k8s.io",
						},
					},
				},
			},
		},
		Desired: runtimehooksv1.CanUpdateMachineRequestObjects{
			Machine: clusterv1.Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-machine",
					Namespace: "default",
				},
				Spec: clusterv1.MachineSpec{
					Version: "v1.29.0+k0s.0",
					Bootstrap: clusterv1.Bootstrap{
						ConfigRef: clusterv1.ContractVersionedObjectReference{
							Kind:     "K0sControllerConfig",
							Name:     "test-config",
							APIGroup: "bootstrap.cluster.x-k8s.io",
						},
					},
				},
			},
		},
	}
	resp := &runtimehooksv1.CanUpdateMachineResponse{}

	handler.CanUpdateMachine(context.Background(), req, resp)

	assert.Equal(t, runtimehooksv1.ResponseStatusSuccess, resp.Status)
	require.True(t, resp.MachinePatch.IsDefined(), "Should return a patch for version change")
	assert.Equal(t, runtimehooksv1.JSONMergePatchType, resp.MachinePatch.PatchType)

	// Verify the patch content
	var patch map[string]interface{}
	err := json.Unmarshal(resp.MachinePatch.Patch, &patch)
	require.NoError(t, err)

	spec, ok := patch["spec"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "v1.29.0+k0s.0", spec["version"])
}

func TestCanUpdateMachine_K0sWorkerConfig_VersionChange(t *testing.T) {
	handler := &ExtensionHandler{}

	req := &runtimehooksv1.CanUpdateMachineRequest{
		Current: runtimehooksv1.CanUpdateMachineRequestObjects{
			Machine: clusterv1.Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-worker",
					Namespace: "default",
				},
				Spec: clusterv1.MachineSpec{
					Version: "v1.28.0+k0s.0",
					Bootstrap: clusterv1.Bootstrap{
						ConfigRef: clusterv1.ContractVersionedObjectReference{
							Kind:     "K0sWorkerConfig",
							Name:     "test-config",
							APIGroup: "bootstrap.cluster.x-k8s.io",
						},
					},
				},
			},
		},
		Desired: runtimehooksv1.CanUpdateMachineRequestObjects{
			Machine: clusterv1.Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-worker",
					Namespace: "default",
				},
				Spec: clusterv1.MachineSpec{
					Version: "v1.29.0+k0s.0",
					Bootstrap: clusterv1.Bootstrap{
						ConfigRef: clusterv1.ContractVersionedObjectReference{
							Kind:     "K0sWorkerConfig",
							Name:     "test-config",
							APIGroup: "bootstrap.cluster.x-k8s.io",
						},
					},
				},
			},
		},
	}
	resp := &runtimehooksv1.CanUpdateMachineResponse{}

	handler.CanUpdateMachine(context.Background(), req, resp)

	assert.Equal(t, runtimehooksv1.ResponseStatusSuccess, resp.Status)
	require.True(t, resp.MachinePatch.IsDefined(), "Should return a patch for worker version change")
}

func TestCanUpdateMachine_NoVersionChange(t *testing.T) {
	handler := &ExtensionHandler{}

	req := &runtimehooksv1.CanUpdateMachineRequest{
		Current: runtimehooksv1.CanUpdateMachineRequestObjects{
			Machine: clusterv1.Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-machine",
					Namespace: "default",
				},
				Spec: clusterv1.MachineSpec{
					Version: "v1.28.0+k0s.0",
					Bootstrap: clusterv1.Bootstrap{
						ConfigRef: clusterv1.ContractVersionedObjectReference{
							Kind:     "K0sControllerConfig",
							Name:     "test-config",
							APIGroup: "bootstrap.cluster.x-k8s.io",
						},
					},
				},
			},
		},
		Desired: runtimehooksv1.CanUpdateMachineRequestObjects{
			Machine: clusterv1.Machine{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-machine",
					Namespace: "default",
				},
				Spec: clusterv1.MachineSpec{
					Version: "v1.28.0+k0s.0", // Same version
					Bootstrap: clusterv1.Bootstrap{
						ConfigRef: clusterv1.ContractVersionedObjectReference{
							Kind:     "K0sControllerConfig",
							Name:     "test-config",
							APIGroup: "bootstrap.cluster.x-k8s.io",
						},
					},
				},
			},
		},
	}
	resp := &runtimehooksv1.CanUpdateMachineResponse{}

	handler.CanUpdateMachine(context.Background(), req, resp)

	assert.Equal(t, runtimehooksv1.ResponseStatusSuccess, resp.Status)
	assert.False(t, resp.MachinePatch.IsDefined(), "Should not return a patch when version is unchanged")
}

func TestCanUpdateMachineSet_VersionChange(t *testing.T) {
	handler := &ExtensionHandler{}

	req := &runtimehooksv1.CanUpdateMachineSetRequest{
		Current: runtimehooksv1.CanUpdateMachineSetRequestObjects{
			MachineSet: clusterv1.MachineSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-machineset",
					Namespace: "default",
				},
				Spec: clusterv1.MachineSetSpec{
					Template: clusterv1.MachineTemplateSpec{
						Spec: clusterv1.MachineSpec{
							Version: "v1.28.0+k0s.0",
						},
					},
				},
			},
		},
		Desired: runtimehooksv1.CanUpdateMachineSetRequestObjects{
			MachineSet: clusterv1.MachineSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-machineset",
					Namespace: "default",
				},
				Spec: clusterv1.MachineSetSpec{
					Template: clusterv1.MachineTemplateSpec{
						Spec: clusterv1.MachineSpec{
							Version: "v1.29.0+k0s.0",
						},
					},
				},
			},
		},
	}
	resp := &runtimehooksv1.CanUpdateMachineSetResponse{}

	handler.CanUpdateMachineSet(context.Background(), req, resp)

	assert.Equal(t, runtimehooksv1.ResponseStatusSuccess, resp.Status)
	require.True(t, resp.MachineSetPatch.IsDefined(), "Should return a patch for MachineSet version change")
	assert.Equal(t, runtimehooksv1.JSONMergePatchType, resp.MachineSetPatch.PatchType)

	// Verify the patch content
	var patch map[string]interface{}
	err := json.Unmarshal(resp.MachineSetPatch.Patch, &patch)
	require.NoError(t, err)

	spec, ok := patch["spec"].(map[string]interface{})
	require.True(t, ok)
	template, ok := spec["template"].(map[string]interface{})
	require.True(t, ok)
	templateSpec, ok := template["spec"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "v1.29.0+k0s.0", templateSpec["version"])
}

func TestCanUpdateMachineSet_NoVersionChange(t *testing.T) {
	handler := &ExtensionHandler{}

	req := &runtimehooksv1.CanUpdateMachineSetRequest{
		Current: runtimehooksv1.CanUpdateMachineSetRequestObjects{
			MachineSet: clusterv1.MachineSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-machineset",
					Namespace: "default",
				},
				Spec: clusterv1.MachineSetSpec{
					Template: clusterv1.MachineTemplateSpec{
						Spec: clusterv1.MachineSpec{
							Version: "v1.28.0+k0s.0",
						},
					},
				},
			},
		},
		Desired: runtimehooksv1.CanUpdateMachineSetRequestObjects{
			MachineSet: clusterv1.MachineSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-machineset",
					Namespace: "default",
				},
				Spec: clusterv1.MachineSetSpec{
					Template: clusterv1.MachineTemplateSpec{
						Spec: clusterv1.MachineSpec{
							Version: "v1.28.0+k0s.0", // Same version
						},
					},
				},
			},
		},
	}
	resp := &runtimehooksv1.CanUpdateMachineSetResponse{}

	handler.CanUpdateMachineSet(context.Background(), req, resp)

	assert.Equal(t, runtimehooksv1.ResponseStatusSuccess, resp.Status)
	assert.False(t, resp.MachineSetPatch.IsDefined(), "Should not return a patch when version is unchanged")
}

func TestIsK0sMachine(t *testing.T) {
	tests := []struct {
		name     string
		kind     string
		expected bool
	}{
		{
			name:     "K0sControllerConfig",
			kind:     "K0sControllerConfig",
			expected: true,
		},
		{
			name:     "K0sWorkerConfig",
			kind:     "K0sWorkerConfig",
			expected: true,
		},
		{
			name:     "KubeadmConfig",
			kind:     "KubeadmConfig",
			expected: false,
		},
		{
			name:     "Empty kind",
			kind:     "",
			expected: false,
		},
	}

	handler := &ExtensionHandler{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			machine := &clusterv1.Machine{
				Spec: clusterv1.MachineSpec{
					Bootstrap: clusterv1.Bootstrap{
						ConfigRef: clusterv1.ContractVersionedObjectReference{
							Kind:     tt.kind,
							Name:     "test-config",
							APIGroup: "bootstrap.cluster.x-k8s.io",
						},
					},
				},
			}

			result := handler.isK0sMachine(context.Background(), machine)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetControllerConfigDownloadURL(t *testing.T) {
	tests := []struct {
		name           string
		downloadURL    string
		version        string
		expectedAmd64  string
		expectedArm64  string
		expectedArm    string
	}{
		{
			name:           "Default URLs",
			downloadURL:    "",
			version:        "v1.28.0+k0s.0",
			expectedAmd64:  "https://get.k0sproject.io/v1.28.0+k0s.0/k0s-v1.28.0+k0s.0-amd64",
			expectedArm64:  "https://get.k0sproject.io/v1.28.0+k0s.0/k0s-v1.28.0+k0s.0-arm64",
			expectedArm:    "https://get.k0sproject.io/v1.28.0+k0s.0/k0s-v1.28.0+k0s.0-arm",
		},
		{
			name:           "Custom download URL",
			downloadURL:    "https://custom.example.com/k0s",
			version:        "v1.28.0+k0s.0",
			expectedAmd64:  "https://custom.example.com/k0s",
			expectedArm64:  "https://custom.example.com/k0s",
			expectedArm:    "https://custom.example.com/k0s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kcp := &cpv1beta1.K0sControlPlane{
				Spec: cpv1beta1.K0sControlPlaneSpec{
					K0sConfigSpec: bootstrapv1.K0sConfigSpec{
						DownloadURL: tt.downloadURL,
					},
				},
			}

			amd64URL, arm64URL, armURL := getControllerConfigDownloadURL(kcp, tt.version)
			assert.Equal(t, tt.expectedAmd64, amd64URL)
			assert.Equal(t, tt.expectedArm64, arm64URL)
			assert.Equal(t, tt.expectedArm, armURL)
		})
	}
}
