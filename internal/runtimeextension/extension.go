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
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	runtimehooksv1 "sigs.k8s.io/cluster-api/api/runtime/hooks/v1alpha1"
	runtimecatalog "sigs.k8s.io/cluster-api/exp/runtime/catalog"
	runtimeserver "sigs.k8s.io/cluster-api/exp/runtime/server"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// DefaultPort is the default port for the runtime extension server.
	DefaultPort = 9444
)

// ExtensionHandler handles runtime extension requests for k0smotron in-place updates.
type ExtensionHandler struct {
	Client     client.Client
	ClientSet  *kubernetes.Clientset
	RESTConfig *rest.Config
}

// NewServer creates a new runtime extension server.
func NewServer(handler *ExtensionHandler, port int, certDir string) (*runtimeserver.Server, error) {
	catalog := runtimecatalog.New()
	if err := runtimehooksv1.AddToCatalog(catalog); err != nil {
		return nil, err
	}

	opts := runtimeserver.Options{
		Catalog: catalog,
		Port:    port,
	}
	if certDir != "" {
		opts.CertDir = certDir
	}

	srv, err := runtimeserver.New(opts)
	if err != nil {
		return nil, err
	}

	// Register CanUpdateMachine hook for control plane machines
	if err := srv.AddExtensionHandler(runtimeserver.ExtensionHandler{
		Hook:        runtimehooksv1.CanUpdateMachine,
		Name:        "k0smotron-can-update-machine",
		HandlerFunc: handler.CanUpdateMachine,
	}); err != nil {
		return nil, err
	}

	// Register CanUpdateMachineSet hook for worker machines
	if err := srv.AddExtensionHandler(runtimeserver.ExtensionHandler{
		Hook:        runtimehooksv1.CanUpdateMachineSet,
		Name:        "k0smotron-can-update-machine-set",
		HandlerFunc: handler.CanUpdateMachineSet,
	}); err != nil {
		return nil, err
	}

	// Register UpdateMachine hook for executing in-place updates
	if err := srv.AddExtensionHandler(runtimeserver.ExtensionHandler{
		Hook:        runtimehooksv1.UpdateMachine,
		Name:        "k0smotron-update-machine",
		HandlerFunc: handler.UpdateMachine,
	}); err != nil {
		return nil, err
	}

	return srv, nil
}
