// Package v1alpha1 contains the API types for the integron.integronlabs.io
// API group. An IntegronAPI is a fully declarative, no-code REST API: its
// OpenAPI document (with x-integron-steps) is the entire definition, and the
// operator turns it into a running integron engine on k3s/Kubernetes.
// +kubebuilder:object:generate=true
// +groupName=integron.integronlabs.io
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

// GroupVersion is the group/version for this API.
var GroupVersion = schema.GroupVersion{Group: "integron.integronlabs.io", Version: "v1alpha1"}

// SchemeBuilder registers the API types with a runtime.Scheme.
var SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

// AddToScheme adds the types in this group-version to the given scheme.
var AddToScheme = SchemeBuilder.AddToScheme

func init() {
	SchemeBuilder.Register(&IntegronAPI{}, &IntegronAPIList{})
	SchemeBuilder.Register(&IntegronAsyncAPI{}, &IntegronAsyncAPIList{})
}
