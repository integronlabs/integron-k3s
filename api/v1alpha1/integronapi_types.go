package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// IntegronAPISpec defines the desired state of an IntegronAPI.
//
// Exactly one of OpenAPI or OpenAPIConfigMapRef must be set. The document is an
// OpenAPI 3 spec where each operation carries an x-integron-steps extension that
// declares the no-code orchestration pipeline executed for that endpoint.
type IntegronAPISpec struct {
	// OpenAPI is the inline OpenAPI 3 document (YAML or JSON) for this API.
	// +optional
	OpenAPI string `json:"openapi,omitempty"`

	// OpenAPIConfigMapRef references an existing ConfigMap holding the spec.
	// Use this instead of OpenAPI to manage the document out of band.
	// +optional
	OpenAPIConfigMapRef *ConfigMapKeyRef `json:"openapiConfigMapRef,omitempty"`

	// Image is the integron engine container image to run.
	// +kubebuilder:default="ghcr.io/integronlabs/integron-k3s/engine:latest"
	// +optional
	Image string `json:"image,omitempty"`

	// ImagePullPolicy for the engine container.
	// +kubebuilder:default="IfNotPresent"
	// +optional
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// Replicas is the number of engine pods to run.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// Ingress, when set, exposes the API through an Ingress resource.
	// +optional
	Ingress *IngressSpec `json:"ingress,omitempty"`

	// Resources are the compute resources for the engine container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

// ConfigMapKeyRef points at a single key inside a ConfigMap.
type ConfigMapKeyRef struct {
	// Name of the ConfigMap (in the same namespace as the IntegronAPI).
	Name string `json:"name"`

	// Key holding the OpenAPI document.
	// +kubebuilder:default="openapi.yaml"
	// +optional
	Key string `json:"key,omitempty"`
}

// IngressSpec configures optional Ingress exposure for the API.
type IngressSpec struct {
	// Host is the hostname to route to the API.
	Host string `json:"host"`

	// Path prefix for the API.
	// +kubebuilder:default="/"
	// +optional
	Path string `json:"path,omitempty"`

	// PathType for the Ingress rule.
	// +kubebuilder:default="Prefix"
	// +optional
	PathType string `json:"pathType,omitempty"`

	// ClassName is the IngressClass to use.
	// +optional
	ClassName string `json:"className,omitempty"`

	// Annotations to add to the generated Ingress.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// IntegronAPIStatus is the observed state of an IntegronAPI.
type IntegronAPIStatus struct {
	// Conditions represent the latest available observations.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ObservedGeneration is the generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// ReadyReplicas is the number of engine pods that are ready.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// URL is the external URL when an Ingress is configured.
	// +optional
	URL string `json:"url,omitempty"`
}

// ConditionReady is the condition type reporting overall readiness.
const ConditionReady = "Ready"

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=iapi
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="URL",type=string,JSONPath=`.status.url`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// IntegronAPI is a no-code REST API defined entirely by an OpenAPI document.
type IntegronAPI struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   IntegronAPISpec   `json:"spec,omitempty"`
	Status IntegronAPIStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// IntegronAPIList contains a list of IntegronAPI.
type IntegronAPIList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []IntegronAPI `json:"items"`
}
