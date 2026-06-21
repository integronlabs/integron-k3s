package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// IntegronAsyncAPISpec defines the desired state of an IntegronAsyncAPI.
//
// Exactly one of AsyncAPI or AsyncAPIConfigMapRef must be set. The document is
// an AsyncAPI 3 spec where each operation carries an x-integron-steps extension
// that declares the no-code orchestration pipeline executed for each message
// consumed from the operation's channel (Kafka topic).
//
// Unlike IntegronAPI (a request/response HTTP engine), an IntegronAsyncAPI is a
// consumer-only workload: the operator runs a long-lived Kafka consumer that
// subscribes to the spec's topics and executes the workflow per message. No
// Service or Ingress is created.
type IntegronAsyncAPISpec struct {
	// AsyncAPI is the inline AsyncAPI 3 document (YAML or JSON) for this API.
	// +optional
	AsyncAPI string `json:"asyncapi,omitempty"`

	// AsyncAPIConfigMapRef references an existing ConfigMap holding the spec.
	// Use this instead of AsyncAPI to manage the document out of band.
	// +optional
	AsyncAPIConfigMapRef *ConfigMapKeyRef `json:"asyncapiConfigMapRef,omitempty"`

	// Kafka configures the broker connection and consumer group.
	Kafka KafkaSpec `json:"kafka"`

	// Image is the integron async consumer container image to run.
	// +kubebuilder:default="ghcr.io/integronlabs/integron-k3s/async-engine:latest"
	// +optional
	Image string `json:"image,omitempty"`

	// ImagePullPolicy for the consumer container.
	// +kubebuilder:default="IfNotPresent"
	// +optional
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// Replicas is the number of consumer pods to run. Replicas join the same
	// consumer group, so Kafka spreads the spec's topic partitions across them;
	// scaling beyond the partition count leaves the extra pods idle.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=0
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// Resources are the compute resources for the consumer container.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

// KafkaSpec configures the Kafka connection for an async consumer.
type KafkaSpec struct {
	// Brokers is the list of bootstrap broker addresses (host:port).
	// +kubebuilder:validation:MinItems=1
	Brokers []string `json:"brokers"`

	// GroupID is the Kafka consumer group. Defaults to the IntegronAsyncAPI
	// name. All replicas share this group so partitions are balanced across
	// them.
	// +optional
	GroupID string `json:"groupID,omitempty"`

	// Topics restricts the topics the consumer subscribes to. When empty, the
	// consumer subscribes to every topic declared by the AsyncAPI document's
	// channels.
	// +optional
	Topics []string `json:"topics,omitempty"`

	// TLS enables and configures TLS for the broker connection.
	// +optional
	TLS *KafkaTLS `json:"tls,omitempty"`

	// SASL configures SASL authentication for the broker connection.
	// +optional
	SASL *KafkaSASL `json:"sasl,omitempty"`

	// MinBytes is the minimum number of bytes to fetch in a request. Higher
	// values reduce request volume at the cost of latency. Defaults to 1.
	// +optional
	MinBytes int32 `json:"minBytes,omitempty"`

	// MaxBytes is the maximum number of bytes to fetch in a request. Defaults
	// to 1048576 (1 MiB).
	// +optional
	MaxBytes int32 `json:"maxBytes,omitempty"`

	// BatchSize is the maximum number of messages handed to the workflow engine
	// in a single ProcessBatch call. Defaults to 100.
	// +kubebuilder:validation:Minimum=1
	// +optional
	BatchSize int32 `json:"batchSize,omitempty"`

	// MaxWaitMillis bounds how long the consumer waits to fill a batch before
	// processing what it has. Defaults to 1000 (1s).
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxWaitMillis int32 `json:"maxWaitMillis,omitempty"`
}

// KafkaTLS configures TLS for the broker connection.
type KafkaTLS struct {
	// Enabled turns on TLS for the broker connection.
	Enabled bool `json:"enabled"`

	// InsecureSkipVerify disables broker certificate verification. Do not use
	// in production.
	// +optional
	InsecureSkipVerify bool `json:"insecureSkipVerify,omitempty"`

	// CASecretRef references a Secret key holding a PEM CA bundle used to verify
	// the broker certificate.
	// +optional
	CASecretRef *SecretKeyRef `json:"caSecretRef,omitempty"`
}

// KafkaSASL configures SASL authentication for the broker connection.
type KafkaSASL struct {
	// Mechanism is the SASL mechanism: PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512.
	// +kubebuilder:validation:Enum=PLAIN;SCRAM-SHA-256;SCRAM-SHA-512
	Mechanism string `json:"mechanism"`

	// UsernameSecretRef references a Secret key holding the SASL username.
	UsernameSecretRef SecretKeyRef `json:"usernameSecretRef"`

	// PasswordSecretRef references a Secret key holding the SASL password.
	PasswordSecretRef SecretKeyRef `json:"passwordSecretRef"`
}

// SecretKeyRef points at a single key inside a Secret.
type SecretKeyRef struct {
	// Name of the Secret (in the same namespace as the IntegronAsyncAPI).
	Name string `json:"name"`

	// Key holding the value.
	Key string `json:"key"`
}

// IntegronAsyncAPIStatus is the observed state of an IntegronAsyncAPI.
type IntegronAsyncAPIStatus struct {
	// Conditions represent the latest available observations.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// ObservedGeneration is the generation last reconciled.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// ReadyReplicas is the number of consumer pods that are ready.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// Topics lists the resolved topics the consumer subscribes to.
	// +optional
	Topics []string `json:"topics,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=iasync
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Group",type=string,JSONPath=`.spec.kafka.groupID`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// IntegronAsyncAPI is a no-code, event-driven API: an AsyncAPI document whose
// operations consume Kafka messages and run x-integron-steps workflows.
type IntegronAsyncAPI struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   IntegronAsyncAPISpec   `json:"spec,omitempty"`
	Status IntegronAsyncAPIStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// IntegronAsyncAPIList contains a list of IntegronAsyncAPI.
type IntegronAsyncAPIList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []IntegronAsyncAPI `json:"items"`
}
