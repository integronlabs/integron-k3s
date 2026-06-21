package controller

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/integronlabs/integron-async/asyncapi"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	integronv1alpha1 "github.com/integronlabs/integron-k3s/api/v1alpha1"
)

const (
	// asyncSpecMountPath is the consumer working directory; the consumer reads
	// the AsyncAPI document from <asyncSpecMountPath>/asyncapi.yaml.
	asyncSpecMountPath = "/app/docs"
	asyncSpecFileName  = "asyncapi.yaml"

	// caMountPath is where a TLS CA bundle is mounted when configured.
	caMountPath = "/app/secrets"
	caFileName  = "ca.pem"

	defaultAsyncImage = "ghcr.io/integronlabs/integron-k3s/async-engine:latest"
)

// IntegronAsyncAPIReconciler reconciles an IntegronAsyncAPI into a running
// Kafka consumer Deployment.
type IntegronAsyncAPIReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=integron.integronlabs.io,resources=integronasyncapis,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=integron.integronlabs.io,resources=integronasyncapis/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=integron.integronlabs.io,resources=integronasyncapis/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps;secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=create;update;patch;delete

// Reconcile drives cluster state toward the IntegronAsyncAPI spec.
func (r *IntegronAsyncAPIReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var api integronv1alpha1.IntegronAsyncAPI
	if err := r.Get(ctx, req.NamespacedName, &api); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	cmName, specHash, topics, err := r.reconcileSpec(ctx, &api)
	if err != nil {
		return r.failAsync(ctx, &api, "SpecError", err)
	}

	if err := r.reconcileDeployment(ctx, &api, cmName, specHash, topics); err != nil {
		return r.failAsync(ctx, &api, "DeploymentError", err)
	}

	return r.succeedAsync(ctx, &api, topics)
}

// reconcileSpec materializes the AsyncAPI document into an owned ConfigMap and
// returns its name, a content hash and the resolved topic list.
func (r *IntegronAsyncAPIReconciler) reconcileSpec(ctx context.Context, api *integronv1alpha1.IntegronAsyncAPI) (string, string, []string, error) {
	content, err := r.resolveAsyncSpecContent(ctx, api)
	if err != nil {
		return "", "", nil, err
	}

	topics, err := resolveTopics(content, api.Spec.Kafka.Topics)
	if err != nil {
		return "", "", nil, err
	}

	cmName := api.Name + "-asyncapi"
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: cmName, Namespace: api.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = labelsForAsync(api)
		cm.Data = map[string]string{asyncSpecFileName: content}
		return controllerutil.SetControllerReference(api, cm, r.Scheme)
	}); err != nil {
		return "", "", nil, fmt.Errorf("reconciling spec ConfigMap: %w", err)
	}
	return cmName, hashString(content), topics, nil
}

// resolveAsyncSpecContent returns the AsyncAPI document from inline spec or a ref.
func (r *IntegronAsyncAPIReconciler) resolveAsyncSpecContent(ctx context.Context, api *integronv1alpha1.IntegronAsyncAPI) (string, error) {
	if ref := api.Spec.AsyncAPIConfigMapRef; ref != nil {
		key := ref.Key
		if key == "" {
			key = asyncSpecFileName
		}
		var cm corev1.ConfigMap
		if err := r.Get(ctx, types.NamespacedName{Namespace: api.Namespace, Name: ref.Name}, &cm); err != nil {
			return "", fmt.Errorf("reading referenced ConfigMap %q: %w", ref.Name, err)
		}
		content, ok := cm.Data[key]
		if !ok {
			return "", fmt.Errorf("ConfigMap %q has no key %q", ref.Name, key)
		}
		return content, nil
	}
	if api.Spec.AsyncAPI == "" {
		return "", fmt.Errorf("one of spec.asyncapi or spec.asyncapiConfigMapRef must be set")
	}
	return api.Spec.AsyncAPI, nil
}

// resolveTopics returns the explicit override when set, otherwise the topics the
// AsyncAPI document declares. The result is sorted for stable output.
func resolveTopics(content string, override []string) ([]string, error) {
	if len(override) > 0 {
		topics := append([]string(nil), override...)
		sort.Strings(topics)
		return topics, nil
	}
	doc, err := asyncapi.Parse([]byte(content))
	if err != nil {
		return nil, fmt.Errorf("parsing AsyncAPI document: %w", err)
	}
	topicMap, err := doc.GetTopicToOperationMap()
	if err != nil {
		return nil, fmt.Errorf("resolving AsyncAPI topics: %w", err)
	}
	if len(topicMap) == 0 {
		return nil, fmt.Errorf("AsyncAPI document declares no topics")
	}
	topics := make([]string, 0, len(topicMap))
	for topic := range topicMap {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	return topics, nil
}

func (r *IntegronAsyncAPIReconciler) reconcileDeployment(ctx context.Context, api *integronv1alpha1.IntegronAsyncAPI, cmName, specHash string, topics []string) error {
	labels := labelsForAsync(api)
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: api.Name, Namespace: api.Namespace}}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, dep, func() error {
		dep.Labels = labels
		dep.Spec.Replicas = api.Spec.Replicas
		dep.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{instanceLabel: api.Name}}

		image := api.Spec.Image
		if image == "" {
			image = defaultAsyncImage
		}
		pullPolicy := api.Spec.ImagePullPolicy
		if pullPolicy == "" {
			pullPolicy = corev1.PullIfNotPresent
		}

		dep.Spec.Template.ObjectMeta.Labels = labels
		if dep.Spec.Template.ObjectMeta.Annotations == nil {
			dep.Spec.Template.ObjectMeta.Annotations = map[string]string{}
		}
		dep.Spec.Template.ObjectMeta.Annotations[specHashAnnotation] = specHash

		volumes := []corev1.Volume{{
			Name: "spec",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: cmName},
					Items:                []corev1.KeyToPath{{Key: asyncSpecFileName, Path: asyncSpecFileName}},
				},
			},
		}}
		mounts := []corev1.VolumeMount{{Name: "spec", MountPath: asyncSpecMountPath, ReadOnly: true}}

		env := asyncEnv(api, topics)

		// Mount a CA bundle from a Secret when TLS verification needs one.
		if tls := api.Spec.Kafka.TLS; tls != nil && tls.CASecretRef != nil {
			volumes = append(volumes, corev1.Volume{
				Name: "kafka-ca",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: tls.CASecretRef.Name,
						Items:      []corev1.KeyToPath{{Key: tls.CASecretRef.Key, Path: caFileName}},
					},
				},
			})
			mounts = append(mounts, corev1.VolumeMount{Name: "kafka-ca", MountPath: caMountPath, ReadOnly: true})
			env = append(env, corev1.EnvVar{Name: "KAFKA_TLS_CA_FILE", Value: caMountPath + "/" + caFileName})
		}

		dep.Spec.Template.Spec.Volumes = volumes
		dep.Spec.Template.Spec.Containers = []corev1.Container{{
			Name:            "consumer",
			Image:           image,
			ImagePullPolicy: pullPolicy,
			Args:            []string{"-spec", asyncSpecMountPath + "/" + asyncSpecFileName},
			Env:             env,
			VolumeMounts:    mounts,
			Resources:       api.Spec.Resources,
		}}
		return controllerutil.SetControllerReference(api, dep, r.Scheme)
	})
	return err
}

// asyncEnv builds the consumer's environment from the Kafka spec.
func asyncEnv(api *integronv1alpha1.IntegronAsyncAPI, topics []string) []corev1.EnvVar {
	k := api.Spec.Kafka
	groupID := k.GroupID
	if groupID == "" {
		groupID = api.Name
	}

	env := []corev1.EnvVar{
		{Name: "ASYNCAPI_SPEC_PATH", Value: asyncSpecMountPath + "/" + asyncSpecFileName},
		{Name: "KAFKA_BROKERS", Value: joinComma(k.Brokers)},
		{Name: "KAFKA_GROUP_ID", Value: groupID},
		{Name: "KAFKA_TOPICS", Value: joinComma(topics)},
	}
	if api.Spec.LogLevel != "" {
		env = append(env, corev1.EnvVar{Name: "LOG_LEVEL", Value: api.Spec.LogLevel})
	}
	if k.MinBytes > 0 {
		env = append(env, corev1.EnvVar{Name: "KAFKA_MIN_BYTES", Value: strconv.Itoa(int(k.MinBytes))})
	}
	if k.MaxBytes > 0 {
		env = append(env, corev1.EnvVar{Name: "KAFKA_MAX_BYTES", Value: strconv.Itoa(int(k.MaxBytes))})
	}
	if k.BatchSize > 0 {
		env = append(env, corev1.EnvVar{Name: "KAFKA_BATCH_SIZE", Value: strconv.Itoa(int(k.BatchSize))})
	}
	if k.MaxWaitMillis > 0 {
		env = append(env, corev1.EnvVar{Name: "KAFKA_MAX_WAIT_MS", Value: strconv.Itoa(int(k.MaxWaitMillis))})
	}

	if tls := k.TLS; tls != nil && tls.Enabled {
		env = append(env, corev1.EnvVar{Name: "KAFKA_TLS_ENABLED", Value: "true"})
		if tls.InsecureSkipVerify {
			env = append(env, corev1.EnvVar{Name: "KAFKA_TLS_INSECURE_SKIP_VERIFY", Value: "true"})
		}
	}

	if sasl := k.SASL; sasl != nil {
		env = append(env,
			corev1.EnvVar{Name: "KAFKA_SASL_MECHANISM", Value: sasl.Mechanism},
			corev1.EnvVar{Name: "KAFKA_SASL_USERNAME", ValueFrom: secretEnv(sasl.UsernameSecretRef)},
			corev1.EnvVar{Name: "KAFKA_SASL_PASSWORD", ValueFrom: secretEnv(sasl.PasswordSecretRef)},
		)
	}
	return env
}

func secretEnv(ref integronv1alpha1.SecretKeyRef) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{
		SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: ref.Name},
			Key:                  ref.Key,
		},
	}
}

func (r *IntegronAsyncAPIReconciler) succeedAsync(ctx context.Context, api *integronv1alpha1.IntegronAsyncAPI, topics []string) (ctrl.Result, error) {
	var dep appsv1.Deployment
	if err := r.Get(ctx, types.NamespacedName{Namespace: api.Namespace, Name: api.Name}, &dep); err == nil {
		api.Status.ReadyReplicas = dep.Status.ReadyReplicas
	}
	api.Status.Topics = topics
	api.Status.ObservedGeneration = api.Generation
	meta.SetStatusCondition(&api.Status.Conditions, metav1.Condition{
		Type:               integronv1alpha1.ConditionReady,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: api.Generation,
		Reason:             "Reconciled",
		Message:            "Consumer resources are in sync",
	})
	return ctrl.Result{}, r.Status().Update(ctx, api)
}

func (r *IntegronAsyncAPIReconciler) failAsync(ctx context.Context, api *integronv1alpha1.IntegronAsyncAPI, reason string, cause error) (ctrl.Result, error) {
	meta.SetStatusCondition(&api.Status.Conditions, metav1.Condition{
		Type:               integronv1alpha1.ConditionReady,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: api.Generation,
		Reason:             reason,
		Message:            cause.Error(),
	})
	api.Status.ObservedGeneration = api.Generation
	if err := r.Status().Update(ctx, api); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, cause
}

// SetupWithManager wires the controller to watch IntegronAsyncAPI and owned objects.
func (r *IntegronAsyncAPIReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&integronv1alpha1.IntegronAsyncAPI{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.ConfigMap{}).
		Complete(r)
}

func labelsForAsync(api *integronv1alpha1.IntegronAsyncAPI) map[string]string {
	return map[string]string{
		nameLabel:      "integron-async",
		instanceLabel:  api.Name,
		managedByLabel: "integron-operator",
	}
}

func joinComma(items []string) string {
	out := ""
	for i, s := range items {
		if i > 0 {
			out += ","
		}
		out += s
	}
	return out
}
