package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/yaml"

	integronv1alpha1 "github.com/integronlabs/integron-k3s/api/v1alpha1"
)

const (
	// specMountPath is the engine working directory; integron defaults to
	// reading docs/openapi.yaml relative to it, so the spec lands at
	// <specMountPath>/openapi.yaml.
	specMountPath = "/app/docs"
	specFileName  = "openapi.yaml"
	enginePort    = 8080

	// specHashAnnotation drives a rolling restart whenever the spec changes.
	specHashAnnotation = "integron.integronlabs.io/spec-hash"
	managedByLabel     = "app.kubernetes.io/managed-by"
	nameLabel          = "app.kubernetes.io/name"
	instanceLabel      = "app.kubernetes.io/instance"
)

// IntegronAPIReconciler reconciles an IntegronAPI object into a running engine.
type IntegronAPIReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=integron.integronlabs.io,resources=integronapis,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=integron.integronlabs.io,resources=integronapis/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=integron.integronlabs.io,resources=integronapis/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete

// Reconcile drives the actual cluster state toward the IntegronAPI spec.
func (r *IntegronAPIReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var api integronv1alpha1.IntegronAPI
	if err := r.Get(ctx, req.NamespacedName, &api); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Resolve the spec content and the ConfigMap that holds it.
	cmName, cmKey, specHash, err := r.reconcileSpec(ctx, &api)
	if err != nil {
		return r.fail(ctx, &api, "SpecError", err)
	}

	if err := r.reconcileDeployment(ctx, &api, cmName, cmKey, specHash); err != nil {
		return r.fail(ctx, &api, "DeploymentError", err)
	}

	if err := r.reconcileService(ctx, &api); err != nil {
		return r.fail(ctx, &api, "ServiceError", err)
	}

	url, err := r.reconcileIngress(ctx, &api)
	if err != nil {
		return r.fail(ctx, &api, "IngressError", err)
	}

	return r.succeed(ctx, &api, url)
}

// reconcileSpec ensures a ConfigMap holds the OpenAPI document and returns its
// name, key and a content hash.
//
// The document is resolved from inline spec.openapi or a referenced ConfigMap.
// When BasePath is set, a relative servers entry is injected so the engine
// serves every operation under the prefix, and the (possibly rewritten)
// document is always materialized into an owned ConfigMap with its servers
// rewritten to the effective base path: integron's router requires a servers
// entry with a non-empty path to match routes, so every API is mounted under a
// prefix (BasePath, or "/<name>" by default).
func (r *IntegronAPIReconciler) reconcileSpec(ctx context.Context, api *integronv1alpha1.IntegronAPI) (string, string, string, error) {
	content, err := r.resolveSpecContent(ctx, api)
	if err != nil {
		return "", "", "", err
	}

	content, err = withBasePath(content, effectiveBasePath(api))
	if err != nil {
		return "", "", "", err
	}

	cmName := api.Name + "-spec"
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: cmName, Namespace: api.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = labelsFor(api)
		cm.Data = map[string]string{specFileName: content}
		return controllerutil.SetControllerReference(api, cm, r.Scheme)
	}); err != nil {
		return "", "", "", fmt.Errorf("reconciling spec ConfigMap: %w", err)
	}
	return cmName, specFileName, hashString(content), nil
}

// resolveSpecContent returns the raw OpenAPI document from inline spec or a ref.
func (r *IntegronAPIReconciler) resolveSpecContent(ctx context.Context, api *integronv1alpha1.IntegronAPI) (string, error) {
	if ref := api.Spec.OpenAPIConfigMapRef; ref != nil {
		key := ref.Key
		if key == "" {
			key = specFileName
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
	if api.Spec.OpenAPI == "" {
		return "", fmt.Errorf("one of spec.openapi or spec.openapiConfigMapRef must be set")
	}
	return api.Spec.OpenAPI, nil
}

func (r *IntegronAPIReconciler) reconcileDeployment(ctx context.Context, api *integronv1alpha1.IntegronAPI, cmName, cmKey, specHash string) error {
	labels := labelsFor(api)
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: api.Name, Namespace: api.Namespace}}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, dep, func() error {
		dep.Labels = labels
		dep.Spec.Replicas = api.Spec.Replicas
		dep.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{instanceLabel: api.Name}}

		image := api.Spec.Image
		if image == "" {
			image = "ghcr.io/integronlabs/integron-k3s/engine:latest"
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

		dep.Spec.Template.Spec.Volumes = []corev1.Volume{{
			Name: "spec",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: cmName},
					Items:                []corev1.KeyToPath{{Key: cmKey, Path: specFileName}},
				},
			},
		}}

		probe := &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(enginePort)},
			},
			InitialDelaySeconds: 2,
			PeriodSeconds:       10,
		}

		dep.Spec.Template.Spec.Containers = []corev1.Container{{
			Name:            "integron",
			Image:           image,
			ImagePullPolicy: pullPolicy,
			Args:            []string{"-spec", "docs/" + specFileName},
			Ports:           []corev1.ContainerPort{{Name: "http", ContainerPort: enginePort}},
			VolumeMounts:    []corev1.VolumeMount{{Name: "spec", MountPath: specMountPath, ReadOnly: true}},
			Resources:       api.Spec.Resources,
			ReadinessProbe:  probe,
			LivenessProbe:   probe.DeepCopy(),
		}}
		return controllerutil.SetControllerReference(api, dep, r.Scheme)
	})
	return err
}

func (r *IntegronAPIReconciler) reconcileService(ctx context.Context, api *integronv1alpha1.IntegronAPI) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: api.Name, Namespace: api.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = labelsFor(api)
		svc.Spec.Selector = map[string]string{instanceLabel: api.Name}
		svc.Spec.Type = corev1.ServiceTypeClusterIP
		svc.Spec.Ports = []corev1.ServicePort{{
			Name:       "http",
			Port:       80,
			TargetPort: intstr.FromInt32(enginePort),
			Protocol:   corev1.ProtocolTCP,
		}}
		return controllerutil.SetControllerReference(api, svc, r.Scheme)
	})
	return err
}

// reconcileIngress creates an Ingress when configured and returns the public URL.
// When no Ingress is configured it removes any previously-created one.
func (r *IntegronAPIReconciler) reconcileIngress(ctx context.Context, api *integronv1alpha1.IntegronAPI) (string, error) {
	ing := &networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: api.Name, Namespace: api.Namespace}}

	if api.Spec.Ingress == nil {
		if err := r.Delete(ctx, ing); err != nil && !apierrors.IsNotFound(err) {
			return "", err
		}
		return "", nil
	}

	in := api.Spec.Ingress
	path := in.Path
	if path == "" {
		path = effectiveBasePath(api)
	}
	pathType := networkingv1.PathType(in.PathType)
	if pathType == "" {
		pathType = networkingv1.PathTypePrefix
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, ing, func() error {
		ing.Labels = labelsFor(api)
		ing.Annotations = in.Annotations
		if in.ClassName != "" {
			cn := in.ClassName
			ing.Spec.IngressClassName = &cn
		}
		ing.Spec.Rules = []networkingv1.IngressRule{{
			Host: in.Host,
			IngressRuleValue: networkingv1.IngressRuleValue{
				HTTP: &networkingv1.HTTPIngressRuleValue{
					Paths: []networkingv1.HTTPIngressPath{{
						Path:     path,
						PathType: &pathType,
						Backend: networkingv1.IngressBackend{
							Service: &networkingv1.IngressServiceBackend{
								Name: api.Name,
								Port: networkingv1.ServiceBackendPort{Number: 80},
							},
						},
					}},
				},
			},
		}}
		return controllerutil.SetControllerReference(api, ing, r.Scheme)
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("http://%s%s", in.Host, path), nil
}

func (r *IntegronAPIReconciler) succeed(ctx context.Context, api *integronv1alpha1.IntegronAPI, url string) (ctrl.Result, error) {
	var dep appsv1.Deployment
	if err := r.Get(ctx, types.NamespacedName{Namespace: api.Namespace, Name: api.Name}, &dep); err == nil {
		api.Status.ReadyReplicas = dep.Status.ReadyReplicas
	}
	api.Status.URL = url
	api.Status.ObservedGeneration = api.Generation
	meta.SetStatusCondition(&api.Status.Conditions, metav1.Condition{
		Type:               integronv1alpha1.ConditionReady,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: api.Generation,
		Reason:             "Reconciled",
		Message:            "Engine resources are in sync",
	})
	return ctrl.Result{}, r.Status().Update(ctx, api)
}

func (r *IntegronAPIReconciler) fail(ctx context.Context, api *integronv1alpha1.IntegronAPI, reason string, cause error) (ctrl.Result, error) {
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

// SetupWithManager wires the controller to watch IntegronAPI and owned objects.
func (r *IntegronAPIReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&integronv1alpha1.IntegronAPI{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&networkingv1.Ingress{}).
		Complete(r)
}

func labelsFor(api *integronv1alpha1.IntegronAPI) map[string]string {
	return map[string]string{
		nameLabel:      "integron",
		instanceLabel:  api.Name,
		managedByLabel: "integron-operator",
	}
}

func hashString(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// normalizeBasePath returns a clean "/prefix" form, or "" when unset.
func normalizeBasePath(p string) string {
	p = strings.TrimSpace(p)
	if p == "" || p == "/" {
		return ""
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return strings.TrimRight(p, "/")
}

// effectiveBasePath is the prefix the API is mounted under. integron cannot
// serve at the bare root, so an unset BasePath defaults to "/<name>".
func effectiveBasePath(api *integronv1alpha1.IntegronAPI) string {
	if bp := normalizeBasePath(api.Spec.BasePath); bp != "" {
		return bp
	}
	return "/" + api.Name
}

// withBasePath rewrites the OpenAPI document's servers to a single relative URL
// equal to basePath, so integron's router serves every operation beneath it.
// The steps array order is preserved (object keys may be reordered, which is
// semantically irrelevant for OpenAPI).
func withBasePath(content, basePath string) (string, error) {
	var doc map[string]interface{}
	if err := yaml.Unmarshal([]byte(content), &doc); err != nil {
		return "", fmt.Errorf("parsing OpenAPI document: %w", err)
	}
	doc["servers"] = []interface{}{map[string]interface{}{"url": basePath}}
	out, err := yaml.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("serializing OpenAPI document: %w", err)
	}
	return string(out), nil
}
