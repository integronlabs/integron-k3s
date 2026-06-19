# integron-k3s

Run **no-code, OpenAPI-defined REST APIs on k3s/Kubernetes.**

[integron](https://github.com/integronlabs/integron) is an engine that turns an
OpenAPI 3 document into a running REST API: each operation carries an
`x-integron-steps` extension that declares an orchestration pipeline (HTTP
calls, array/object transforms, remove-null, error) executed per request — no
application code.

`integron-k3s` is a Kubernetes **operator** that makes that fully declarative on
a cluster. You apply a single `IntegronAPI` custom resource containing the
OpenAPI document, and the operator provisions and keeps in sync:

```
IntegronAPI (your OpenAPI + x-integron-steps)
   └─ operator reconciles ─▶ ConfigMap (spec)
                            ▶ Deployment (integron engine, mounts spec)
                            ▶ Service (ClusterIP :80 ▶ :8080)
                            ▶ Ingress (optional)
```

Changing the spec rolls the engine pods automatically (the pod template carries
a hash of the spec).

## Components

| Path | What it is |
| --- | --- |
| `api/v1alpha1` | The `IntegronAPI` CRD Go types |
| `internal/controller` | The reconciler |
| `cmd/manager` | Operator entrypoint |
| `Dockerfile` | Builds the operator image |
| `Dockerfile.engine` | Builds the integron engine image (`go install …@INTEGRON_VERSION`) |
| `config/` | CRD, RBAC, operator Deployment, sample, kustomization |

## Quick start

Prereqs: a cluster (k3s is assumed; it ships Traefik + a default IngressClass),
Go 1.24+, Docker, `kubectl`.

```sh
# 1. Build images and import them into k3s's containerd
make k3s-import          # = docker-build + ctr images import

# 2. Install the CRD, RBAC and operator
make install             # kubectl apply -k config

# 3. Create a no-code API
make sample              # applies config/samples/dogfacts.yaml
kubectl get integronapi  # shows replicas / ready / url

# 4. Call it (via the Service, or the Ingress host if DNS/hosts is set up)
kubectl run curl --rm -it --image=curlimages/curl --restart=Never -- \
  curl -s "http://dogfacts.default.svc/facts?amount=3"
```

When pushing images to a registry instead of importing into k3s, override the
image names:

```sh
make docker-build OPERATOR_IMG=you/operator:dev ENGINE_IMG=you/engine:dev
```

…and set `spec.image` on the `IntegronAPI` (and the operator Deployment image
in `config/manager/manager.yaml`) to match.

## Defining an API

A minimal `IntegronAPI`:

```yaml
apiVersion: integron.integronlabs.io/v1alpha1
kind: IntegronAPI
metadata:
  name: dogfacts
spec:
  replicas: 1
  ingress:
    host: dogfacts.local
  openapi: |
    openapi: 3.0.3
    info: { title: Dog Facts, version: 1.0.0 }
    paths:
      /facts:
        get:
          operationId: getDogFact
          parameters:
            - { name: amount, in: query, required: true, schema: { type: integer } }
          responses: { '200': { description: ok } }
          x-integron-steps:
            - name: dogFacts
              type: http
              url: 'https://dogapi.dog/api/v2/facts?limit=$.request.amount'
              method: GET
              responses:
                '200': { output: { response: $.body, status: $.status }, next: arrayTransform }
            - name: arrayTransform
              type: transformarray
              input: $.dogFacts.response.data
              output: { fact: $.attributes.body, id: $.id }
              next: responseMarshal
            - name: responseMarshal
              type: transformobject
              output: { body: { data: $.arrayTransform } }
              next: ""
            - name: error
              type: error
              next: ""
```

### Spec fields

| Field | Default | Description |
| --- | --- | --- |
| `spec.openapi` | — | Inline OpenAPI 3 document (with `x-integron-steps`). |
| `spec.openapiConfigMapRef` | — | Alternatively, reference an existing ConfigMap (`name`, `key`). |
| `spec.image` | `…/engine:latest` | Engine image to run. |
| `spec.imagePullPolicy` | `IfNotPresent` | |
| `spec.replicas` | `1` | Engine pod count. |
| `spec.ingress` | — | `host`, `path` (`/`), `pathType` (`Prefix`), `className`, `annotations`. Omit to skip Ingress. |
| `spec.resources` | — | Standard pod resource requirements. |

Exactly one of `spec.openapi` / `spec.openapiConfigMapRef` is required.

## Develop the operator

```sh
make tidy            # go mod tidy
make build           # compile ./cmd/manager
make run             # run against your current kubeconfig (out-of-cluster)
make test vet
```

> Note: `api/v1alpha1/zz_generated.deepcopy.go` and
> `config/crd/...integronapis.yaml` are maintained by hand to mirror what
> `controller-gen` would produce — keep them in step with `*_types.go`.
