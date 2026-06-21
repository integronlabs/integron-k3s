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
| `api/v1alpha1` | The `IntegronAPI` and `IntegronAsyncAPI` CRD Go types |
| `internal/controller` | The reconcilers |
| `cmd/manager` | Operator entrypoint |
| `cmd/async-consumer` | The Kafka consumer that runs `integron-async` workflows per message |
| `Dockerfile` | Builds the operator image |
| `Dockerfile.engine` | Builds the integron engine image (`go install …@INTEGRON_VERSION`) |
| `Dockerfile.async` | Builds the async consumer image |
| `config/` | CRDs, RBAC, operator Deployment, samples, kustomization |

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

# 4. Call it (note the /dogfacts prefix — every API is mounted under one)
kubectl run curl --rm -it --image=curlimages/curl --restart=Never -- \
  curl -s "http://dogfacts.default.svc/dogfacts/facts?amount=3"
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
| `spec.basePath` | `/<name>` | Mount the API under a path prefix (e.g. `/dogfacts`) so many APIs share one host. See below. |
| `spec.image` | `…/engine:latest` | Engine image to run. |
| `spec.imagePullPolicy` | `IfNotPresent` | |
| `spec.replicas` | `1` | Engine pod count. |
| `spec.ingress` | — | `host`, `path` (`/`), `pathType` (`Prefix`), `className`, `annotations`. Omit to skip Ingress. |
| `spec.resources` | — | Standard pod resource requirements. |

Exactly one of `spec.openapi` / `spec.openapiConfigMapRef` is required.

## Hosting many APIs on one host

Set `spec.basePath` to mount each API under a path prefix and point every
`IntegronAPI` at the **same** `spec.ingress.host`:

```yaml
# dogfacts
spec:
  basePath: /dogfacts
  ingress: { host: apis.local }
---
# echo
spec:
  basePath: /echo
  ingress: { host: apis.local }
```

```
            apis.local            (one Ingress host, any controller)
              ├── /dogfacts/*  ─▶ dogfacts engine   (GET /dogfacts/facts)
              └── /echo/*      ─▶ echo engine        (GET /echo/ping)
```

How it works: the operator rewrites the document's `servers` to a single
relative entry `servers: [{ url: /dogfacts }]`, so integron's router natively
serves every operation **under** the prefix. The generated Ingress is a plain
host+path rule — **no prefix-stripping middleware, no controller-specific
annotations**, so it works the same on Traefik (k3s default), nginx, or any
other controller. The prefix also applies to in-cluster calls:
`http://dogfacts.default.svc/dogfacts/facts`.

> **Every API is mounted under a prefix.** integron's router will not match
> routes at the bare root, so `spec.basePath` defaults to `/<name>` and the
> operator always manages the `servers` field — any `servers` you put in the
> document is replaced. Requests without the prefix return `404 Method not
> found`.

There's no hard limit on how many APIs share a host — each is an independent
`IntegronAPI` with its own Ingress rule for the shared host, which controllers
merge. Try it: `kubectl apply -f config/samples/dogfacts.yaml -f config/samples/second-api-same-host.yaml`.

### Scaling note: one engine pod per API

Each `IntegronAPI` runs its own integron Deployment (integron loads a single
spec per process). Hundreds of APIs therefore means hundreds of pods. To keep
that affordable: keep `spec.replicas: 1` and set small `spec.resources`
requests (each engine is a tiny static Go binary). If you need to pack
thousands of mostly-idle APIs onto a node, the next step would be either
scale-to-zero (e.g. KEDA/Knative) or a shared multi-spec engine — happy to
explore either; it's not wired up today.

## Event-driven (async) APIs over Kafka

The same no-code model also runs **event-driven**:
[integron-async](https://github.com/integronlabs/integron-async) interprets an
**AsyncAPI 3** document where each operation's `x-integron-steps` pipeline runs
once **per message consumed from a Kafka topic** (the operation's channel
address). Upstream, integron-async is built for AWS Lambda — it consumes Kafka
*indirectly*, through EventBridge Pipes. `integron-k3s` adapts that engine to a
plain cluster: a small consumer (`cmd/async-consumer`) drives the **same engine**
from a native Kafka **consumer group**, so no AWS is involved.

You apply an `IntegronAsyncAPI` and the operator provisions:

```
IntegronAsyncAPI (your AsyncAPI + x-integron-steps + Kafka config)
   └─ operator reconciles ─▶ ConfigMap (spec)
                            ▶ Deployment (async-consumer, mounts spec)
```

No Service or Ingress — it is **consumer-only**. The consumer subscribes to the
spec's topics, and for each batch it maps messages to the engine's record type,
runs `ProcessBatch`, and commits offsets **selectively**: any offset the engine
reports as failed is left uncommitted so Kafka redelivers it. Processing is
**at-least-once** — make steps idempotent. Scale with `spec.replicas`: all
replicas join one consumer group, so Kafka balances partitions across them
(replicas beyond the partition count sit idle).

```sh
make sample-async              # applies config/samples/dogfacts-async.yaml
kubectl get integronasyncapi   # shows replicas / ready / group
```

A minimal `IntegronAsyncAPI`:

```yaml
apiVersion: integron.integronlabs.io/v1alpha1
kind: IntegronAsyncAPI
metadata:
  name: dogfacts-async
spec:
  replicas: 1
  kafka:
    brokers: [kafka.default.svc:9092]
    # groupID defaults to the resource name; topics default to the spec's channels
  asyncapi: |
    asyncapi: 3.0.0
    info: { title: Dog Facts (async), version: 1.0.0 }
    channels:
      dogFactRequests:
        address: dogfact-requests-topic        # the Kafka topic
    operations:
      onFactRequest:
        action: receive
        channel: { $ref: '#/channels/dogFactRequests' }
        x-integron-steps:
          - name: fetchFact
            type: http
            url: 'https://dogapi.dog/api/v2/facts?limit=$.message.payload.amount'
            method: GET
            responses:
              '200': { output: { response: $.body }, next: "" }
          - name: error
            type: error
            next: ""
```

The message payload is available to steps as `$.message.payload.*`.

### Async spec fields

| Field | Default | Description |
| --- | --- | --- |
| `spec.asyncapi` | — | Inline AsyncAPI 3 document (with `x-integron-steps`). |
| `spec.asyncapiConfigMapRef` | — | Alternatively, reference an existing ConfigMap (`name`, `key`). |
| `spec.kafka.brokers` | — | **Required.** Bootstrap broker addresses (`host:port`). |
| `spec.kafka.groupID` | `<name>` | Consumer group shared by all replicas. |
| `spec.kafka.topics` | spec channels | Restrict subscription; defaults to every channel address. |
| `spec.kafka.tls` | — | `enabled`, `insecureSkipVerify`, `caSecretRef` (`name`, `key`). |
| `spec.kafka.sasl` | — | `mechanism` (`PLAIN`/`SCRAM-SHA-256`/`SCRAM-SHA-512`), `usernameSecretRef`, `passwordSecretRef`. |
| `spec.kafka.batchSize` | `100` | Max messages per `ProcessBatch` call. |
| `spec.kafka.maxWaitMillis` | `1000` | Max time to fill a batch before processing. |
| `spec.kafka.minBytes` / `maxBytes` | `1` / `1 MiB` | Fetch sizing. |
| `spec.image` | `…/async-engine:latest` | Consumer image to run. |
| `spec.replicas` | `1` | Consumer pod count (joins the group). |
| `spec.resources` | — | Standard pod resource requirements. |

Exactly one of `spec.asyncapi` / `spec.asyncapiConfigMapRef` is required.

Connecting to a TLS + SASL broker (e.g. managed Kafka) pulls credentials from a
Secret in the same namespace:

```yaml
spec:
  kafka:
    brokers: [pkc-xxxx.region.aws.confluent.cloud:9092]
    tls: { enabled: true }
    sasl:
      mechanism: PLAIN
      usernameSecretRef: { name: kafka-creds, key: username }
      passwordSecretRef: { name: kafka-creds, key: password }
```

## Develop the operator

```sh
make tidy            # go mod tidy
make build           # compile ./cmd/manager
make run             # run against your current kubeconfig (out-of-cluster)
make test vet
```

> Note: `api/v1alpha1/zz_generated.deepcopy.go` and the `config/crd/*.yaml`
> manifests are maintained by hand to mirror what `controller-gen` would
> produce — keep them in step with `*_types.go`.
