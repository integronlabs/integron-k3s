# Testing IntegronAsyncAPI end-to-end on k3s

A complete, copy-pasteable runbook for exercising the event-driven
(`IntegronAsyncAPI`) path on a **native k3s cluster (Linux)**, against a
**lightweight single-node Kafka** running in the cluster.

The single-node Kafka exposes a Service named `kafka` in the `default`
namespace, which matches the sample's default broker
(`kafka.default.svc:9092`) — so the sample applies unchanged.

## Prerequisites

- A running k3s cluster (k3s ships Traefik + a default IngressClass and the
  `local-path` storage provisioner).
- `kubectl` pointed at it, Go 1.24+, and Docker on the build host.
- This branch checked out: `git switch feat/async-kafka-api`.
- Outbound internet from the cluster nodes — the sample's `http` step calls
  `https://dogapi.dog`.

## 1. Build the images and import them into k3s

The operator and async-consumer images are referenced with
`imagePullPolicy: IfNotPresent`, so importing them locally avoids any registry.

```sh
make k3s-import          # docker-build (operator + engine + async-engine)
                         # then `sudo k3s ctr images import` for each
```

## 2. Install the operator + CRDs

```sh
make install             # kubectl apply -k config
kubectl -n integron-system rollout status deploy/integron-operator
kubectl get crd | grep integron        # integronapis + integronasyncapis
```

## 3. Deploy a single-node Kafka (KRaft, ephemeral)

One pod, no operator. `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true` means the topic is
created on first use; `KAFKA_NUM_PARTITIONS=3` gives auto-created topics three
partitions so you can later see consumer-group balancing.

```sh
kubectl apply -f - <<'EOF'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka
  namespace: default
spec:
  replicas: 1
  selector: { matchLabels: { app: kafka } }
  template:
    metadata: { labels: { app: kafka } }
    spec:
      containers:
        - name: kafka
          image: apache/kafka:3.8.0
          ports: [{ containerPort: 9092 }]
          env:
            - { name: KAFKA_NODE_ID, value: "1" }
            - { name: KAFKA_PROCESS_ROLES, value: "broker,controller" }
            - { name: KAFKA_LISTENERS, value: "PLAINTEXT://:9092,CONTROLLER://:9093" }
            - { name: KAFKA_ADVERTISED_LISTENERS, value: "PLAINTEXT://kafka.default.svc.cluster.local:9092" }
            - { name: KAFKA_CONTROLLER_LISTENER_NAMES, value: "CONTROLLER" }
            - { name: KAFKA_LISTENER_SECURITY_PROTOCOL_MAP, value: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT" }
            - { name: KAFKA_CONTROLLER_QUORUM_VOTERS, value: "1@localhost:9093" }
            - { name: KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR, value: "1" }
            - { name: KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR, value: "1" }
            - { name: KAFKA_TRANSACTION_STATE_LOG_MIN_ISR, value: "1" }
            - { name: KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS, value: "0" }
            - { name: KAFKA_AUTO_CREATE_TOPICS_ENABLE, value: "true" }
            - { name: KAFKA_NUM_PARTITIONS, value: "3" }
---
apiVersion: v1
kind: Service
metadata:
  name: kafka
  namespace: default
spec:
  selector: { app: kafka }
  ports: [{ port: 9092, targetPort: 9092 }]
EOF

kubectl rollout status deploy/kafka
```

## 4. Deploy the async API

The sample defaults to `kafka.default.svc:9092`, so it applies as-is.

```sh
make sample-async
kubectl rollout status deploy/dogfacts-async
kubectl get integronasyncapi           # Ready=1, Group=dogfacts-async
kubectl get integronasyncapi dogfacts-async -o jsonpath='{.status.topics}'; echo
```

`status.topics` should report `["dogfact-requests-topic"]` (resolved from the
AsyncAPI channels by the operator).

## 5. Send a message and watch it process

In one terminal, tail the consumer:

```sh
kubectl logs -f deploy/dogfacts-async
```

In another, produce a message from the Kafka pod itself:

```sh
KPOD=$(kubectl get pod -l app=kafka -o name)
echo '{"amount":3}' | kubectl exec -i "$KPOD" -- \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic dogfact-requests-topic
```

The consumer log should show it picked up the message and logged
`processed 1 messages: all 1 committed`. A **committed** message is the
positive signal that the workflow ran to completion — a failed step leaves its
offset *uncommitted* and the log instead reads `… N committed, M failed (will be
redelivered)`.

### Seeing each workflow step run

The `processed …: all N committed` line proves the pipeline finished without
error, but it doesn't show the individual steps. To watch the engine execute
each step (the HTTP request, array/object transforms), turn on debug logging —
this is read from `LOG_LEVEL` by both the consumer and the engine:

```sh
kubectl patch integronasyncapi dogfacts-async --type=merge -p '{"spec":{"logLevel":"debug"}}'
kubectl rollout status deploy/dogfacts-async
kubectl logs -f deploy/dogfacts-async
```

The engine logs `Processing message: topic=… offset=…` (at info, on dequeue),
then per-step debug lines, and a step failure surfaces at warn/error with the
recovery step. Set `logLevel` back to `info` (or remove it) when done — debug is
verbose.

> The engine emits structured JSON logs (it calls `helpers.SetupLogging()`),
> so `kubectl logs … | jq` works for filtering.

## 6. Verify offset commits and group balancing

```sh
kubectl exec -it "$KPOD" -- /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group dogfacts-async
```

`CURRENT-OFFSET` advancing with `LAG → 0` confirms the selective offset-commit
path. To see partitions balanced across members (the topic has 3):

```sh
kubectl scale deploy/dogfacts-async --replicas=3
# re-run the --describe above; CONSUMER-ID differs across partitions
```

## 7. Cleanup

```sh
kubectl delete -f config/samples/dogfacts-async.yaml
kubectl delete deploy/kafka svc/kafka
make uninstall           # removes operator + CRDs
```

## Troubleshooting

- **Consumer `CrashLoopBackOff` / "KAFKA_BROKERS is required":** the operator
  didn't render env — check `kubectl describe integronasyncapi dogfacts-async`
  for a `Ready=False` condition and its message.
- **No progress, lag stays > 0:** the consumer can't reach the broker. Confirm
  `kafka.default.svc:9092` resolves and the Kafka pod is `Ready`. The advertised
  listener must be the Service DNS (`kafka.default.svc.cluster.local`), as set
  above — an incorrect advertised listener is the usual cause.
- **Consumer is a `Stable` group member but `--members` shows `#PARTITIONS 0`:**
  it joined before the topic existed and cached an empty assignment. The consumer
  enables kafka-go's partition watcher, so it self-heals within
  ~15s of the topic being created — give it a moment, or
  `kubectl rollout restart deploy/<name>` to force it immediately. For
  production, prefer **pre-creating topics** over relying on broker
  auto-creation so partitions exist before the consumer starts.
- **Offset reported as failed and redelivered:** the workflow step failed. This
  is the at-least-once redelivery path working, but it usually means a step's
  JSONPath (e.g. `$.message.payload.amount`) doesn't match your message shape,
  or the `http` step had no egress. Check the consumer logs for the step error.
- **`http` step errors:** confirm the node has outbound internet to
  `dogapi.dog`; a restrictive NetworkPolicy will block it.

## What this does and doesn't cover

- ✅ Operator reconcile → ConfigMap + consumer Deployment, topic resolution into
  status, real Kafka consumer-group consumption, the engine's HTTP step, and
  selective offset commit / redelivery.
- ❌ TLS/SASL — this Kafka is plaintext. To test auth, stand up a broker with
  SCRAM/TLS (e.g. via Strimzi), put the credentials in a `Secret`, and set
  `spec.kafka.tls` / `spec.kafka.sasl` referencing it (see the README).

## Alternative: a real multi-broker cluster (Strimzi)

For a production-like cluster with the Topic/User operators (and a clean path to
SASL/TLS), install [Strimzi](https://strimzi.io) instead of the single pod in
step 3, create a `Kafka` + `KafkaNodePool` + `KafkaTopic`, then point the sample
at its bootstrap service:

```sh
kubectl patch integronasyncapi dogfacts-async --type=merge \
  -p '{"spec":{"kafka":{"brokers":["my-cluster-kafka-bootstrap.kafka.svc:9092"]}}}'
```

The `IntegronAsyncAPI` steps (4–6) are otherwise identical.
