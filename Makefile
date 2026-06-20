# integron-k3s — operator + engine images and k3s install targets.

OPERATOR_IMG ?= ghcr.io/integronlabs/integron-k3s/operator:latest
ENGINE_IMG   ?= ghcr.io/integronlabs/integron-k3s/engine:latest
INTEGRON_VERSION ?= v0.2.0

.PHONY: help
help: ## Show this help.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

.PHONY: tidy
tidy: ## Resolve Go module dependencies.
	go mod tidy

.PHONY: build
build: ## Compile the operator binary.
	go build -o bin/manager ./cmd/manager

.PHONY: test
test: ## Run unit tests.
	go test ./...

.PHONY: vet
vet: ## Run go vet.
	go vet ./...

.PHONY: run
run: ## Run the operator locally against the current kubeconfig.
	go run ./cmd/manager

.PHONY: docker-operator
docker-operator: ## Build the operator image.
	docker build -t $(OPERATOR_IMG) -f Dockerfile .

.PHONY: docker-engine
docker-engine: ## Build the integron engine image.
	docker build -t $(ENGINE_IMG) -f Dockerfile.engine --build-arg INTEGRON_VERSION=$(INTEGRON_VERSION) .

.PHONY: docker-build
docker-build: docker-operator docker-engine ## Build both images.

.PHONY: install
install: ## Install CRD, RBAC and operator into the current cluster.
	kubectl apply -k config

.PHONY: uninstall
uninstall: ## Remove the operator and CRD from the cluster.
	kubectl delete -k config

.PHONY: sample
sample: ## Apply the dog facts sample IntegronAPI.
	kubectl apply -f config/samples/dogfacts.yaml

.PHONY: k3s-import
k3s-import: docker-build ## Import locally-built images into k3s containerd.
	docker save $(OPERATOR_IMG) | sudo k3s ctr images import -
	docker save $(ENGINE_IMG) | sudo k3s ctr images import -
