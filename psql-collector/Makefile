IMAGE_NAME=psql-collector
TAG=latest
DOCKER_REGISTRY=vladmsnk/$(IMAGE_NAME)

all: build

build:
	docker build -t $(IMAGE_NAME):$(TAG) .
	docker tag $(IMAGE_NAME):$(TAG) $(DOCKER_REGISTRY):$(TAG)

push:
	docker push $(DOCKER_REGISTRY):$(TAG)

clean:
	docker rmi $(IMAGE_NAME):$(TAG)
	docker rmi $(DOCKER_REGISTRY):$(TAG)

help:
	@echo "Makefile commands:"
	@echo "build  - Build Docker image"
	@echo "push   - Push Docker image to Docker Hub"
	@echo "clean  - Remove Docker images locally"
	@echo "all    - Build the Docker image"

.PHONY: build push clean all help
