##
# Copyright 2023 Aiven Oy
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##
VERSION := $(shell grep -oP 'version=\K[^[:space:]]+' gradle.properties)
IMAGE_NAME := aivenoy/kafka-with-ts-plugin
IMAGE_VERSION := latest
IMAGE_TAG := $(IMAGE_NAME):$(IMAGE_VERSION)

.PHONY: clean checkstyle build test integration_test e2e_test docker_image docker_push

clean:
	./gradlew clean

checkstyle:
	./gradlew checkstyleMain checkstyleTest checkstyleIntegrationTest

build: build/distributions/tiered-storage-for-apache-kafka-$(VERSION).tgz

build/distributions/tiered-storage-for-apache-kafka-$(VERSION).tgz:
	./gradlew build distTar -x test -x integrationTest -x e2e:test

test: build/distributions/tiered-storage-for-apache-kafka-$(VERSION).tgz
	./gradlew test -x e2e:test

integration_test: build/distributions/tiered-storage-for-apache-kafka-$(VERSION).tgz
	./gradlew integrationTest

E2E_TEST=LocalSystem

e2e_test:  build/distributions/tiered-storage-for-apache-kafka-$(VERSION).tgz
	./gradlew e2e:test --tests $(E2E_TEST)*

.PHONY: docker_image
docker_image: build/distributions/tiered-storage-for-apache-kafka-$(VERSION).tgz
	docker build . \
		-f docker/Dockerfile \
		--build-arg _VERSION=$(VERSION) \
		-t $(IMAGE_TAG)

.PHONY: docker_push
docker_push:
	docker push $(IMAGE_TAG)
