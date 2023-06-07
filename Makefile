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
VERSION=0.0.1-SNAPSHOT
IMAGE_TAG=aivenoy/kafka:3.3-2022-10-06-tiered-storage-1-ts-2

.PHONY: clean
clean:
	./gradlew clean

build/distributions/tiered-storage-for-apache-kafka-$(VERSION).tgz:
	./gradlew build distTar

.PHONY: docker_image
docker_image: build/distributions/tiered-storage-for-apache-kafka-$(VERSION).tgz
	docker build . \
		--build-arg _VERSION=$(VERSION) \
		-t $(IMAGE_TAG)

.PHONY: docker_push
docker_push:
	docker push $(IMAGE_TAG)

bench_prep:
	sudo sh -c 'echo 1 >/proc/sys/kernel/perf_event_paranoid'
	sudo sh -c 'echo 0 >/proc/sys/kernel/kptr_restrict'

BENCH=io.aiven.kafka.tieredstorage.benchs.transform.TransformBench

bench_run:
	java -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints -cp "benchmarks/build/install/benchmarks/*" $(BENCH)
