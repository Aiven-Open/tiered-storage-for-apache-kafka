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

kafka_container=kafka-ts
bootstrap_server=kafka:29092
bootstrap_server_host=localhost:9092

# Topic defaults
topic=topic1
partitions=1
replication_factor=1
segment=1000000 # 1MB
retention_bytes=20000000 # 20MB
local_retention_bytes=5000000 # 5MB
retention_ms=360000000 # 100 hours
local_retention_ms=1000 # 1 second

.PHONY: create_topic_by_size_ts
create_topic_by_size_ts:
	docker exec -e KAFKA_OPTS= $(kafka_container) \
		kafka-topics \
		--bootstrap-server $(bootstrap_server) \
		--create \
		--config remote.storage.enable=true \
		--config retention.ms=-1 \
		--config segment.bytes=$(segment) \
		--config retention.bytes=$(retention_bytes) \
		--config local.retention.bytes=$(local_retention_bytes) \
		--partitions $(partitions) \
		--replication-factor $(replication_factor) \
		--topic $(topic)

.PHONY: create_topic_by_time_ts
create_topic_by_time_ts:
	docker exec -e KAFKA_OPTS= $(kafka_container) \
		kafka-topics \
		--bootstrap-server $(bootstrap_server) \
		--create \
		--config remote.storage.enable=true \
		--config segment.bytes=$(segment) \
		--config retention.ms=$(retention_ms) \
		--config local.retention.ms=$(local_retention_ms) \
		--partitions $(partitions) \
		--replication-factor $(replication_factor) \
		--topic $(topic)

.PHONY: create_topic_by_size_no_ts
create_topic_by_size_no_ts:
	docker exec -e KAFKA_OPTS= $(kafka_container) \
		kafka-topics \
		--bootstrap-server $(bootstrap_server) \
		--create \
		--config retention.ms=-1 \
		--config segment.bytes=$(segment) \
		--config retention.bytes=$(retention_bytes) \
		--partitions $(partitions) \
		--replication-factor $(replication_factor) \
		--topic $(topic)

.PHONY: create_topic_by_time_no_ts
create_topic_by_time_no_ts:
	docker exec -e KAFKA_OPTS= $(kafka_container) \
		kafka-topics \
		--bootstrap-server $(bootstrap_server) \
		--create \
		--config segment.bytes=$(segment) \
		--config retention.ms=$(retention_ms) \
		--partitions $(partitions) \
		--replication-factor $(replication_factor) \
		--topic $(topic)

num_records = 60000
record_size = 1024
throughput = 1000

.PHONY: fill_topic
fill_topic:
	docker exec -e KAFKA_OPTS= $(kafka_container) \
		kafka-producer-perf-test --producer-props bootstrap.servers=$(bootstrap_server) \
		--topic $(topic) \
		--num-records $(num_records) \
		--record-size $(record_size) \
		--throughput $(throughput)

.PHONY: run_local_fs
run_local_fs:
	docker compose -f compose-local-fs.yml up

.PHONY: run_s3_aws
run_s3_aws:
	docker compose -f compose-s3-aws.yml up

.PHONY: run_s3_minio
run_s3_minio:
	docker compose -f compose-s3-minio.yml up

.PHONY: clean
clean:
	docker compose -f compose-local-fs.yml down
	docker compose -f compose-s3-aws.yml down
	docker compose -f compose-s3-minio.yml down

.PHONY: show_local_data
show_local_data:
	docker exec $(kafka_container) \
		ls -lah /var/lib/kafka/data/$(topic)-0/

.PHONY: show_remote_data_fs
show_remote_data_fs:
	docker exec -e KAFKA_OPTS= $(kafka_container) \
		/usr/bin/bash -c 'ls -lah /home/appuser/kafka-tiered-storage/tiered-storage-demo/$(topic)-*/0/'

.PHONY: show_remote_data_s3_aws
show_remote_data_s3_aws:
	aws s3 ls s3://

.PHONY: show_remote_data_s3_minio
show_remote_data_s3_minio:
	docker run --rm --network=host --entrypoint /usr/bin/bash quay.io/minio/mc \
		-c "mc alias set mycloud http://localhost:9000 minioadmin minioadmin && mc ls --recursive mycloud/test-bucket | grep $(topic)"

offset = 0
consume = 10
.PHONY: consume
consume:
	docker run --rm --network=host edenhill/kcat:1.7.1 \
		-b $(bootstrap_server_host) -C -t $(topic) -c $(consume) -o $(offset) -e -f '%t-%p-%o\n'

.env:
	echo "AWS_ACCESS_KEY_ID=" > .env
	echo "AWS_SECRET_KEY=" >> .env
	echo "KAFKA_RSM_CONFIG_STORAGE_S3_BUCKET_NAME=" >> .env
	echo "KAFKA_RSM_CONFIG_STORAGE_S3_REGION=" >> .env

.PHONY: rsa_keys
rsa_keys: private.pem public.pem

private.pem:
	openssl genrsa -out $@ 512

public.pem: private.pem
	openssl rsa -in $< -outform PEM -out $@ -pubout
