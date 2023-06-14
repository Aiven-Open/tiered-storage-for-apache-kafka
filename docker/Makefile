all:

# Generate keys for S3 plugin
rsa_keys: private.pem public.pem
	openssl genrsa -out private.pem 512
	openssl rsa -in private.pem -outform PEM -out public.pem -pubout

# Topic Defaults
t=t1
p=6
segment=100000000 # 100M
retention=1000000000 # 1G
local_retention=300000000 # 300M

.PHONY: create_topic
create_topic:
	docker exec kafka-ts kafka-topics \
		--bootstrap-server kafka:29092 \
		--create \
		--config remote.storage.enable=true \
		--config segment.bytes=$(segment) \
		--config retention.bytes=$(retention) \
		--config local.retention.bytes=$(local_retention) \
		--partitions $(p) \
		--topic $(t)

.PHONY: fill_topic
fill_topic:
	docker exec kafka-ts kafka-producer-perf-test --producer-props bootstrap.servers=localhost:9092 \
	    --topic t1 \
	    --num-records 1000000 \
	    --record-size 10000 \
	    --throughput -1

.PHONY: run_kafka_filesystem
run_kafka_filesystem:
	cd filesystem; docker compose up -d

.PHONY: run_kafka_s3
run_kafka_s3:
	cd s3; docker compose up -d

.PHONY: run_kafka_s3_minio
run_kafka_s3_minio:
	cd s3-minio; docker compose up -d

clean:
	cd filesystem; docker compose down
	cd test; docker compose down
	cd s3; docker compose down
	cd s3-minio; docker compose down

.PHONY: check_local_data
check_local_data:
	docker exec kafka-ts ls -lh /var/lib/kafka/data/t1-0

.PHONY: check_fs_remote_data
check_fs_remote_data:
	docker exec kafka-ts ls -lh /home/appuser/kafka/kafka-tiered-storage/tiered-storage-demo/

# add arguments to check specific partition / offset e.g. "-p 0 -o 120000"
args=
.PHONY: consume
consume:
	kcat -b localhost:9092 -C -t t1 -c 10 -f '%t-%p-%o\n' $(args)
