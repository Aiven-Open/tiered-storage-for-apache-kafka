all: build up

build:
	docker compose build

up:
	docker compose up -d

# Generate keys for S3 plugin
rsa-keys:
	openssl genrsa -out private.pem 512
	openssl rsa -in private.pem -outform PEM -out public.pem -pubout

topic:
	${KAFKA_HOME}/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create \
		--config segment.bytes=10000000 \
		--config retention.bytes=500000000 \
		--config remote.storage.enable=true \
		--config local.retention.bytes=50000000 \
		--partitions 6 \
		--topic t1

localstack: localstack_profile localstack_s3_bucket

localstack_profile:
	aws configure --profile localstack

localstack_s3_bucket:
	aws s3 --endpoint-url http://localhost:4566 --profile localstack mb s3://kafka-ts-v1
