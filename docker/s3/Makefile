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
		--config segment.bytes=1000000 \
		--config retention.bytes=100000000 \
		--config remote.storage.enable=true \
		--config local.retention.bytes=2000000 \
		--partitions 6 \
		--topic t1
