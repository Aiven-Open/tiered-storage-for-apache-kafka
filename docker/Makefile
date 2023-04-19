all:

# Generate keys for S3 plugin
rsa-keys:
	openssl genrsa -out private.pem 512
	openssl rsa -in private.pem -outform PEM -out public.pem -pubout

# Topic Defaults
t=t1
p=6
segment=100000000 # 100M
retention=1000000000 # 1G
local_retention=300000000 # 300M

kafka-topic:
	${KAFKA_HOME}/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create \
		--config remote.storage.enable=true \
		--config segment.bytes=$(segment) \
		--config retention.bytes=$(retention) \
		--config local.retention.bytes=$(local_retention) \
		--partitions $(p) \
		--topic $(t)
