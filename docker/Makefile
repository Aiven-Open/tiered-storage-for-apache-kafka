all:

# Defaults
t=t1
segment=10000000 # 10M
retention=500000000 # 500M
local_retention=50000000 # 50M

ts-topic:
	${KAFKA_HOME}/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create \
		--config remote.storage.enable=true \
		--config segment.bytes=$(segment) \
		--config retention.bytes=$(retention) \
		--config local.retention.bytes=$(local_retention) \
		--partitions 6 \
		--topic $(t)
