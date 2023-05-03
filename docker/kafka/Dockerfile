FROM fedora:37 AS base

ENV JAVA_VERSION 11
RUN dnf install -y java-${JAVA_VERSION}-openjdk-devel git

ENV JAVA_HOME /usr/lib/jvm/java-${JAVA_VERSION}-openjdk
# Working with a fully functional TS branch by default
# - upstream doesn't have a fully working TS
#   - https://github.com/apache/kafka/pull/13535
# - aiven's 3.3 has issue with fetching
#   - https://github.com/aiven/kafka/issues/15
ARG AK_REPO=https://github.com/aiven/kafka
ARG AK_BRANCH=3.0-2022-03-31-tiered-storage
ARG KAFKA_VERSION=3.0.1-SNAPSHOT

RUN git clone ${AK_REPO}
WORKDIR /kafka
RUN git checkout ${AK_BRANCH}

ARG SCALA_VERSION=2.12

RUN ./gradlew -PscalaVersion=${SCALA_VERSION} testJar releaseTarGz
WORKDIR /kafka/core/build/distributions
RUN tar xf kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz;

# Kafka 3.0.x
FROM confluentinc/cp-kafka:7.0.9 AS kafka

ARG SCALA_VERSION=2.12
ARG KAFKA_VERSION=3.0.1-SNAPSHOT

USER root
RUN rm -r /usr/share/java/kafka/*
COPY --from=base /kafka/core/build/distributions/kafka_${SCALA_VERSION}-${KAFKA_VERSION}/libs /usr/share/java/kafka/
RUN ln -s /usr/share/java/kafka/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.jar /usr/share/java/kafka/kafka.jar
# Adding test jars with local implementations
COPY --from=base /kafka/clients/build/libs/kafka-clients-${KAFKA_VERSION}-test.jar /usr/share/java/kafka/
COPY --from=base /kafka/storage/build/libs/kafka-storage-${KAFKA_VERSION}-test.jar /usr/share/java/kafka/
COPY --from=base /kafka/storage/api/build/libs/kafka-storage-api-${KAFKA_VERSION}-test.jar /usr/share/java/kafka/

USER appuser
