FROM eclipse-temurin:17 AS plugin

ENV PLUGIN_VERSION 0.0.1-SNAPSHOT

COPY . /kafka-ts-plugin

# Build TS plugin
WORKDIR /kafka-ts-plugin
RUN ./gradlew releaseTarGz

# Unpack release distribution
WORKDIR /kafka-ts-plugin/build/distributions
RUN tar -xf tiered-storage-for-apache-kafka-${PLUGIN_VERSION}.tgz

FROM fedora:37 AS base

ENV JAVA_VERSION 17
RUN dnf install -y java-${JAVA_VERSION}-openjdk-devel git

ENV JAVA_HOME /usr/lib/jvm/java-${JAVA_VERSION}-openjdk
# Working with a fully functional TS branch by default
# - upstream doesn't have a fully working TS
#   - https://github.com/apache/kafka/pull/13535
ARG AK_REPO=https://github.com/aiven/kafka
ARG AK_BRANCH=3.3-2022-10-06-tiered-storage
ARG KAFKA_VERSION=3.3.2-SNAPSHOT

RUN git clone ${AK_REPO}
WORKDIR /kafka
RUN git checkout ${AK_BRANCH}

ARG SCALA_VERSION=2.13

RUN ./gradlew -PscalaVersion=${SCALA_VERSION} testJar releaseTarGz
WORKDIR /kafka/core/build/distributions
RUN tar xf kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz;

FROM eclipse-temurin:17-jre AS kafka

ARG KAFKA_VERSION=3.3.2-SNAPSHOT
ARG SCALA_VERSION=2.13
ENV PLUGIN_VERSION 0.0.1-SNAPSHOT

COPY --from=base /kafka/core/build/distributions/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /kafka
# Adding test jars with local implementations
COPY --from=base /kafka/clients/build/libs/kafka-clients-${KAFKA_VERSION}-test.jar /kafka/libs/
COPY --from=base /kafka/storage/build/libs/kafka-storage-${KAFKA_VERSION}-test.jar /kafka/libs/
COPY --from=base /kafka/storage/api/build/libs/kafka-storage-api-${KAFKA_VERSION}-test.jar /kafka/libs/

COPY --from=plugin /kafka-ts-plugin/build/distributions/tiered-storage-for-apache-kafka-${PLUGIN_VERSION} /kafka-ts-plugin

ENV KAFKA_HOME /kafka
WORKDIR ${KAFKA_HOME}
ENV PATH $PATH:${KAFKA_HOME}/bin
CMD [ "kafka-server-start.sh" ]