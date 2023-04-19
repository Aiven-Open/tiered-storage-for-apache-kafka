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

FROM eclipse-temurin:11-jre AS kafka

ARG SCALA_VERSION=2.12
ARG KAFKA_VERSION=3.0.1-SNAPSHOT

COPY --from=base /kafka/core/build/distributions/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /kafka
# Adding test jars with local implementations
COPY --from=base /kafka/clients/build/libs/kafka-clients-${KAFKA_VERSION}-test.jar /kafka/libs/
COPY --from=base /kafka/storage/build/libs/kafka-storage-${KAFKA_VERSION}-test.jar /kafka/libs/
COPY --from=base /kafka/storage/api/build/libs/kafka-storage-api-${KAFKA_VERSION}-test.jar /kafka/libs/

ENV KAFKA_HOME /kafka
WORKDIR ${KAFKA_HOME}
ENV PATH $PATH:${KAFKA_HOME}/bin
CMD [ "kafka-server-start.sh" ]