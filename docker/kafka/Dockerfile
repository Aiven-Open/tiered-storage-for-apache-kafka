FROM fedora:37 AS base

ENV JAVA_VERSION 17
RUN dnf install -y java-${JAVA_VERSION}-openjdk-devel git

ENV JAVA_HOME /usr/lib/jvm/java-${JAVA_VERSION}-openjdk
ARG AK_REPO=https://github.com/apache/kafka
ARG AK_BRANCH=3.4

ENV SCALA_VERSION 2.13
ARG KAFKA_VERSION=3.4.1-SNAPSHOT

RUN git clone ${AK_REPO}

WORKDIR /kafka
RUN git checkout ${AK_BRANCH}

RUN ./gradlew clean testJar releaseTarGz
WORKDIR /kafka/core/build/distributions
RUN tar xf kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz;

FROM eclipse-temurin:17-jre AS kafka

ENV SCALA_VERSION 2.13
ARG KAFKA_VERSION=3.4.1-SNAPSHOT

COPY --from=base /kafka/core/build/distributions/kafka_${SCALA_VERSION}-${KAFKA_VERSION} /kafka
# Adding test jars with local implementations
COPY --from=base /kafka/clients/build/libs/kafka-clients-${KAFKA_VERSION}-test.jar /kafka/libs/
COPY --from=base /kafka/storage/build/libs/kafka-storage-${KAFKA_VERSION}-test.jar /kafka/libs/
COPY --from=base /kafka/storage/api/build/libs/kafka-storage-api-${KAFKA_VERSION}-test.jar /kafka/libs/

ENV KAFKA_HOME /kafka
WORKDIR ${KAFKA_HOME}
ENV PATH $PATH:${KAFKA_HOME}/bin
CMD [ "kafka-server-start.sh" ]