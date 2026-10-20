/*
 * Copyright 2023 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.tieredstorage.e2e;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;

import io.aiven.kafka.tieredstorage.storage.S3Container;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

public class S3SingleBrokerTest extends SingleBrokerTest {

    static final String S3_NETWORK_ALIAS = "s3";
    static final S3Container S3 = new S3Container()
        .withNetwork(NETWORK)
        .withNetworkAliases(S3_NETWORK_ALIAS);
    static final String BUCKET = "test-bucket";

    static S3Client s3Client;

    @BeforeAll
    static void init() throws Exception {
        S3.start();

        initializeS3Client();
        s3Client.createBucket(b -> b.bucket(BUCKET));

        final String endpointUrl = String.format("http://%s:%s", S3_NETWORK_ALIAS, S3Container.S3_PORT);
        setupKafka(kafka -> kafka.withEnv("KAFKA_RSM_CONFIG_STORAGE_BACKEND_CLASS",
                "io.aiven.kafka.tieredstorage.storage.s3.S3Storage")
            .withEnv("KAFKA_REMOTE_LOG_STORAGE_MANAGER_CLASS_PATH",
                "/tiered-storage-for-apache-kafka/core/*:/tiered-storage-for-apache-kafka/s3/*")
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_S3_BUCKET_NAME", BUCKET)
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_S3_REGION", S3Container.REGION)
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_S3_PATH_STYLE_ACCESS_ENABLED", "true")
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_AWS_ACCESS_KEY_ID", S3Container.ACCESS_KEY)
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_AWS_SECRET_ACCESS_KEY", S3Container.SECRET_KEY)
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_S3_ENDPOINT_URL", endpointUrl)
            .dependsOn(S3));
    }

    private static void initializeS3Client() {
        s3Client = S3Client.builder()
            .region(Region.of(S3Container.REGION))
            .endpointOverride(S3.endpoint())
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(S3Container.ACCESS_KEY, S3Container.SECRET_KEY)
                )
            )
            .forcePathStyle(true)
            .build();
    }

    @AfterAll
    static void cleanup() {
        stopKafka();

        S3.stop();

        cleanupStorage();
    }

    @Override
    boolean assertNoTopicDataOnTierStorage(final String topicName, final Uuid topicId) {
        final String prefix = String.format("%s-%s", topicName, topicId.toString());
        final var request = ListObjectsV2Request.builder().bucket(BUCKET).prefix(prefix).build();
        return s3Client.listObjectsV2(request).keyCount() == 0;
    }

    @Override
    List<String> remotePartitionFiles(final TopicIdPartition topicIdPartition) {
        ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(BUCKET).build();
        final List<S3Object> s3Objects = new ArrayList<>();
        ListObjectsV2Response result;
        while ((result = s3Client.listObjectsV2(request)).isTruncated()) {
            s3Objects.addAll(result.contents());
            request = request.toBuilder().continuationToken(result.nextContinuationToken()).build();
        }
        s3Objects.addAll(result.contents());

        return s3Objects.stream()
            .map(S3Object::key)
            .map(k -> k.substring(k.lastIndexOf('/') + 1))
            .sorted()
            .collect(Collectors.toList());
    }
}
