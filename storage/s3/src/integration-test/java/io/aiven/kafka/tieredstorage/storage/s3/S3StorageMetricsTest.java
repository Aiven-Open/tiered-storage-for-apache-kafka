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

package io.aiven.kafka.tieredstorage.storage.s3;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.Map;

import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@Testcontainers
class S3StorageMetricsTest {
    @Container
    public static final LocalStackContainer LOCALSTACK = S3TestContainer.container();

    static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();

    public static final int PART_SIZE = 5 * 1024 * 1024;

    static AmazonS3 s3Client;
    static String bucketName = "test-bucket";

    S3Storage storage;

    @BeforeAll
    static void setupS3() {
        s3Client = AmazonS3ClientBuilder
            .standard()
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(
                    LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3).toString(),
                    LOCALSTACK.getRegion()
                )
            )
            .withCredentials(
                new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())
                )
            )
            .build();
        s3Client.createBucket(bucketName);
    }

    @BeforeEach
    void setupStorage() {
        storage = new S3Storage();
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", bucketName,
            "s3.region", LOCALSTACK.getRegion(),
            "s3.endpoint.url", LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3).toString(),
            "s3.path.style.access.enabled", true,
            "s3.multipart.upload.part.size", PART_SIZE
        );
        storage.configure(configs);
    }

    @Test
    void metricsShouldBeReported() throws StorageBackendException, IOException, JMException {
        final byte[] data = new byte[PART_SIZE + 1];

        final String key = "x";

        storage.upload(new ByteArrayInputStream(data), key);
        try (final InputStream fetch = storage.fetch(key)) {
            fetch.readAllBytes();
        }
        try (final InputStream fetch = storage.fetch(key, BytesRange.of(0, 1))) {
            fetch.readAllBytes();
        }
        storage.delete(key);

        final InputStream failingInputStream = mock(InputStream.class);
        final IOException exception = new IOException("test");
        when(failingInputStream.transferTo(any())).thenThrow(exception);
        assertThatThrownBy(() -> storage.upload(failingInputStream, key))
            .hasRootCause(exception);

        final ObjectName segmentCopyPerSecName = ObjectName.getInstance(
            "aiven.kafka.server.tieredstorage.s3:type=s3-metrics");
        assertThat((double) MBEAN_SERVER.getAttribute(
            segmentCopyPerSecName, "get-object-requests-rate"))
            .isGreaterThan(0.0);
        assertThat((double) MBEAN_SERVER.getAttribute(
            segmentCopyPerSecName, "get-object-requests-total"))
            .isEqualTo(2.0);

        assertThat((double) MBEAN_SERVER.getAttribute(
            segmentCopyPerSecName, "put-object-requests-rate"))
            .isEqualTo(0.0);
        assertThat((double) MBEAN_SERVER.getAttribute(
            segmentCopyPerSecName, "put-object-requests-total"))
            .isEqualTo(0.0);

        assertThat((double) MBEAN_SERVER.getAttribute(
            segmentCopyPerSecName, "delete-object-requests-rate"))
            .isGreaterThan(0.0);
        assertThat((double) MBEAN_SERVER.getAttribute(
            segmentCopyPerSecName, "delete-object-requests-total"))
            .isEqualTo(1.0);

        assertThat((double) MBEAN_SERVER.getAttribute(
            segmentCopyPerSecName, "create-multipart-upload-requests-rate"))
            .isGreaterThan(0.0);
        assertThat((double) MBEAN_SERVER.getAttribute(
            segmentCopyPerSecName, "create-multipart-upload-requests-total"))
            .isEqualTo(2.0);

        assertThat((double) MBEAN_SERVER.getAttribute(
            segmentCopyPerSecName, "upload-part-requests-rate"))
            .isGreaterThan(0.0);
        assertThat((double) MBEAN_SERVER.getAttribute(
            segmentCopyPerSecName, "upload-part-requests-total"))
            .isEqualTo(2.0);

        assertThat((double) MBEAN_SERVER.getAttribute(
            segmentCopyPerSecName, "complete-multipart-upload-requests-rate"))
            .isGreaterThan(0.0);
        assertThat((double) MBEAN_SERVER.getAttribute(
            segmentCopyPerSecName, "complete-multipart-upload-requests-total"))
            .isEqualTo(1.0);

        assertThat((double) MBEAN_SERVER.getAttribute(
            segmentCopyPerSecName, "abort-multipart-upload-requests-rate"))
            .isGreaterThan(0.0);
        assertThat((double) MBEAN_SERVER.getAttribute(
            segmentCopyPerSecName, "abort-multipart-upload-requests-total"))
            .isEqualTo(1.0);
    }
}
