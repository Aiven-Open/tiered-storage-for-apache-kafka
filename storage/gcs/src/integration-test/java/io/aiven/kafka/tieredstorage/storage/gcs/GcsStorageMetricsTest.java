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

package io.aiven.kafka.tieredstorage.storage.gcs;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.Map;

import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;
import io.aiven.kafka.tieredstorage.storage.TestObjectKey;
import io.aiven.kafka.tieredstorage.storage.TestUtils;
import io.aiven.testcontainers.fakegcsserver.FakeGcsServerContainer;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;

@ExtendWith(MockitoExtension.class)
@Testcontainers
public class GcsStorageMetricsTest {
    private static final int RESUMABLE_UPLOAD_CHUNK_SIZE = 256 * 1024;

    @Container
    static final FakeGcsServerContainer GCS_SERVER = new FakeGcsServerContainer();

    static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();
    static Storage storageClient;

    GcsStorage storage;

    @BeforeAll
    static void setUpClass() throws Exception {
        storageClient = StorageOptions.newBuilder()
            .setCredentials(NoCredentials.getInstance())
            .setHost(GCS_SERVER.url())
            .setProjectId("test-project")
            .build()
            .getService();
    }

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        final String bucketName = TestUtils.testNameToBucketName(testInfo);
        storageClient.create(BucketInfo.newBuilder(bucketName).build());

        storage = new GcsStorage();
        final Map<String, Object> configs = Map.of(
            "gcs.bucket.name", bucketName,
            "gcs.endpoint.url", GCS_SERVER.url(),
            "gcs.resumable.upload.chunk.size", Integer.toString(RESUMABLE_UPLOAD_CHUNK_SIZE),
            "gcs.credentials.default", "false"
        );
        storage.configure(configs);
    }

    @Test
    void metricsShouldBeReported() throws StorageBackendException, IOException, JMException {
        final byte[] data = new byte[RESUMABLE_UPLOAD_CHUNK_SIZE + 1];

        final ObjectKey key = new TestObjectKey("x");

        storage.upload(new ByteArrayInputStream(data), key);
        try (final InputStream fetch = storage.fetch(key)) {
            fetch.readAllBytes();
        }
        try (final InputStream fetch = storage.fetch(key, BytesRange.of(0, 1))) {
            fetch.readAllBytes();
        }
        storage.delete(key);

        final ObjectName gcsMetricsObjectName =
            ObjectName.getInstance("aiven.kafka.server.tieredstorage.gcs:type=gcs-client-metrics");
        assertThat(MBEAN_SERVER.getAttribute(gcsMetricsObjectName, "object-metadata-get-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(gcsMetricsObjectName, "object-metadata-get-total"))
            .isEqualTo(2.0);

        assertThat(MBEAN_SERVER.getAttribute(gcsMetricsObjectName, "object-get-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(gcsMetricsObjectName, "object-get-total"))
            .isEqualTo(2.0);

        assertThat(MBEAN_SERVER.getAttribute(gcsMetricsObjectName, "resumable-upload-initiate-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(gcsMetricsObjectName, "resumable-upload-initiate-total"))
            .isEqualTo(1.0);

        assertThat(MBEAN_SERVER.getAttribute(gcsMetricsObjectName, "resumable-chunk-upload-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(gcsMetricsObjectName, "resumable-chunk-upload-total"))
            .isEqualTo(2.0);

        assertThat(MBEAN_SERVER.getAttribute(gcsMetricsObjectName, "object-delete-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(gcsMetricsObjectName, "object-delete-total"))
            .isEqualTo(1.0);
    }
}
