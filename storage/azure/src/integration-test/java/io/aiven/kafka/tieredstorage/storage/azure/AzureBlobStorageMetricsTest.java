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

package io.aiven.kafka.tieredstorage.storage.azure;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.stream.Stream;

import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;
import io.aiven.kafka.tieredstorage.storage.TestObjectKey;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static io.aiven.kafka.tieredstorage.storage.azure.AzuriteBlobStorageUtils.azuriteContainer;
import static io.aiven.kafka.tieredstorage.storage.azure.AzuriteBlobStorageUtils.connectionString;
import static io.aiven.kafka.tieredstorage.storage.azure.AzuriteBlobStorageUtils.endpoint;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;

@Testcontainers
public class AzureBlobStorageMetricsTest {
    static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();
    private static final int UPLOAD_BLOCK_SIZE = 256 * 1024;
    private static final int BLOB_STORAGE_PORT = 10000;
    @Container
    static final GenericContainer<?> AZURITE_SERVER = azuriteContainer(BLOB_STORAGE_PORT);

    static BlobServiceClient blobServiceClient;

    protected String azureContainerName;

    @BeforeAll
    static void setUpClass() {
        // Generally setting JVM-wide trust store needed only for one test may be not OK,
        // but it's not conflicting with any other test now and this is the most straightforward way
        // to make the self-signed certificate work.
        System.setProperty("javax.net.ssl.trustStore",
            AzureBlobStorageMetricsTest.class.getResource("/azurite-cacerts.jks").getPath());
        System.setProperty("javax.net.ssl.trustStorePassword", "changeit");
        blobServiceClient = new BlobServiceClientBuilder()
            .connectionString(connectionString(AZURITE_SERVER, BLOB_STORAGE_PORT))
            .buildClient();
    }

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        azureContainerName = testInfo.getDisplayName()
            .toLowerCase()
            .replace(" ", "")
            .replace(",", "-")
            .replace("(", "")
            .replace(")", "")
            .replace("[", "")
            .replace("]", "");
        while (azureContainerName.length() < 3) {
            azureContainerName += azureContainerName;
        }
        blobServiceClient.createBlobContainer(azureContainerName);
    }

    StorageBackend storage() {
        final AzureBlobStorage azureBlobStorage = new AzureBlobStorage();
        // The well-known Azurite account name and key.
        final String accountName = "devstoreaccount1";
        final String accountKey =
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
        final Map<String, Object> configs = Map.of(
            "azure.container.name", azureContainerName,
            "azure.account.name", accountName,
            "azure.account.key", accountKey,
            "azure.endpoint.url", endpoint(AZURITE_SERVER, BLOB_STORAGE_PORT),
            "azure.upload.block.size", UPLOAD_BLOCK_SIZE
        );
        azureBlobStorage.configure(configs);
        return azureBlobStorage;
    }

    static Stream<Arguments> metricsShouldBeReported() {
        return Stream.of(
            Arguments.of(
                Named.of("smaller-than-block-size-payload", UPLOAD_BLOCK_SIZE - 1),
                1, 0, 0),
            Arguments.of(
                Named.of("larger-than-block-size-payload", UPLOAD_BLOCK_SIZE + 1),
                0, 2, 1)
        );
    }

    @ParameterizedTest
    @MethodSource
    void metricsShouldBeReported(
        final int uploadBlockSize,
        final double expectedPutBlob,
        final double expectedPutBlock,
        final double expectedPutBlockList
    ) throws StorageBackendException, IOException, JMException {
        final byte[] data = new byte[uploadBlockSize];

        final ObjectKey key = new TestObjectKey("x");

        final var storage = storage();

        storage.upload(new ByteArrayInputStream(data), key);
        try (final InputStream fetch = storage.fetch(key)) {
            fetch.readAllBytes();
        }
        try (final InputStream fetch = storage.fetch(key, BytesRange.of(0, 1))) {
            fetch.readAllBytes();
        }
        storage.delete(key);

        final ObjectName objectName =
            ObjectName.getInstance("aiven.kafka.server.tieredstorage.azure:type=azure-blob-metrics");
        assertThat(MBEAN_SERVER.getAttribute(objectName, "blob-get-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(objectName, "blob-get-total"))
            .isEqualTo(2.0);

        if (expectedPutBlob > 0) {
            assertThat(MBEAN_SERVER.getAttribute(objectName, "blob-upload-rate"))
                .asInstanceOf(DOUBLE)
                .isGreaterThan(0.0);
        }
        assertThat(MBEAN_SERVER.getAttribute(objectName, "blob-upload-total"))
            .isEqualTo(expectedPutBlob);

        if (expectedPutBlock > 0) {
            assertThat(MBEAN_SERVER.getAttribute(objectName, "block-upload-rate"))
                .asInstanceOf(DOUBLE)
                .isGreaterThan(0.0);
        }
        assertThat(MBEAN_SERVER.getAttribute(objectName, "block-upload-total"))
            .isEqualTo(expectedPutBlock);

        if (expectedPutBlockList > 0) {
            assertThat(MBEAN_SERVER.getAttribute(objectName, "block-list-upload-rate"))
                .asInstanceOf(DOUBLE)
                .isGreaterThan(0.0);
        }
        assertThat(MBEAN_SERVER.getAttribute(objectName, "block-list-upload-total"))
            .isEqualTo(expectedPutBlockList);

        assertThat(MBEAN_SERVER.getAttribute(objectName, "blob-delete-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
        assertThat(MBEAN_SERVER.getAttribute(objectName, "blob-delete-total"))
            .isEqualTo(1.0);
    }
}
