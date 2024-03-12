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

import java.io.ByteArrayInputStream;

import io.aiven.kafka.tieredstorage.storage.BaseStorageTest;
import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.InvalidRangeException;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static io.aiven.kafka.tieredstorage.storage.azure.AzuriteBlobStorageUtils.azuriteContainer;
import static io.aiven.kafka.tieredstorage.storage.azure.AzuriteBlobStorageUtils.connectionString;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Testcontainers
abstract class AzureBlobStorageTest extends BaseStorageTest {
    static final int BLOB_STORAGE_PORT = 10000;
    @Container
    static final GenericContainer<?> AZURITE_SERVER = azuriteContainer(BLOB_STORAGE_PORT);

    static BlobServiceClient blobServiceClient;

    protected String azureContainerName;

    @BeforeAll
    static void setUpClass() {
        blobServiceClient = new BlobServiceClientBuilder()
            .connectionString(connectionString(AZURITE_SERVER, BLOB_STORAGE_PORT))
            .buildClient();
    }

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        azureContainerName = testInfo.getDisplayName()
            .toLowerCase()
            .replace("(", "")
            .replace(")", "");
        while (azureContainerName.length() < 3) {
            azureContainerName += azureContainerName;
        }
        blobServiceClient.createBlobContainer(azureContainerName);
    }

    @Override
    protected void testFetchWithRangeOutsideFileSize() throws StorageBackendException {
        // For some reason, Azure (or only Azurite) considers range 3-5 valid for a 3-byte blob,
        // so the generic test fails.
        final String content = "ABC";
        storage().upload(new ByteArrayInputStream(content.getBytes()), TOPIC_PARTITION_SEGMENT_KEY);

        assertThatThrownBy(() -> storage().fetch(TOPIC_PARTITION_SEGMENT_KEY, BytesRange.of(4, 6)))
            .isInstanceOf(InvalidRangeException.class);
    }
}
