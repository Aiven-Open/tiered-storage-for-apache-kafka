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

import java.time.OffsetDateTime;
import java.util.Map;

import io.aiven.kafka.tieredstorage.storage.StorageBackend;

import com.azure.storage.blob.sas.BlobContainerSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;

class AzureBlobStorageSasTokenTest extends AzureBlobStorageTest {
    @Override
    protected StorageBackend storage() {
        final var permissions = new BlobContainerSasPermission()
            .setCreatePermission(true)
            .setDeletePermission(true)
            .setReadPermission(true)
            .setWritePermission(true);
        final var sasSignatureValues =
            new BlobServiceSasSignatureValues(OffsetDateTime.now().plusDays(1), permissions)
            .setStartTime(OffsetDateTime.now().minusMinutes(5));
        final String sasToken = blobServiceClient.getBlobContainerClient(azureContainerName)
            .generateSas(sasSignatureValues);

        final AzureBlobStorage azureBlobStorage = new AzureBlobStorage();
        final Map<String, Object> configs = Map.of(
            "azure.container.name", azureContainerName,
            "azure.sas.token", sasToken,
            "azure.endpoint.url", endpoint()
        );
        azureBlobStorage.configure(configs);
        return azureBlobStorage;
    }
}
