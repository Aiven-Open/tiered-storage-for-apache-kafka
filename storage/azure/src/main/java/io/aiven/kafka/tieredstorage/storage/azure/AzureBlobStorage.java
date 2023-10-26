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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.InvalidRangeException;
import io.aiven.kafka.tieredstorage.storage.KeyNotFoundException;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.blob.specialized.SpecializedBlobClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;

public class AzureBlobStorage implements StorageBackend {
    private AzureBlobStorageConfig config;
    private BlobContainerClient blobContainerClient;

    // TODO make configurable
    private final int blockSize = 5 * 1024 * 1024;

    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = new AzureBlobStorageConfig(configs);

        final BlobServiceClientBuilder blobServiceClientBuilder = new BlobServiceClientBuilder();
        if (config.connectionString() != null) {
            blobServiceClientBuilder.connectionString(config.connectionString());
        } else {
            blobServiceClientBuilder.endpoint(endpointUrl());

            if (config.accountKey() != null) {
                blobServiceClientBuilder.credential(
                    new StorageSharedKeyCredential(config.accountName(), config.accountKey()));
            } else if (config.sasToken() != null) {
                blobServiceClientBuilder.sasToken(config.sasToken());
            } else {
                blobServiceClientBuilder.credential(
                    new DefaultAzureCredentialBuilder().build());
            }
        }

        blobContainerClient = blobServiceClientBuilder.buildClient()
            .getBlobContainerClient(config.containerName());
    }

    private String endpointUrl() {
        if (config.endpointUrl() != null) {
            return config.endpointUrl();
        } else {
            return "https://" + config.accountName() + ".blob.core.windows.net";
        }
    }

    @Override
    public long upload(final InputStream inputStream, final ObjectKey key) throws StorageBackendException {
        final var specializedBlobClientBuilder = new SpecializedBlobClientBuilder();
        if (config.connectionString() != null) {
            specializedBlobClientBuilder.connectionString(config.connectionString());
        } else {
            specializedBlobClientBuilder.endpoint(endpointUrl());

            if (config.accountKey() != null) {
                specializedBlobClientBuilder.credential(
                    new StorageSharedKeyCredential(config.accountName(), config.accountKey()));
            } else if (config.sasToken() != null) {
                specializedBlobClientBuilder.sasToken(config.sasToken());
            } else {
                specializedBlobClientBuilder.credential(
                    new DefaultAzureCredentialBuilder().build());
            }
        }
        final BlockBlobClient blockBlobClient = specializedBlobClientBuilder
            .containerName(config.containerName())
            .blobName(key.value())
            .buildBlockBlobClient();

        try (OutputStream os = new BufferedOutputStream(blockBlobClient.getBlobOutputStream(true), blockSize)) {
            return inputStream.transferTo(os);
        } catch (final IOException e) {
            throw new StorageBackendException("Failed to upload " + key, e);
        }
    }

    @Override
    public InputStream fetch(final ObjectKey key) throws StorageBackendException {
        try {
            return blobContainerClient.getBlobClient(key.value()).openInputStream();
        } catch (final BlobStorageException e) {
            if (e.getStatusCode() == 404) {
                throw new KeyNotFoundException(this, key, e);
            } else {
                throw new StorageBackendException("Failed to fetch " + key, e);
            }
        }
    }

    @Override
    public InputStream fetch(final ObjectKey key, final BytesRange range) throws StorageBackendException {
        try {
            return blobContainerClient.getBlobClient(key.value()).openInputStream(
                new BlobRange(range.from, (long) range.size()), null);
        } catch (final BlobStorageException e) {
            if (e.getStatusCode() == 404) {
                throw new KeyNotFoundException(this, key, e);
            } else if (e.getStatusCode() == 416) {
                throw new InvalidRangeException("Invalid range " + range, e);
            } else {
                throw new StorageBackendException("Failed to fetch " + key, e);
            }
        }
    }

    @Override
    public void delete(final ObjectKey key) throws StorageBackendException {
        try {
            blobContainerClient.getBlobClient(key.value()).deleteIfExists();
        } catch (final BlobStorageException e) {
            throw new StorageBackendException("Failed to delete " + key, e);
        }
    }

    @Override
    public String toString() {
        return "AzureStorage{"
            + "containerName='" + config.containerName() + '\''
            + '}';
    }
}
