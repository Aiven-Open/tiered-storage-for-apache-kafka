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
import java.net.InetSocketAddress;
import java.util.Map;

import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.InvalidRangeException;
import io.aiven.kafka.tieredstorage.storage.KeyNotFoundException;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;
import io.aiven.kafka.tieredstorage.storage.proxy.ProxyConfig;

import com.azure.core.http.ProxyOptions;
import com.azure.core.util.HttpClientOptions;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.options.BlockBlobOutputStreamOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.blob.specialized.SpecializedBlobClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import reactor.core.Exceptions;

public class AzureBlobStorage implements StorageBackend {
    private AzureBlobStorageConfig config;
    private BlobContainerClient blobContainerClient;
    private MetricCollector metricsPolicy;
    private ProxyOptions proxyOptions = null;

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

        metricsPolicy = new MetricCollector(config);

        final ProxyConfig proxyConfig = config.proxyConfig();
        if (proxyConfig != null) {
            proxyOptions = new ProxyOptions(ProxyOptions.Type.SOCKS5,
                new InetSocketAddress(proxyConfig.host(), proxyConfig.port()));
            if (proxyConfig.username() != null) {
                proxyOptions.setCredentials(proxyConfig.username(), proxyConfig.password());
            }
            blobServiceClientBuilder.clientOptions(new HttpClientOptions().setProxyOptions(proxyOptions));
        }

        blobContainerClient = blobServiceClientBuilder
            .addPolicy(metricsPolicy.policy())
            .buildClient()
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

        if (proxyOptions != null) {
            specializedBlobClientBuilder.clientOptions(new HttpClientOptions().setProxyOptions(proxyOptions));
        }

        final BlockBlobClient blockBlobClient = specializedBlobClientBuilder
            .addPolicy(metricsPolicy.policy())
            .containerName(config.containerName())
            .blobName(key.value())
            .buildBlockBlobClient();

        final long blockSizeLong = config.uploadBlockSize();
        final ParallelTransferOptions parallelTransferOptions = new ParallelTransferOptions()
            .setBlockSizeLong(blockSizeLong);
        // Setting this is important, because otherwise if the size is below 256 MiB,
        // block upload won't be used and up to 256 MiB may be cached in memory.
        parallelTransferOptions.setMaxSingleUploadSizeLong(blockSizeLong);
        final BlockBlobOutputStreamOptions options = new BlockBlobOutputStreamOptions()
            .setParallelTransferOptions(parallelTransferOptions);
        // Be aware that metrics instrumentation is based on PutBlob (single upload), PutBlock (upload part),
        // and PutBlockList (complete upload) used by this call.
        // If upload changes, change metrics instrumentation accordingly.
        try (OutputStream os = new BufferedOutputStream(
            blockBlobClient.getBlobOutputStream(options), config.uploadBlockSize())) {
            return inputStream.transferTo(os);
        } catch (final IOException e) {
            throw new StorageBackendException("Failed to upload " + key, e);
        } catch (final RuntimeException e) {
            throw unwrapReactorExceptions(e, "Failed to upload " + key);
        }
    }

    @Override
    public InputStream fetch(final ObjectKey key) throws StorageBackendException {
        try {
            return blobContainerClient.getBlobClient(key.value())
                .openInputStream();
        } catch (final BlobStorageException e) {
            if (e.getStatusCode() == 404) {
                throw new KeyNotFoundException(this, key, e);
            } else {
                throw new StorageBackendException("Failed to fetch " + key, e);
            }
        } catch (final RuntimeException e) {
            throw unwrapReactorExceptions(e, "Failed to fetch " + key);
        }
    }

    @Override
    public InputStream fetch(final ObjectKey key, final BytesRange range) throws StorageBackendException {
        try {
            if (range.isEmpty()) {
                return InputStream.nullInputStream();
            }

            return blobContainerClient.getBlobClient(key.value()).openInputStream(
                new BlobRange(range.firstPosition(), (long) range.size()), null);
        } catch (final BlobStorageException e) {
            if (e.getStatusCode() == 404) {
                throw new KeyNotFoundException(this, key, e);
            } else if (e.getStatusCode() == 416) {
                throw new InvalidRangeException("Invalid range " + range, e);
            } else {
                throw new StorageBackendException("Failed to fetch " + key, e);
            }
        } catch (final RuntimeException e) {
            throw unwrapReactorExceptions(e, "Failed to fetch " + key);
        }
    }

    @Override
    public void delete(final ObjectKey key) throws StorageBackendException {
        try {
            blobContainerClient.getBlobClient(key.value()).deleteIfExists();
        } catch (final BlobStorageException e) {
            throw new StorageBackendException("Failed to delete " + key, e);
        } catch (final RuntimeException e) {
            throw unwrapReactorExceptions(e, "Failed to delete " + key);
        }
    }

    private StorageBackendException unwrapReactorExceptions(final RuntimeException e, final String message) {
        final Throwable unwrapped = Exceptions.unwrap(e);
        if (unwrapped != e) {
            return new StorageBackendException(message, unwrapped);
        } else {
            throw e;
        }
    }

    @Override
    public String toString() {
        return "AzureStorage{"
            + "containerName='" + config.containerName() + '\''
            + '}';
    }
}
