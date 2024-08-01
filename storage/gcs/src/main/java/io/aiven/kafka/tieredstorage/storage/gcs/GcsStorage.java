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

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.Map;

import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.InvalidRangeException;
import io.aiven.kafka.tieredstorage.storage.KeyNotFoundException;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;
import io.aiven.kafka.tieredstorage.storage.proxy.ProxyConfig;
import io.aiven.kafka.tieredstorage.storage.proxy.Socks5ProxyAuthenticator;

import com.google.cloud.BaseServiceException;
import com.google.cloud.ReadChannel;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class GcsStorage implements StorageBackend {
    private Storage storage;
    private String bucketName;
    private String secondaryBucketName;
    private Integer resumableUploadChunkSize;
    private GcsMetrics gcsMetrics;

    @Override
    public void configure(final Map<String, ?> configs) {
        final GcsStorageConfig config = new GcsStorageConfig(configs);
        this.bucketName = config.bucketName();
        this.secondaryBucketName = config.secondaryBucketName();
        this.gcsMetrics = new GcsMetrics();

        final HttpTransportOptions.Builder httpTransportOptionsBuilder = HttpTransportOptions.newBuilder();

        final ProxyConfig proxyConfig = config.proxyConfig();
        if (proxyConfig != null) {
            httpTransportOptionsBuilder.setHttpTransportFactory(
                new ProxiedHttpTransportFactory(proxyConfig.host(), proxyConfig.port())
            );
            if (proxyConfig.username() != null) {
                Socks5ProxyAuthenticator.register(
                    proxyConfig.host(), proxyConfig.port(), proxyConfig.username(), proxyConfig.password());
            }
        }

        final StorageOptions.Builder builder = StorageOptions.newBuilder()
            .setCredentials(config.credentials())
            .setTransportOptions(new MetricCollector().httpTransportOptions(httpTransportOptionsBuilder));
        if (config.endpointUrl() != null) {
            builder.setHost(config.endpointUrl());
        }
        storage = builder.build().getService();

        resumableUploadChunkSize = config.resumableUploadChunkSize();
    }

    @Override
    public long upload(final InputStream inputStream, final ObjectKey key) throws StorageBackendException {
        try {
            final BlobInfo blobInfo = BlobInfo.newBuilder(this.bucketName, key.value()).build();
            final Blob blob;
            if (resumableUploadChunkSize != null) {
                blob = storage.createFrom(blobInfo, inputStream, resumableUploadChunkSize);
            } else {
                blob = storage.createFrom(blobInfo, inputStream);
            }
            return blob.getSize();
        } catch (final IOException | BaseServiceException e) {
            throw new StorageBackendException("Failed to upload " + key, e);
        }
    }

    @Override
    public void delete(final ObjectKey key) throws StorageBackendException {
        try {
            storage.delete(this.bucketName, key.value());
        } catch (final BaseServiceException e) {
            throw new StorageBackendException("Failed to delete " + key, e);
        }
    }

    @Override
    public InputStream fetch(final ObjectKey key) throws StorageBackendException {
        try {
            final Blob blob = getBlob(key);
            final ReadChannel reader = blob.reader();
            return Channels.newInputStream(reader);
        } catch (final BaseServiceException e) {
            if (e.getCode() == 404) {
                // https://cloud.google.com/storage/docs/json_api/v1/status-codes#404_Not_Found
                throw new KeyNotFoundException(this, key, e);
            } else {
                throw new StorageBackendException("Failed to fetch " + key, e);
            }
        }
    }

    @Override
    public InputStream fetch(final ObjectKey key, final BytesRange range) throws StorageBackendException {
        try {
            if (range.isEmpty()) {
                return InputStream.nullInputStream();
            }

            final Blob blob = getBlob(key);

            if (range.firstPosition() >= blob.getSize()) {
                throw new InvalidRangeException("Range start position " + range.firstPosition()
                    + " is outside file content. file size = " + blob.getSize());
            }

            final ReadChannel reader = blob.reader();
            reader.limit(range.lastPosition() + 1);
            reader.seek(range.firstPosition());
            return Channels.newInputStream(reader);
        } catch (final IOException e) {
            throw new StorageBackendException("Failed to fetch " + key, e);
        } catch (final BaseServiceException e) {
            if (e.getCode() == 404) {
                // https://cloud.google.com/storage/docs/json_api/v1/status-codes#404_Not_Found
                throw new KeyNotFoundException(this, key, e);
            } else if (e.getCode() == 416) {
                // https://cloud.google.com/storage/docs/json_api/v1/status-codes#416_Requested_Range_Not_Satisfiable
                throw new InvalidRangeException("Invalid range " + range, e);
            } else {
                throw new StorageBackendException("Failed to fetch " + key, e);
            }
        }
    }

    private Blob getBlob(final ObjectKey key) throws KeyNotFoundException {
        // Unfortunately, it seems Google will do two a separate (HEAD-like) call to get blob metadata.
        // Since the blobs are immutable in tiered storage, we can consider caching them locally
        // to avoid the extra round trip.
        Blob blob = storage.get(this.bucketName, key.value());
        if (blob == null && this.secondaryBucketName != null) {
            gcsMetrics.recordFallbackRead();
            blob = storage.get(this.secondaryBucketName, key.value());
        }
        if (blob == null) {
            throw new KeyNotFoundException(this, key);
        }
        return blob;
    }

    @Override
    public String toString() {
        return "GCSStorage{"
            + "bucketName='" + bucketName + '\''
            + '}';
    }
}
