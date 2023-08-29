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
import io.aiven.kafka.tieredstorage.storage.StorageBackend;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;

public class GcsStorage implements StorageBackend {
    private final MetricCollector metricCollector = new MetricCollector();

    private Storage storage;
    private String bucketName;
    private Integer resumableUploadChunkSize;

    @Override
    public void configure(final Map<String, ?> configs) {
        final GcsStorageConfig config = new GcsStorageConfig(configs);
        this.bucketName = config.bucketName();
        final StorageOptions.Builder builder = StorageOptions.newBuilder()
            .setCredentials(config.credentials())
            .setTransportOptions(metricCollector.httpTransportOptions());
        if (config.endpointUrl() != null) {
            builder.setHost(config.endpointUrl());
        }
        storage = builder.build().getService();

        resumableUploadChunkSize = config.resumableUploadChunkSize();
    }

    @Override
    public long upload(final InputStream inputStream, final String key) throws StorageBackendException {
        try {
            final BlobInfo blobInfo = BlobInfo.newBuilder(this.bucketName, key).build();
            final Blob blob;
            if (resumableUploadChunkSize != null) {
                blob = storage.createFrom(blobInfo, inputStream, resumableUploadChunkSize);
            } else {
                blob = storage.createFrom(blobInfo, inputStream);
            }
            return blob.getSize();
        } catch (final IOException e) {
            throw new StorageBackendException("Failed to upload " + key, e);
        }
    }

    @Override
    public void delete(final String key) throws StorageBackendException {
        try {
            storage.delete(this.bucketName, key);
        } catch (final StorageException e) {
            throw new StorageBackendException("Failed to delete " + key, e);
        }
    }

    @Override
    public InputStream fetch(final String key) throws StorageBackendException {
        try {
            final Blob blob = getBlob(key);
            final ReadChannel reader = blob.reader();
            return Channels.newInputStream(reader);
        } catch (final StorageException e) {
            throw new StorageBackendException("Failed to fetch " + key, e);
        }
    }

    @Override
    public InputStream fetch(final String key, final BytesRange range) throws StorageBackendException {
        try {
            final Blob blob = getBlob(key);

            if (range.from >= blob.getSize()) {
                throw new InvalidRangeException("Range start position " + range.from
                    + " is outside file content. file size = " + blob.getSize());
            }

            final ReadChannel reader = blob.reader();
            reader.limit(range.to + 1);
            reader.seek(range.from);
            return Channels.newInputStream(reader);
        } catch (final IOException e) {
            throw new StorageBackendException("Failed to fetch " + key, e);
        } catch (final StorageException e) {
            // https://cloud.google.com/storage/docs/json_api/v1/status-codes#416_Requested_Range_Not_Satisfiable
            if (e.getCode() == 416) {
                throw new InvalidRangeException("Invalid range " + range, e);
            }

            throw new StorageBackendException("Failed to fetch " + key, e);
        }
    }

    private Blob getBlob(final String key) throws KeyNotFoundException {
        // Unfortunately, it seems Google will do two a separate (HEAD-like) call to get blob metadata.
        // Since the blobs are immutable in tiered storage, we can consider caching them locally
        // to avoid the extra round trip.
        final Blob blob = storage.get(this.bucketName, key);
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
