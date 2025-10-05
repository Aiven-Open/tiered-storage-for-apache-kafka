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

package io.aiven.kafka.tieredstorage.storage.oci;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.InvalidRangeException;
import io.aiven.kafka.tieredstorage.storage.KeyNotFoundException;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.model.Range;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.model.StorageTier;
import com.oracle.bmc.objectstorage.requests.DeleteObjectRequest;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;


public class OciStorage implements StorageBackend {

    private ObjectStorageClient objectStorageClient;
    private String namespaceName;
    private String bucketName;
    private int partSize;
    private StorageTier storageTier;

    @Override
    public void configure(final Map<String, ?> configs) {
        final OciStorageConfig config = new OciStorageConfig(configs);
        this.objectStorageClient = OciClientBuilder.build(config);
        this.namespaceName = config.namespaceName();
        this.bucketName = config.bucketName();
        this.partSize = config.uploadPartSize();
        this.storageTier = config.storageTier();
    }

    @Override
    public long upload(final InputStream inputStream, final ObjectKey key) throws StorageBackendException {
        final var out = ociOutputStream(key);
        try (out) {
            inputStream.transferTo(out);
        } catch (final IOException e) {
            throw new StorageBackendException("Failed to upload " + key, e);
        }
        // getting the processed bytes after close to account last flush.
        return out.processedBytes();
    }

    OciUploadOutputStream ociOutputStream(final ObjectKey key) {
        return new OciUploadOutputStream(namespaceName, bucketName, key, partSize, storageTier, objectStorageClient);
    }

    @Override
    public void delete(final ObjectKey key) throws StorageBackendException {
        try {
            final DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                    .namespaceName(namespaceName)
                    .bucketName(bucketName)
                    .objectName(key.value())
                    .build();
            objectStorageClient.deleteObject(deleteRequest);
        } catch (final BmcException e) {
            throw new StorageBackendException("Failed to delete " + key, e);
        }
    }

    @Override
    public void delete(final Set<ObjectKey> keys) throws StorageBackendException {
        final List<ObjectKey> objectKeys = new ArrayList<>(keys);
        //OCI don't support batch delete
        for (final ObjectKey objectKey : objectKeys) {
            this.delete(objectKey);
        }
    }

    @Override
    public InputStream fetch(final ObjectKey key) throws StorageBackendException {
        final GetObjectRequest getObjectRequest = GetObjectRequest.builder()
            .namespaceName(namespaceName)
            .bucketName(bucketName)
            .objectName(key.value()).build();
        try {
            return objectStorageClient.getObject(getObjectRequest).getInputStream();
        } catch (final BmcException e) {
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
            if (range.isEmpty()) {
                return InputStream.nullInputStream();
            }
            
            final GetObjectRequest getRequest = GetObjectRequest.builder()
                 .namespaceName(namespaceName)
                .bucketName(bucketName)
                .objectName(key.value())
                .range(formatRange(range))
                .build();
            return objectStorageClient.getObject(getRequest).getInputStream();
        } catch (final BmcException e) {
            if (e.getStatusCode() == 404) {
                throw new KeyNotFoundException(this, key, e);
            }
            if (e.getStatusCode() == 416) {
                throw new InvalidRangeException("Invalid range " + range, e);
            }

            throw new StorageBackendException("Failed to fetch " + key, e);
        }
    }

    private Range formatRange(final BytesRange range) {
        return new Range((long) range.firstPosition(), (long) range.lastPosition());
    }

    @Override
    public String toString() {
        return "OciStorage{"
            + "namespaceName='" + namespaceName + '\''
            + "bucketName='" + bucketName + '\''
            + ", partSize=" + partSize
            + '}';
    }
}
