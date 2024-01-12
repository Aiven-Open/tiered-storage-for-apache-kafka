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

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.InvalidRangeException;
import io.aiven.kafka.tieredstorage.storage.KeyNotFoundException;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

public class S3Storage implements StorageBackend {

    private S3Client s3Client;
    private String bucketName;
    private int partSize;

    @Override
    public void configure(final Map<String, ?> configs) {
        final S3StorageConfig config = new S3StorageConfig(configs);
        this.s3Client = config.s3Client();
        this.bucketName = config.bucketName();
        this.partSize = config.uploadPartSize();
    }

    @Override
    public long upload(final InputStream inputStream, final ObjectKey key) throws StorageBackendException {
        try (final var out = s3OutputStream(key)) {
            inputStream.transferTo(out);
            return out.processedBytes();
        } catch (final IOException e) {
            throw new StorageBackendException("Failed to upload " + key, e);
        }
    }

    S3MultiPartOutputStream s3OutputStream(final ObjectKey key) {
        return new S3MultiPartOutputStream(bucketName, key, partSize, s3Client);
    }

    @Override
    public void delete(final ObjectKey key) throws StorageBackendException {
        try {
            final var deleteRequest = DeleteObjectRequest.builder().bucket(bucketName).key(key.value()).build();
            s3Client.deleteObject(deleteRequest);
        } catch (final AwsServiceException e) {
            throw new StorageBackendException("Failed to delete " + key, e);
        }
    }

    @Override
    public InputStream fetch(final ObjectKey key) throws StorageBackendException {
        final GetObjectRequest getRequest = GetObjectRequest.builder().bucket(bucketName).key(key.value()).build();
        try {
            return s3Client.getObject(getRequest);
        } catch (final AwsServiceException e) {
            if (e.statusCode() == 404) {
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
                .bucket(bucketName)
                .key(key.value())
                .range(formatRange(range))
                .build();
            return s3Client.getObject(getRequest);
        } catch (final AwsServiceException e) {
            if (e.statusCode() == 404) {
                throw new KeyNotFoundException(this, key, e);
            }
            if (e.statusCode() == 416) {
                throw new InvalidRangeException("Invalid range " + range, e);
            }

            throw new StorageBackendException("Failed to fetch " + key, e);
        }
    }

    private String formatRange(final BytesRange range) {
        return "bytes=" + range.firstPosition() + "-" + range.lastPosition();
    }

    @Override
    public String toString() {
        return "S3Storage{"
            + "bucketName='" + bucketName + '\''
            + ", partSize=" + partSize
            + '}';
    }
}
