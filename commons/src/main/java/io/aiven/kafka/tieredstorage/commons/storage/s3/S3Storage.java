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

package io.aiven.kafka.tieredstorage.commons.storage.s3;

import java.io.IOException;
import java.io.InputStream;

import io.aiven.kafka.tieredstorage.commons.storage.BytesRange;
import io.aiven.kafka.tieredstorage.commons.storage.FileDeleter;
import io.aiven.kafka.tieredstorage.commons.storage.FileFetcher;
import io.aiven.kafka.tieredstorage.commons.storage.FileUploader;
import io.aiven.kafka.tieredstorage.commons.storage.InvalidRangeException;
import io.aiven.kafka.tieredstorage.commons.storage.KeyNotFoundException;
import io.aiven.kafka.tieredstorage.commons.storage.StorageBackEndException;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class S3Storage implements FileUploader, FileDeleter, FileFetcher {

    final AmazonS3 s3Client;
    final String bucketName;

    public S3Storage(final AmazonS3 s3Client, final String bucketName) {
        this.s3Client = s3Client;
        this.bucketName = bucketName;
    }

    @Override
    public void upload(final InputStream inputStream, final String key) throws StorageBackEndException {
        // TODO make multipart part size configurable
        try (final S3MultiPartOutputStream out = new S3MultiPartOutputStream(bucketName, key, s3Client)) {
            inputStream.transferTo(out);
        } catch (final AmazonS3Exception | IOException e) {
            throw new StorageBackEndException("Failed to upload " + key, e);
        }
    }

    @Override
    public void delete(final String key) throws StorageBackEndException {
        try {
            s3Client.deleteObject(bucketName, key);
        } catch (final AmazonS3Exception e) {
            throw new StorageBackEndException("Failed to delete " + key, e);
        }
    }

    @Override
    public InputStream fetch(final String key) throws StorageBackEndException {
        try {
            final GetObjectRequest getRequest = new GetObjectRequest(bucketName, key);
            final S3Object object = s3Client.getObject(getRequest);
            return object.getObjectContent();
        } catch (final AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                throw new KeyNotFoundException(this, key, e);
            } else {
                throw new StorageBackEndException("Failed to fetch " + key, e);
            }
        }
    }

    @Override
    public InputStream fetch(final String key, final BytesRange range) throws StorageBackEndException {
        try {
            final GetObjectRequest getRequest = new GetObjectRequest(bucketName, key);
            getRequest.setRange(range.from, range.to);
            final S3Object object = s3Client.getObject(getRequest);
            return object.getObjectContent();
        } catch (final AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                throw new KeyNotFoundException(this, key, e);
            }
            if (e.getStatusCode() == 416) {
                throw new InvalidRangeException("Invalid range " + range, e);
            }

            throw new StorageBackEndException("Failed to fetch " + key, e);
        }
    }

    @Override
    public String toString() {
        return "S3Storage{"
            + "bucketName='" + bucketName + '\''
            + '}';
    }
}
