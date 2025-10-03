/*
 * Copyright 2021 Aiven Oy
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

import java.io.InputStream;
import java.util.List;

import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.upload.AbstractUploadOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

public class S3UploadOutputStream extends AbstractUploadOutputStream<CompletedPart> {

    private static final Logger log = LoggerFactory.getLogger(S3UploadOutputStream.class);

    private final S3Client client;
    private final StorageClass storageClass;

    public S3UploadOutputStream(final String bucketName,
                                final ObjectKey key,
                                final int partSize,
                                final S3Client client){
        this(bucketName, key, StorageClass.STANDARD, partSize, client);
    }

    public S3UploadOutputStream(final String bucketName,
                                final ObjectKey key,
                                final StorageClass storageClass,
                                final int partSize,
                                final S3Client client) {
        super(bucketName, key.value(), partSize);
        this.storageClass = storageClass;
        this.client = client;
    }

    @Override
    protected String createMultipartUploadRequest(final String bucketName, final String key) {
        final CreateMultipartUploadRequest initialRequest = CreateMultipartUploadRequest.builder().bucket(bucketName)
                .storageClass(storageClass)
                .key(key).build();
        final CreateMultipartUploadResponse initiateResult = client.createMultipartUpload(initialRequest);
        log.debug("Create new multipart upload request: {}", initiateResult.uploadId());
        return initiateResult.uploadId();
    }

    protected void uploadAsSingleFile(final String bucketName,
                                      final String key,
                                      final InputStream inputStream,
                                      final int size) {
        final PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(bucketName)
                .storageClass(storageClass)
                .key(key)
                .build();
        final RequestBody requestBody = RequestBody.fromInputStream(inputStream, size);
        client.putObject(putObjectRequest, requestBody);
    }

    @Override
    protected CompletedPart _uploadPart(final String bucketName,
                                        final String key,
                                        final String uploadId,
                                        final int partNumber,
                                        final InputStream in,
                                        final int actualPartSize) {
        final UploadPartRequest uploadPartRequest =
                UploadPartRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .uploadId(uploadId)
                        .partNumber(partNumber)
                        .build();
        final RequestBody body = RequestBody.fromInputStream(in, actualPartSize);
        final UploadPartResponse uploadResult = client.uploadPart(uploadPartRequest, body);

        return CompletedPart.builder()
                .partNumber(partNumber)
                .eTag(uploadResult.eTag())
                .build();
    }

    @Override
    protected void abortUpload(final String bucketName, final String key, final String uploadId) {
        final var request = AbortMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(key)
                .uploadId(uploadId)
                .build();
        client.abortMultipartUpload(request);
    }

    @Override
    protected void completeUpload(final List<CompletedPart> completedParts,
                                  final String bucketName,
                                  final String key,
                                  final String uploadId) {
        final CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
                .parts(completedParts)
                .build();
        final var request = CompleteMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(key)
                .uploadId(uploadId)
                .multipartUpload(completedMultipartUpload)
                .build();
        client.completeMultipartUpload(request);
    }
}
