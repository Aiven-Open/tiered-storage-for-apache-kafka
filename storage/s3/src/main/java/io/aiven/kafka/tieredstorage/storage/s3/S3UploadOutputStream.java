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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.utils.ByteBufferInputStream;

import io.aiven.kafka.tieredstorage.storage.ObjectKey;

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

/**
 * S3 output stream.
 * Enable uploads to S3 with unknown size by feeding input bytes to multiple parts or single file and upload.
 *
 * <p>Requires S3 client and starts a multipart transaction when sending file over upload part size. Do not reuse.
 *
 * <p>{@link S3UploadOutputStream} is not thread-safe.
 */
public class S3UploadOutputStream extends OutputStream {

    private static final Logger log = LoggerFactory.getLogger(S3UploadOutputStream.class);

    private final S3Client client;
    private final ByteBuffer partBuffer;
    private final String bucketName;
    private final ObjectKey key;
    private final StorageClass storageClass;
    final int partSize;

    private String uploadId;
    private final List<CompletedPart> completedParts = new ArrayList<>();

    private boolean closed;
    private long processedBytes;

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
        this.bucketName = bucketName;
        this.key = key;
        this.storageClass = storageClass;
        this.client = client;
        this.partSize = partSize;
        this.partBuffer = ByteBuffer.allocate(partSize);
    }

    @Override
    public void write(final int b) throws IOException {
        write(new byte[] {(byte) b}, 0, 1);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        if (isClosed()) {
            throw new IllegalStateException("Already closed");
        }
        if (b.length == 0) {
            return;
        }
        try {
            final ByteBuffer inputBuffer = ByteBuffer.wrap(b, off, len);
            while (inputBuffer.hasRemaining()) {
                // copy batch to part buffer
                final int inputLimit = inputBuffer.limit();
                final int toCopy = Math.min(partBuffer.remaining(), inputBuffer.remaining());
                final int positionAfterCopying = inputBuffer.position() + toCopy;
                inputBuffer.limit(positionAfterCopying);
                partBuffer.put(inputBuffer.slice());

                // prepare current batch for next part
                inputBuffer.limit(inputLimit);
                inputBuffer.position(positionAfterCopying);

                if (!partBuffer.hasRemaining()) {
                    if (uploadId == null){
                        uploadId = createMultipartUploadRequest();
                        // this is not expected (another exception should be thrown by S3) but adding for completeness
                        if (uploadId == null || uploadId.isEmpty()) {
                            throw new IOException("Failed to create multipart upload, uploadId is empty");
                        }
                    }
                    partBuffer.position(0);
                    partBuffer.limit(partSize);
                    flushBuffer(partBuffer.slice(), partSize, true);
                }
            }
        } catch (final RuntimeException e) {
            closed = true;
            if (multiPartUploadStarted()) {
                log.error("Failed to write to stream on upload {}, aborting transaction", uploadId, e);
                abortUpload();
            }
            throw new IOException(e);
        }
    }

    private String createMultipartUploadRequest() {
        final CreateMultipartUploadRequest initialRequest = CreateMultipartUploadRequest.builder().bucket(bucketName)
                .storageClass(storageClass)
                .key(key.value()).build();
        final CreateMultipartUploadResponse initiateResult = client.createMultipartUpload(initialRequest);
        log.debug("Create new multipart upload request: {}", initiateResult.uploadId());
        return initiateResult.uploadId();
    }

    private boolean multiPartUploadStarted() {
        return uploadId != null;
    }

    @Override
    public void close() throws IOException {
        if (!isClosed()) {
            closed = true;
            final int lastPosition = partBuffer.position();
            if (lastPosition > 0) {
                try {
                    partBuffer.position(0);
                    partBuffer.limit(lastPosition);
                    flushBuffer(partBuffer.slice(), lastPosition, multiPartUploadStarted());
                } catch (final RuntimeException e) {
                    if (multiPartUploadStarted()) {
                        log.error("Failed to upload last part {}, aborting transaction", uploadId, e);
                        abortUpload();
                    } else {
                        log.error("Failed to upload the file {}", key, e);
                    }
                    throw new IOException(e);
                }
            }
            if (multiPartUploadStarted()) {
                completeOrAbortMultiPartUpload();
            }
        }
    }

    private void completeOrAbortMultiPartUpload() throws IOException {
        if (!completedParts.isEmpty()) {
            try {
                completeUpload();
                log.debug("Completed multipart upload {}", uploadId);
            } catch (final RuntimeException e) {
                log.error("Failed to complete multipart upload {}, aborting transaction", uploadId, e);
                abortUpload();
                throw new IOException(e);
            }
        } else {
            abortUpload();
        }
    }

    /**
     * Upload the {@code size} of {@code inputStream} as one whole single file to S3.
     * The caller of this method should be responsible for closing the inputStream.
     */
    private void uploadAsSingleFile(final InputStream inputStream, final int size) {
        final PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(bucketName)
            .storageClass(storageClass)
            .key(key.value())
            .build();
        final RequestBody requestBody = RequestBody.fromInputStream(inputStream, size);
        client.putObject(putObjectRequest, requestBody);
    }

    public boolean isClosed() {
        return closed;
    }

    private void completeUpload() {
        final CompletedMultipartUpload completedMultipartUpload = CompletedMultipartUpload.builder()
            .parts(completedParts)
            .build();
        final var request = CompleteMultipartUploadRequest.builder()
            .bucket(bucketName)
            .key(key.value())
            .uploadId(uploadId)
            .multipartUpload(completedMultipartUpload)
            .build();
        client.completeMultipartUpload(request);
    }

    private void abortUpload() {
        final var request = AbortMultipartUploadRequest.builder()
            .bucket(bucketName)
            .key(key.value())
            .uploadId(uploadId)
            .build();
        client.abortMultipartUpload(request);
    }

    private void flushBuffer(final ByteBuffer buffer,
                             final int actualPartSize,
                             final boolean multiPartUpload) {
        try (final InputStream in = new ByteBufferInputStream(buffer)) {
            processedBytes += actualPartSize;
            if (multiPartUpload){
                uploadPart(in, actualPartSize);
            } else {
                uploadAsSingleFile(in, actualPartSize);
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void uploadPart(final InputStream in, final int actualPartSize) {
        final int partNumber = completedParts.size() + 1;
        final UploadPartRequest uploadPartRequest =
            UploadPartRequest.builder()
                .bucket(bucketName)
                .key(key.value())
                .uploadId(uploadId)
                .partNumber(partNumber)
                .build();
        final RequestBody body = RequestBody.fromInputStream(in, actualPartSize);
        final UploadPartResponse uploadResult = client.uploadPart(uploadPartRequest, body);
        final CompletedPart completedPart = CompletedPart.builder()
            .partNumber(partNumber)
            .eTag(uploadResult.eTag())
            .build();
        completedParts.add(completedPart);
    }

    long processedBytes() {
        return processedBytes;
    }
}
