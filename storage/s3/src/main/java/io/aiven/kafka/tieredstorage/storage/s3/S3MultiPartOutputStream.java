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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * S3 multipart output stream.
 * Enable uploads to S3 with unknown size by feeding input bytes to multiple parts and upload them on close.
 *
 * <p>Requires S3 client and starts a multipart transaction when instantiated. Do not reuse.
 */
public class S3MultiPartOutputStream extends OutputStream {

    private static final Logger log = LoggerFactory.getLogger(S3MultiPartOutputStream.class);

    private final AmazonS3 client;
    private final ByteBuffer partBuffer;
    private final String bucketName;
    private final String key;
    final int partSize;

    private final String uploadId;
    private final List<PartETag> partETags = Collections.synchronizedList(new ArrayList<>());
    private final List<CompletableFuture<Void>> partUploads = new ArrayList<>();

    private boolean closed;

    public S3MultiPartOutputStream(final String bucketName,
                                   final String key,
                                   final int partSize,
                                   final AmazonS3 client) {
        this.bucketName = bucketName;
        this.key = key;
        this.client = client;
        this.partSize = partSize;
        this.partBuffer = ByteBuffer.allocate(partSize);
        final InitiateMultipartUploadRequest initialRequest = new InitiateMultipartUploadRequest(bucketName, key);
        final InitiateMultipartUploadResult initiateResult = client.initiateMultipartUpload(initialRequest);
        log.debug("Create new multipart upload request: {}", initiateResult.getUploadId());
        this.uploadId = initiateResult.getUploadId();
    }

    @Override
    public void write(final int b) throws IOException {
        write(new byte[] {(byte) b}, 0, 1);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        if (closed) {
            throw new IllegalStateException("Already closed");
        }
        if (b.length == 0) {
            return;
        }
        final ByteBuffer source = ByteBuffer.wrap(b, off, len);
        while (source.hasRemaining()) {
            final int toCopy = Math.min(partBuffer.remaining(), source.remaining());
            final int positionAfterCopying = source.position() + toCopy;
            source.limit(positionAfterCopying);
            partBuffer.put(source.slice());
            source.clear(); // reset limit
            source.position(positionAfterCopying);
            if (!partBuffer.hasRemaining()) {
                partBuffer.position(0);
                partBuffer.limit(partSize);
                flushBuffer(partBuffer.slice(), partSize);
                partBuffer.clear();
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (partBuffer.position() > 0) {
            final int actualPartSize = partBuffer.position();
            partBuffer.position(0);
            partBuffer.limit(actualPartSize);
            flushBuffer(partBuffer.slice(), actualPartSize);
        }
        if (Objects.nonNull(uploadId)) {
            if (!partUploads.isEmpty()) {
                try {
                    CompletableFuture.allOf(partUploads.toArray(new CompletableFuture[0]))
                        .thenAccept(unused -> {
                            final CompleteMultipartUploadRequest request =
                                new CompleteMultipartUploadRequest(bucketName, key, uploadId, partETags);
                            final CompleteMultipartUploadResult result = client.completeMultipartUpload(request);
                            log.debug("Completed multipart upload {} with result {}", uploadId, result);
                        })
                        .get(); // TODO: maybe set a timeout?
                } catch (final InterruptedException | ExecutionException e) {
                    log.error("Failed to complete multipart upload {}, aborting transaction", uploadId, e);
                    client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadId));
                    throw new IOException(e);
                }
            } else {
                client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadId));
            }
        }
        closed = true;
    }

    private void flushBuffer(final ByteBuffer partBuffer, final int actualPartSize) throws IOException {
        try {
            final byte[] array = new byte[actualPartSize];
            partBuffer.get(array, 0, actualPartSize);
            final UploadPartRequest uploadPartRequest =
                new UploadPartRequest()
                    .withBucketName(bucketName)
                    .withKey(key)
                    .withUploadId(uploadId)
                    .withPartSize(actualPartSize)
                    .withPartNumber(partUploads.size() + 1)
                    .withInputStream(new ByteArrayInputStream(array));

            // Run request async
            final CompletableFuture<Void> upload =
                CompletableFuture.supplyAsync(() -> client.uploadPart(uploadPartRequest))
                    .thenAccept(uploadPartResult -> partETags.add(uploadPartResult.getPartETag()));

            partUploads.add(upload);
        } catch (final Exception e) {
            log.error("Failed to upload part in multipart upload {}, aborting transaction", uploadId, e);
            client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadId));
            closed = true;
            throw new IOException(e);
        }
    }
}
