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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
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
 * <p>OutputStream is used to write sequentially, but
 * uploading parts happen asynchronously to reduce full upload latency.
 * Concurrency happens within the output stream implementation and does not require changes on the callers.
 *
 * <p>Requires S3 client and starts a multipart transaction when instantiated. Do not reuse.
 *
 * <p>{@link S3MultiPartOutputStream} is not thread-safe.
 */
public class S3MultiPartOutputStream extends OutputStream {

    private static final Logger log = LoggerFactory.getLogger(S3MultiPartOutputStream.class);

    private final AmazonS3 client;
    private final ByteBuffer partBuffer;
    private final String bucketName;
    private final String key;
    final int partSize;

    private final String uploadId;
    private final AtomicInteger partNumber = new AtomicInteger(0);

    // holds async part upload operations building a list of partETags required when committing
    private CompletableFuture<ConcurrentLinkedQueue<PartETag>> partUploads =
        CompletableFuture.completedFuture(new ConcurrentLinkedQueue<>());
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
        if (isClosed()) {
            throw new IllegalStateException("Already closed");
        }
        if (b.length == 0) {
            return;
        }
        try {
            final ByteBuffer currentBatch = ByteBuffer.wrap(b, off, len);
            while (currentBatch.hasRemaining()) {
                // copy batch to part buffer
                final int toCopy = Math.min(partBuffer.remaining(), currentBatch.remaining());
                final int positionAfterCopying = currentBatch.position() + toCopy;
                currentBatch.limit(positionAfterCopying);
                partBuffer.put(currentBatch.slice());

                // prepare current batch for next part
                currentBatch.clear(); // reset limit
                currentBatch.position(positionAfterCopying);

                if (!partBuffer.hasRemaining()) {
                    partBuffer.position(0);
                    partBuffer.limit(partSize);
                    uploadPart(partBuffer.slice(), partSize);
                    partBuffer.clear();
                }
            }
        } catch (final RuntimeException e) {
            log.error("Failed to write to stream on upload {}, aborting transaction", uploadId, e);
            abortUpload();
            throw new IOException(e);
        }
    }

    /**
     * Completes pending part uploads
     *
     * @throws IOException if uploads fail and abort transaction
     */
    @Override
    public void flush() throws IOException {
        try {
            if (partBuffer.position() > 0) {
                // flush missing bytes
                final int actualPartSize = partBuffer.position();
                partBuffer.position(0);
                partBuffer.limit(actualPartSize);
                uploadPart(partBuffer.slice(), actualPartSize);
                partBuffer.clear();
            }

            // wait for requests to be processed
            partUploads.join();
        } catch (final RuntimeException e) {
            log.error("Failed to upload parts {}, aborting transaction", uploadId, e);
            abortUpload();
            throw new IOException("Failed to flush upload part operations", e);
        }
    }

    @Override
    public void close() throws IOException {
        if (!isClosed()) {
            flush();
            if (partNumber.get() > 0) {
                try {
                    // wait for all uploads to complete successfully before committing
                    final ConcurrentLinkedQueue<PartETag> tagsQueue = partUploads.get(); // TODO: maybe set a timeout?
                    final ArrayList<PartETag> partETags = new ArrayList<>(tagsQueue);

                    completeUpload(partETags);
                    log.debug("Completed multipart upload {}", uploadId);
                } catch (final RuntimeException | InterruptedException | ExecutionException e) {
                    log.error("Failed to complete multipart upload {}, aborting transaction", uploadId, e);
                    abortUpload();
                    throw new IOException("Failed to complete upload transaction", e);
                }
            } else {
                abortUpload();
            }
        }
    }

    public boolean isClosed() {
        return closed;
    }

    private void completeUpload(final List<PartETag> partETags) {
        final var request = new CompleteMultipartUploadRequest(bucketName, key, uploadId, partETags);
        client.completeMultipartUpload(request);
        closed = true;
    }

    private void abortUpload() {
        final var request = new AbortMultipartUploadRequest(bucketName, key, uploadId);
        client.abortMultipartUpload(request);
        closed = true;
    }

    private void uploadPart(final ByteBuffer partBuffer, final int actualPartSize) {
        final byte[] partContent = new byte[actualPartSize];
        partBuffer.get(partContent, 0, actualPartSize);
        final var is = new ByteArrayInputStream(partContent);

        // Run request async
        partUploads = partUploads.thenCombine(
            CompletableFuture.supplyAsync(() -> {
                try (final var content = is) { // inputStream not closed by upload part
                    final var uploadPartRequest = new UploadPartRequest()
                        .withBucketName(bucketName)
                        .withKey(key)
                        .withUploadId(uploadId)
                        .withPartSize(actualPartSize)
                        .withPartNumber(partNumber.incrementAndGet())
                        .withInputStream(content);
                    return client.uploadPart(uploadPartRequest);
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            }),
            (partETags, result) -> {
                partETags.add(result.getPartETag());
                return partETags;
            });
    }
}
