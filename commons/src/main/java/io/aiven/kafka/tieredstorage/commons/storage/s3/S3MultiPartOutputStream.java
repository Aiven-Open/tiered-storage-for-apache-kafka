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

package io.aiven.kafka.tieredstorage.commons.storage.s3;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
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

    public static final int DEFAULT_PART_SIZE = 5 * 1024 * 1024;

    private final AmazonS3 client;
    private final ByteBuffer partBuffer;
    private final String bucketName;
    private final String key;
    private final int partSize;

    private final String uploadId;
    private final List<PartETag> partETags = new ArrayList<>();

    private boolean closed;

    public S3MultiPartOutputStream(final String bucketName,
                                   final String key,
                                   final AmazonS3 client) {
        this(bucketName, key, DEFAULT_PART_SIZE, client);
    }

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
            final int transferred = Math.min(partBuffer.remaining(), source.remaining());
            final int offset = source.arrayOffset() + source.position();
            // TODO: get rid of this array copying
            partBuffer.put(source.array(), offset, transferred);
            source.position(source.position() + transferred);
            if (!partBuffer.hasRemaining()) {
                flushBuffer(0, partSize, partSize);
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (partBuffer.position() > 0) {
            flushBuffer(partBuffer.arrayOffset(), partBuffer.position(), partBuffer.position());
        }
        if (Objects.nonNull(uploadId)) {
            final CompleteMultipartUploadRequest request =
                new CompleteMultipartUploadRequest(bucketName, key, uploadId, partETags);
            final CompleteMultipartUploadResult result = client.completeMultipartUpload(request);
            log.debug("Completed multipart upload {} with result {}", uploadId, result);
        }
        closed = true;
    }

    private void flushBuffer(final int offset,
                             final int length,
                             final int partSize) throws IOException {
        try {
            final ByteArrayInputStream in = new ByteArrayInputStream(partBuffer.array(), offset, length);
            uploadPart(in, partSize);
            partBuffer.clear();
        } catch (final Exception e) {
            log.error("Failed to upload part in multipart upload {}, aborting transaction", uploadId, e);
            client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadId));
            closed = true;
            throw new IOException(e);
        }
    }

    private void uploadPart(final InputStream in, final int partSize) {
        final int partNumber = partETags.size() + 1;
        final UploadPartRequest uploadPartRequest =
            new UploadPartRequest()
                .withBucketName(bucketName)
                .withKey(key)
                .withUploadId(uploadId)
                .withPartSize(partSize)
                .withPartNumber(partNumber)
                .withInputStream(in);
        final UploadPartResult uploadResult = client.uploadPart(uploadPartRequest);
        partETags.add(uploadResult.getPartETag());
    }
}
