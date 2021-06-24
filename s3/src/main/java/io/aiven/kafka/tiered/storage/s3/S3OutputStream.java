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

package io.aiven.kafka.tiered.storage.s3;

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
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class S3OutputStream extends OutputStream {

    private final Logger logger = LoggerFactory.getLogger(S3OutputStream.class);

    private final AmazonS3 client;

    private final ByteBuffer byteBuffer;

    private final String bucketName;

    private final String key;

    private MultipartUpload multipartUpload;

    private final int partSize;

    public S3OutputStream(final String bucketName,
                          final String key,
                          final int partSize,
                          final AmazonS3 client) {
        this.bucketName = bucketName;
        this.key = key;
        this.client = client;
        this.partSize = partSize;
        this.byteBuffer = ByteBuffer.allocate(partSize);
    }

    @Override
    public void write(final int b) throws IOException {
        write(new byte[]{(byte) b}, 0, 1);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        if (b.length == 0) {
            return;
        }
        if (Objects.isNull(multipartUpload)) {
            multipartUpload = newMultipartUpload();
        }
        final var source = ByteBuffer.wrap(b, off, len);
        while (source.hasRemaining()) {
            final var transferred = Math.min(byteBuffer.remaining(), source.remaining());
            final var offset = source.arrayOffset() + source.position();
            byteBuffer.put(source.array(), offset, transferred);
            source.position(source.position() + transferred);
            if (!byteBuffer.hasRemaining()) {
                flushBuffer(0, partSize, partSize);
            }
        }
    }

    private MultipartUpload newMultipartUpload() {
        logger.debug("Create new multipart upload request");
        final var initialRequest = new InitiateMultipartUploadRequest(bucketName, key);
        final var initiateResult = client.initiateMultipartUpload(initialRequest);
        logger.debug("Upload ID: {}", initiateResult.getUploadId());
        return new MultipartUpload(initiateResult.getUploadId());
    }

    @Override
    public void close() throws IOException {
        if (byteBuffer.position() > 0 && Objects.nonNull(multipartUpload)) {
            flushBuffer(byteBuffer.arrayOffset(), byteBuffer.position(), byteBuffer.position());
        }
        if (Objects.nonNull(multipartUpload)) {
            multipartUpload.complete();
        }
    }

    private void flushBuffer(final int offset, final int length, final int partSize) throws IOException {
        try {
            multipartUpload.uploadPart(
                            new ByteArrayInputStream(
                                    byteBuffer.array(),
                                    offset,
                                    length),
                            partSize
            );
            byteBuffer.clear();
        } catch (final Exception e) {
            multipartUpload.abort();
            multipartUpload = null;
            throw new IOException(e);
        }
    }

    private class MultipartUpload {

        private final String uploadId;

        private final List<PartETag> partETags = new ArrayList<>();

        public MultipartUpload(final String uploadId) {
            this.uploadId = uploadId;
        }

        public void uploadPart(final InputStream in, final int partSize) {
            final var partNumber = partETags.size() + 1;
            final var uploadPartRequest =
                    new UploadPartRequest()
                            .withBucketName(bucketName)
                            .withKey(key)
                            .withUploadId(uploadId)
                            .withPartSize(partSize)
                            .withPartNumber(partNumber)
                            .withInputStream(in);
            final var uploadResult = client.uploadPart(uploadPartRequest);
            partETags.add(uploadResult.getPartETag());
        }

        public void complete() {
            client.completeMultipartUpload(new CompleteMultipartUploadRequest(bucketName, key, uploadId, partETags));
        }

        public void abort() {
            client.abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, key, uploadId));
        }

    }

}
