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

package io.aiven.kafka.tieredstorage.storage.upload;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enable uploads to object storage (such as S3) when the total size is unknown.
 * Feed input bytes to multiple parts or as a single file for upload.
 *
 * <p>Requires an object storage client and starts a multipart transaction when sending file over upload part size.
 * Do not reuse.
 *
 * <p>{@link AbstractUploadOutputStream} is not thread-safe.
 */
public abstract class AbstractUploadOutputStream<T> extends OutputStream {

    private static final Logger log = LoggerFactory.getLogger(AbstractUploadOutputStream.class);

    private final ByteBuffer partBuffer;
    private final String bucketName;
    private final String key;
    private final int partSize;

    private String uploadId;
    private final List<T> completedParts = new ArrayList<>();

    private boolean closed;
    private long processedBytes;

    protected AbstractUploadOutputStream(final String bucketName,
                                      final String key,
                                      final int partSize) {
        this.bucketName = bucketName;
        this.key = key;
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
                        uploadId = createMultipartUploadRequest(this.bucketName, this.key);
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
                abortUpload(this.bucketName, this.key, this.uploadId);
            }
            throw new IOException(e);
        }
    }

    protected abstract String createMultipartUploadRequest(String bucketName, String key);

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
                        abortUpload(this.bucketName, this.key, this.uploadId);
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
                completeUpload(this.completedParts, this.bucketName, this.key, this.uploadId);
                log.debug("Completed multipart upload {}", uploadId);
            } catch (final RuntimeException e) {
                log.error("Failed to complete multipart upload {}, aborting transaction", uploadId, e);
                abortUpload(this.bucketName, this.key, this.uploadId);
                throw new IOException(e);
            }
        } else {
            abortUpload(this.bucketName, this.key, this.uploadId);
        }
    }

    /**
     * Upload the {@code size} of {@code inputStream} as one whole single file to object storage.
     * The caller of this method should be responsible for closing the inputStream.
     */
    protected abstract void uploadAsSingleFile(final String bucketName,
                                               final String key,
                                               final InputStream inputStream,
                                               final int size);

    public boolean isClosed() {
        return closed;
    }

    protected abstract void completeUpload(List<T> completedParts,
                                           String bucketName,
                                           String key,
                                           String uploadId);

    protected abstract void abortUpload(String bucketName, String key, String uploadId);

    private void flushBuffer(final ByteBuffer buffer,
                             final int actualPartSize,
                             final boolean multiPartUpload) {
        try (final InputStream in = new ByteBufferMarkableInputStream(buffer)) {
            processedBytes += actualPartSize;
            if (multiPartUpload){
                uploadPart(in, actualPartSize);
            } else {
                uploadAsSingleFile(this.bucketName, this.key, in, actualPartSize);
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void uploadPart(final InputStream in, final int actualPartSize) {
        final int partNumber = completedParts.size() + 1;
        final T completedPart = _uploadPart(this.bucketName, this.key, this.uploadId, partNumber, in, actualPartSize);
        completedParts.add(completedPart);
    }

    protected abstract T _uploadPart(final String bucketName,
                                     final String key,
                                     final String uploadId,
                                     final int partNumber,
                                     final InputStream in,
                                     final int actualPartSize);

    public long processedBytes() {
        return processedBytes;
    }

    public int partSize() {
        return partSize;
    }
}
