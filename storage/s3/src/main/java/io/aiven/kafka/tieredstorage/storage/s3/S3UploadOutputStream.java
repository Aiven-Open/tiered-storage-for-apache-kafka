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
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import javax.management.ObjectName;
import javax.management.StandardMBean;

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

    // FAULT INJECTION — staging-only Aiven #820 reproduction trigger.
    // Gated by -Dio.aiven.faultinjection.enabled=true; in prod (system property absent)
    // this is `static final false` and JIT eliminates the dead branch in the constructor.
    private static final boolean FAULT_INJECTION_ENABLED =
        Boolean.parseBoolean(System.getProperty("io.aiven.faultinjection.enabled", "false"));

    // Operator-controlled one-shot flag (auto-resets after firing once).
    // Toggle via JMX MBean io.aiven.kafka.tieredstorage:type=FaultInjection,name=S3UploadOom
    private static volatile boolean simulateOomOnNextConstruction = false;

    // Operator-controlled sticky topic-scoped flag. When non-empty, every S3UploadOutputStream
    // construction whose key.value() contains this substring throws OOM. Stays in effect until
    // operator writes empty string (or null, which is coerced to ""). Designed for end-to-end
    // bloat-reproduction on a dedicated test topic without affecting other partitions.
    private static volatile String simulateOomForTopicSubstring = "";

    static {
        if (FAULT_INJECTION_ENABLED) {
            try {
                final ObjectName name = new ObjectName(
                    "io.aiven.kafka.tieredstorage:type=FaultInjection,name=S3UploadOom");
                ManagementFactory.getPlatformMBeanServer().registerMBean(
                    new StandardMBean(new FaultInjectionImpl(), FaultInjectionMBean.class),
                    name);
                log.info("[logzio-rsm-trace] FAULT INJECTION MBean registered (staging-only)");
            } catch (final Exception e) {
                log.warn("[logzio-rsm-trace] Could not register fault injection MBean", e);
            }
        }
    }

    public interface FaultInjectionMBean {
        void setSimulateOomOnNextConstruction(boolean enabled);
        boolean getSimulateOomOnNextConstruction();
        void setSimulateOomForTopicSubstring(String substring);
        String getSimulateOomForTopicSubstring();
    }

    static class FaultInjectionImpl implements FaultInjectionMBean {
        @Override
        public void setSimulateOomOnNextConstruction(final boolean v) {
            simulateOomOnNextConstruction = v;
        }

        @Override
        public boolean getSimulateOomOnNextConstruction() {
            return simulateOomOnNextConstruction;
        }

        @Override
        public void setSimulateOomForTopicSubstring(final String substring) {
            simulateOomForTopicSubstring = substring == null ? "" : substring;
        }

        @Override
        public String getSimulateOomForTopicSubstring() {
            return simulateOomForTopicSubstring;
        }
    }

    // Aiven #820 lazy-cache fix — see docs/kafka/tiered_storage/aiven-820-lazy-cache-fix-plan.md.
    // Initial buffer size pre-sized from worst-case small-file uploads
    // (segment.bytes=100M ⇒ indexes ≈ 300 KiB, manifest ≈ 3 KiB); 1 MiB covers both in a single
    // allocation with no grow. Segment-log uploads grow on demand up to `partSize`.
    private static final int INITIAL_BUFFER_SIZE = 1 * 1024 * 1024;

    private final S3Client client;
    private ByteBuffer partBuffer;       // lazy, grow-on-demand up to partSize (Aiven #820 fix)
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
        // FAULT INJECTION (staging-only): JIT eliminates this branch in prod.
        // Boolean one-shot stays at the constructor for the quick-smoke-test semantics it had
        // before the lazy-cache fix. The topic-substring check moved to growBufferTo() to ride
        // along with the new lazy-allocate site — proves the allocation moved correctly.
        if (FAULT_INJECTION_ENABLED && simulateOomOnNextConstruction) {
            simulateOomOnNextConstruction = false;        // one-shot reset
            throw new OutOfMemoryError("Java heap space");
        }
        // Aiven #820 lazy-cache fix: partBuffer is lazy, grown on demand inside write().
        // Constructor no longer allocates `partSize` (was the eager OOM site at the old line 86).
    }

    /**
     * Grow {@link #partBuffer} so its capacity is at least {@code targetCapacity} bytes
     * (clamped to {@link #partSize}). Lazy-init on first call. Preserves any previously
     * written bytes (position is carried over to the new buffer). Old buffer is dropped for GC.
     *
     * <p>Aiven #820 lazy-cache fix: small-file uploads (indexes, manifest) typically cap at the
     * initial 1 MiB allocation without ever growing; segment-log uploads grow by doubling up to
     * {@code partSize}. Peak per-instance heap usage matches the previous eager allocation only
     * for full multipart uploads; small-file uploads see ~96 % heap savings.
     */
    private void growBufferTo(final int targetCapacity) {
        final int wanted = Math.min(partSize, Math.max(INITIAL_BUFFER_SIZE, targetCapacity));
        // FAULT INJECTION (staging-only): JIT eliminates this branch in prod.
        // Topic-substring check at the NEW lazy-allocate site (moved here from the constructor
        // by Experiment 3.5 to validate the allocation actually moved out of <init>).
        // When flag set, throws OOM right before the real ByteBuffer.allocate below — same
        // outer catch(Error t) chain still fires ABORTED ABNORMALLY in copyLogSegmentData.
        if (FAULT_INJECTION_ENABLED) {
            final String substring = simulateOomForTopicSubstring;   // volatile-read once
            if (!substring.isEmpty() && key.value().contains(substring)) {
                throw new OutOfMemoryError("Java heap space");
            }
        }
        if (partBuffer == null) {
            partBuffer = ByteBuffer.allocate(wanted);
            return;
        }
        if (partBuffer.capacity() >= wanted) {
            return;
        }
        final int newCap = Math.min(partSize, Math.max(wanted, partBuffer.capacity() * 2));
        final ByteBuffer larger = ByteBuffer.allocate(newCap);
        partBuffer.flip();          // limit=position, position=0 (prepare for read)
        larger.put(partBuffer);     // copies bytes; larger.position advances; partBuffer dropped after
        partBuffer = larger;
    }

    @Override
    public void write(final int b) throws IOException {
        write(new byte[] {(byte) b}, 0, 1);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        try {
            if (isClosed()) {
                throw new IllegalStateException("Already closed");
            }
            if (b.length == 0) {
                return;
            }
            try {
                final ByteBuffer inputBuffer = ByteBuffer.wrap(b, off, len);
                while (inputBuffer.hasRemaining()) {
                    // Aiven #820 lazy-cache fix: lazy-init / grow-on-demand. Each iteration
                    // ensures at least 1 byte of space; grow up to `partSize` cap as needed.
                    if (partBuffer == null || (!partBuffer.hasRemaining() && partBuffer.capacity() < partSize)) {
                        final int current = partBuffer == null ? 0 : partBuffer.position();
                        growBufferTo(current + inputBuffer.remaining());
                    }

                    // copy batch to part buffer
                    final int inputLimit = inputBuffer.limit();
                    final int toCopy = Math.min(partBuffer.remaining(), inputBuffer.remaining());
                    final int positionAfterCopying = inputBuffer.position() + toCopy;
                    inputBuffer.limit(positionAfterCopying);
                    partBuffer.put(inputBuffer.slice());

                    // prepare current batch for next part
                    inputBuffer.limit(inputLimit);
                    inputBuffer.position(positionAfterCopying);

                    // Flush as a multipart part only when buffer reached `partSize` AND is full.
                    if (!partBuffer.hasRemaining() && partBuffer.capacity() == partSize) {
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
                        partBuffer.clear();    // reset position=0, limit=capacity for the next part
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
        } catch (final Error t) {
            log.error("[logzio-rsm-trace] S3UploadOutputStream.write ABORTED ABNORMALLY key={} uploadId={} cause={} message={}",
                key.value(), uploadId, t.getClass().getName(), t.getMessage(), t);
            throw t;
        }
    }

    private String createMultipartUploadRequest() {
        log.debug("[logzio-rsm-trace] Entry: S3UploadOutputStream.createMultipartUploadRequest key={}",
            key.value());
        final CreateMultipartUploadRequest initialRequest = CreateMultipartUploadRequest.builder().bucket(bucketName)
                .storageClass(storageClass)
                .key(key.value()).build();
        final CreateMultipartUploadResponse initiateResult = client.createMultipartUpload(initialRequest);
        log.debug("Create new multipart upload request: {}", initiateResult.uploadId());
        log.debug("[logzio-rsm-trace] Exit: S3UploadOutputStream.createMultipartUploadRequest key={} uploadId={}",
            key.value(), initiateResult.uploadId());
        return initiateResult.uploadId();
    }

    private boolean multiPartUploadStarted() {
        return uploadId != null;
    }

    @Override
    public void close() throws IOException {
        try {
            if (!isClosed()) {
                closed = true;
                // Aiven #820 lazy-cache fix: partBuffer may be null if write() was never called.
                final int lastPosition = (partBuffer == null) ? 0 : partBuffer.position();
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
        } catch (final Error t) {
            log.error("[logzio-rsm-trace] S3UploadOutputStream.close ABORTED ABNORMALLY key={} uploadId={} processedBytes={} cause={} message={}",
                key.value(), uploadId, processedBytes, t.getClass().getName(), t.getMessage(), t);
            throw t;
        }
    }

    private void completeOrAbortMultiPartUpload() throws IOException {
        try {
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
        } catch (final Error t) {
            log.error("[logzio-rsm-trace] S3UploadOutputStream.completeOrAbortMultiPartUpload ABORTED ABNORMALLY key={} uploadId={} cause={} message={}",
                key.value(), uploadId, t.getClass().getName(), t.getMessage(), t);
            throw t;
        }
    }

    /**
     * Upload the {@code size} of {@code inputStream} as one whole single file to S3.
     * The caller of this method should be responsible for closing the inputStream.
     */
    private void uploadAsSingleFile(final InputStream inputStream, final int size) {
        log.debug("[logzio-rsm-trace] Entry: S3UploadOutputStream.uploadAsSingleFile key={} size={}",
            key.value(), size);
        final PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(bucketName)
            .storageClass(storageClass)
            .key(key.value())
            .build();
        final RequestBody requestBody = RequestBody.fromInputStream(inputStream, size);
        client.putObject(putObjectRequest, requestBody);
        log.debug("[logzio-rsm-trace] Exit: S3UploadOutputStream.uploadAsSingleFile key={} size={}",
            key.value(), size);
    }

    public boolean isClosed() {
        return closed;
    }

    private void completeUpload() {
        log.debug("[logzio-rsm-trace] Entry: S3UploadOutputStream.completeUpload key={} uploadId={} partCount={}",
            key.value(), uploadId, completedParts.size());
        try {
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
            log.debug("[logzio-rsm-trace] Exit: S3UploadOutputStream.completeUpload key={} uploadId={} outcome=completed",
                key.value(), uploadId);
        } catch (final Error t) {
            log.error("[logzio-rsm-trace] S3UploadOutputStream.completeUpload ABORTED ABNORMALLY key={} uploadId={} cause={} message={}",
                key.value(), uploadId, t.getClass().getName(), t.getMessage(), t);
            throw t;
        }
    }

    private void abortUpload() {
        log.debug("[logzio-rsm-trace] Entry: S3UploadOutputStream.abortUpload key={} uploadId={}",
            key.value(), uploadId);
        final var request = AbortMultipartUploadRequest.builder()
            .bucket(bucketName)
            .key(key.value())
            .uploadId(uploadId)
            .build();
        client.abortMultipartUpload(request);
        log.debug("[logzio-rsm-trace] Exit: S3UploadOutputStream.abortUpload key={} uploadId={}",
            key.value(), uploadId);
    }

    private void flushBuffer(final ByteBuffer buffer,
                             final int actualPartSize,
                             final boolean multiPartUpload) {
        //When building the retry request for fail or computing checksum for request body,
        //It needs the input stream supporting marking and resetting so that it can be read again.
        try (final InputStream in = new ByteBufferMarkableInputStream(buffer)) {
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
        log.debug("[logzio-rsm-trace] Entry: S3UploadOutputStream.uploadPart key={} uploadId={} partNumber={} partSize={}",
            key.value(), uploadId, partNumber, actualPartSize);
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
        log.debug("[logzio-rsm-trace] Exit: S3UploadOutputStream.uploadPart key={} uploadId={} partNumber={} eTag={}",
            key.value(), uploadId, partNumber, uploadResult.eTag());
    }

    long processedBytes() {
        return processedBytes;
    }
}
