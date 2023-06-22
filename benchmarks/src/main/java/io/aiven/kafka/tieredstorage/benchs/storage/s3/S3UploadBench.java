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

package io.aiven.kafka.tieredstorage.benchs.storage.s3;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.aiven.kafka.tieredstorage.storage.s3.S3MultiPartOutputStream;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5, time = 30)
@BenchmarkMode({Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public abstract class S3UploadBench {

    static final String BUCKET_NAME = "kafka-ts-benchmark-test";
    public static final String OBJECT_PREFIX = "topic/partition";
    static final String OBJECT_KEY = OBJECT_PREFIX + "/log";

    static AmazonS3 s3Client;
    static Path segmentPath;
    @Param({"104857600", "209715200", "524288000", "2147483000"})
    public int contentLength; // 100MiB, 200MiB, 500MiB, 1GiB, 2GiB

    @Param({"5242880", "8388608", "10485760", "52428800", "83886080"})
    public int partSize; // 5MiB, 8MiB, 10MiB, 50MiB, 80MiB

    abstract AmazonS3 s3();

    @Setup(Level.Trial)
    public void setup() {
        try {
            segmentPath = Files.createTempFile("segment", ".log");
            // to fill with random bytes.
            final SecureRandom secureRandom = new SecureRandom();
            try (final var out = Files.newOutputStream(segmentPath)) {
                final byte[] bytes = new byte[contentLength];
                secureRandom.nextBytes(bytes);
                out.write(bytes);
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @TearDown
    public void teardown() {
        try {
            Files.deleteIfExists(segmentPath);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Benchmark
    public Object multiPartUploadBenchmark() {
        final var key = OBJECT_KEY + "_" + Instant.now().toString();
        try (final var out = new S3MultiPartOutputStream(BUCKET_NAME, key, partSize, s3Client);
             final InputStream inputStream = Files.newInputStream(segmentPath)) {
            inputStream.transferTo(out);
        } catch (final AmazonS3Exception | IOException e) {
            throw new RuntimeException("Failed to upload " + OBJECT_KEY, e);
        }
        return new Object();
    }

    @Benchmark
    public Object putObjectBenchmark() {
        final ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(contentLength);
        try (final InputStream in = Files.newInputStream(segmentPath)) {
            final var key = OBJECT_KEY + "_" + Instant.now().toString();
            final PutObjectRequest offsetPutRequest = new PutObjectRequest(
                BUCKET_NAME,
                key,
                in,
                metadata);
            return s3Client.putObject(offsetPutRequest);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Benchmark
    public Object blockingMultiPartUpload() {
        final List<PartETag> partETags = new ArrayList<>();
        final var key = OBJECT_KEY + "_" + Instant.now().toString();
        final InitiateMultipartUploadRequest multipartRequest =
            new InitiateMultipartUploadRequest(BUCKET_NAME, key);
        InitiateMultipartUploadResult initiated = null;
        try {
            initiated = s3Client.initiateMultipartUpload(multipartRequest);
            long partSize = this.partSize;
            long filePosition = 0;
            final File file = segmentPath.toFile();
            for (int i = 1; filePosition < contentLength; i++) {
                // Because the last part could be less than 5 MB, adjust the part size as needed.
                partSize = Math.min(partSize, contentLength - filePosition);

                // Create the request to upload a part.
                final UploadPartRequest uploadRequest = new UploadPartRequest()
                    .withBucketName(BUCKET_NAME)
                    .withKey(key)
                    .withUploadId(initiated.getUploadId())
                    .withPartNumber(i)
                    .withFileOffset(filePosition)
                    .withFile(file)
                    .withPartSize(partSize);

                // Upload the part and add the response's ETag to our list.
                final UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);
                partETags.add(uploadResult.getPartETag());

                filePosition += partSize;
            }
            // Complete the multipart upload.
            final CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(
                BUCKET_NAME,
                key,
                initiated.getUploadId(),
                partETags
            );
            return s3Client.completeMultipartUpload(compRequest);
        } catch (final Exception e) {
            if (initiated != null) {
                final AbortMultipartUploadRequest abortRequest =
                    new AbortMultipartUploadRequest(BUCKET_NAME, key, initiated.getUploadId());
                s3Client.abortMultipartUpload(abortRequest);
            }
            throw new RuntimeException(e);
        }
    }
}
