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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.aiven.kafka.tieredstorage.storage.s3.S3MultiPartOutputStream;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
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
import org.openjdk.jmh.profile.AsyncProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 2)
@Measurement(iterations = 3)
@BenchmarkMode({Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class S3UploadBench {

    static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
        DockerImageName.parse("localstack/localstack:2.0.2")
    ).withServices(LocalStackContainer.Service.S3);
    static final String BUCKET_NAME = "test";
    static final String OBJECT_KEY = "topic/partition/log";
    static final int DEFAULT_PART_SIZE = 5 * 1024 * 1024;

    static AmazonS3 s3Client;
    static Path segmentPath;
    @Param({"10485760", "104857600", "524288000", "1073741824"})
    public int contentLength; // 10MiB, 100MiB, 500MiB, 1GiB

    @Setup(Level.Trial)
    public void setup() {
        LOCALSTACK.start();

        s3Client = AmazonS3ClientBuilder
            .standard()
            .withEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration(
                    LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3).toString(),
                    LOCALSTACK.getRegion()
                )
            )
            .withCredentials(
                new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())
                )
            )
            .build();
        s3Client.createBucket(BUCKET_NAME);

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
        LOCALSTACK.stop();
        try {
            Files.deleteIfExists(segmentPath);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Benchmark
    public Object multiPartUploadBenchmark() {
        try (final var out = new S3MultiPartOutputStream(BUCKET_NAME, OBJECT_KEY, DEFAULT_PART_SIZE, s3Client);
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
            final PutObjectRequest offsetPutRequest = new PutObjectRequest(
                BUCKET_NAME,
                OBJECT_KEY,
                in,
                metadata);
            return s3Client.putObject(offsetPutRequest);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Benchmark
    public Object alternativeMultiPartUpload() {
        final List<PartETag> partETags = new ArrayList<>();
        final InitiateMultipartUploadRequest multipartRequest =
            new InitiateMultipartUploadRequest(BUCKET_NAME, OBJECT_KEY);
        InitiateMultipartUploadResult initiated = null;
        try {
            initiated = s3Client.initiateMultipartUpload(multipartRequest);
            long partSize = DEFAULT_PART_SIZE;
            long filePosition = 0;
            final File file = segmentPath.toFile();
            for (int i = 1; filePosition < contentLength; i++) {
                // Because the last part could be less than 5 MB, adjust the part size as needed.
                partSize = Math.min(partSize, contentLength - filePosition);

                // Create the request to upload a part.
                final UploadPartRequest uploadRequest = new UploadPartRequest()
                    .withBucketName(BUCKET_NAME)
                    .withKey(OBJECT_KEY)
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
                OBJECT_KEY,
                initiated.getUploadId(),
                partETags
            );
            return s3Client.completeMultipartUpload(compRequest);
        } catch (final Exception e) {
            if (initiated != null) {
                final AbortMultipartUploadRequest abortRequest =
                    new AbortMultipartUploadRequest(BUCKET_NAME, OBJECT_KEY, initiated.getUploadId());
                s3Client.abortMultipartUpload(abortRequest);
            }
            throw new RuntimeException(e);
        }
    }

    public static void main(final String[] args) throws Exception {
        final Options opts = new OptionsBuilder()
            .include(S3UploadBench.class.getSimpleName())
            .addProfiler(AsyncProfiler.class, "output=flamegraph")
            .build();
        new Runner(opts).run();
    }
}
