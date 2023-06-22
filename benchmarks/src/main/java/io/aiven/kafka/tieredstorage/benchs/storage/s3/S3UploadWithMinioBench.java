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
import io.aiven.kafka.tieredstorage.storage.s3.S3MultiPartOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
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

public class S3UploadWithMinioBench extends S3UploadBench {

    static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
        DockerImageName.parse("localstack/localstack:2.0.2")
    ).withServices(LocalStackContainer.Service.S3);

    @Override
    AmazonS3 s3() {
        final var s3Client = AmazonS3ClientBuilder
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
        return s3Client;
    }

    @Setup(Level.Trial)
    public void setup() {
        LOCALSTACK.start();

        s3Client = s3();

        super.setup();
    }

    @TearDown
    public void teardown() {
        LOCALSTACK.stop();

        super.teardown();
    }


    public static void main(final String[] args) throws Exception {
        final Options opts = new OptionsBuilder()
            .include(S3UploadWithMinioBench.class.getSimpleName())
            .addProfiler(AsyncProfiler.class, "-output=flamegraph,collapsed")
            .build();
        new Runner(opts).run();
    }
}
