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
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.profile.AsyncProfiler;
import org.openjdk.jmh.runner.Runner;
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
        new Runner(new OptionsBuilder()
            .include(S3UploadWithMinioBench.class.getSimpleName())
            .addProfiler(AsyncProfiler.class, "-output=flamegraph;event=cpu")
            .build()).run();
        new Runner(new OptionsBuilder()
            .include(S3UploadWithMinioBench.class.getSimpleName())
            .addProfiler(AsyncProfiler.class, "-output=flamegraph;event=alloc")
            .build()).run();
    }
}
