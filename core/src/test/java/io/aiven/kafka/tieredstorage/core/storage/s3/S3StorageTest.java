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

package io.aiven.kafka.tieredstorage.core.storage.s3;

import io.aiven.kafka.tieredstorage.core.storage.BaseStorageTest;
import io.aiven.kafka.tieredstorage.core.storage.FileDeleter;
import io.aiven.kafka.tieredstorage.core.storage.FileFetcher;
import io.aiven.kafka.tieredstorage.core.storage.FileUploader;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class S3StorageTest extends BaseStorageTest {
    @Container
    public static final LocalStackContainer LOCALSTACK = new LocalStackContainer(
        DockerImageName.parse("localstack/localstack:2.0.2")
    ).withServices(LocalStackContainer.Service.S3);

    static AmazonS3 s3Client;
    private String bucketName;

    @BeforeAll
    public static void setUpClass() {
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
    }

    @BeforeEach
    public void setUp(final TestInfo testInfo) {
        bucketName = testInfo.getDisplayName()
            .toLowerCase()
            .replace("(", "")
            .replace(")", "");
        s3Client.createBucket(bucketName);
    }

    @Override
    protected FileUploader uploader() {
        return new S3Storage(s3Client, bucketName);
    }

    @Override
    protected FileFetcher fetcher() {
        return new S3Storage(s3Client, bucketName);
    }

    @Override
    protected FileDeleter deleter() {
        return new S3Storage(s3Client, bucketName);
    }
}
