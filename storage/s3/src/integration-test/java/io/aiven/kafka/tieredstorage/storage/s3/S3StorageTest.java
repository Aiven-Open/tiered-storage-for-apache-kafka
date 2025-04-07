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

package io.aiven.kafka.tieredstorage.storage.s3;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.aiven.kafka.tieredstorage.storage.BaseStorageTest;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;
import io.aiven.kafka.tieredstorage.storage.TestObjectKey;
import io.aiven.kafka.tieredstorage.storage.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class S3StorageTest extends BaseStorageTest {
    @Container
    private static final LocalStackContainer LOCALSTACK = S3TestContainer.container();
    private static final int PART_SIZE = 8 * 1024 * 1024; // 8MiB

    private static S3Client s3Client;

    private String bucketName;

    @BeforeAll
    static void setUpClass() {
        s3Client = S3Client.builder()
            .region(Region.of(LOCALSTACK.getRegion()))
            .endpointOverride(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        LOCALSTACK.getAccessKey(),
                        LOCALSTACK.getSecretKey()
                    )
                )
            )
            .build();
    }

    @BeforeEach
    void setUp(final TestInfo testInfo) {
        bucketName = TestUtils.testNameToBucketName(testInfo);
        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
    }

    @AfterAll
    static void closeAll() {
        if (s3Client != null) {
            s3Client.close();
        }
    }

    @Override
    protected StorageBackend storage() {
        final S3Storage s3Storage = new S3Storage();
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", bucketName,
            "s3.region", LOCALSTACK.getRegion(),
            "s3.endpoint.url", LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3).toString(),
            "aws.access.key.id", LOCALSTACK.getAccessKey(),
            "aws.secret.access.key", LOCALSTACK.getSecretKey(),
            "s3.path.style.access.enabled", true,
            "s3.multipart.upload.part.size", PART_SIZE
        );
        s3Storage.configure(configs);
        return s3Storage;
    }

    @Test
    void partSizePassedToStream() throws IOException {
        try (final var os = ((S3Storage) storage()).s3OutputStream(new TestObjectKey("test"))) {
            assertThat(os.partSize).isEqualTo(PART_SIZE);
        }
    }

    @Test
    void testUploadANewFileWithBigSize() throws StorageBackendException, IOException {
        final String content = generateFileContentWithSizeOverPartLimit();
        uploadContentAsFileAndVerify(content);
    }

    private static String generateFileContentWithSizeOverPartLimit() {
        return RandomStringUtils.randomAlphabetic(PART_SIZE + 1);
    }

    @Test
    void testUploadRequestAsSingleFile() throws Exception {
        final S3Storage storage = (S3Storage) storage();
        storage.s3Client = Mockito.spy(storage.s3Client);

        final String content = "AABBBBAA";
        storage.upload(new ByteArrayInputStream(content.getBytes()), TOPIC_PARTITION_SEGMENT_KEY);
        Mockito.verify(storage.s3Client, Mockito.times(1))
            .putObject(Mockito.any(PutObjectRequest.class), Mockito.any(RequestBody.class));
    }

    @Test
    void testUploadRequestAsMultiPart() throws Exception {
        final S3Storage storage = (S3Storage) storage();
        storage.s3Client = Mockito.spy(storage.s3Client);

        final String content = generateFileContentWithSizeOverPartLimit();
        storage.upload(new ByteArrayInputStream(content.getBytes()), TOPIC_PARTITION_SEGMENT_KEY);
        Mockito.verify(storage.s3Client, Mockito.times(1))
            .createMultipartUpload(Mockito.any(CreateMultipartUploadRequest.class));
    }

    @Test
    void testDeleteMoreThanLimit() throws Exception {
        // spy s3Client
        final S3Client s3ClientSpy = Mockito.spy(s3Client);

        final S3Storage storage = (S3Storage) storage();
        storage.s3Client = s3ClientSpy;

        // Given 2002 keys
        // 1000 is the limit for deleteObjects
        final Set<ObjectKey> keys = IntStream.range(0, 2002)
            .mapToObj(i -> new TestObjectKey("key" + i))
            .collect(Collectors.toSet());

        // When deleting all keys
        storage.delete(keys);

        // Then deleteObjects is called 3 times
        final ArgumentCaptor<DeleteObjectsRequest> requestCaptor = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        Mockito.verify(s3ClientSpy, Mockito.times(3))
            .deleteObjects(requestCaptor.capture());

        // Get all captured requests
        final List<DeleteObjectsRequest> capturedRequests = requestCaptor.getAllValues();

        // Verify we got 3 requests
        assertThat(capturedRequests).hasSize(3);

        // First two requests should have 1000 objects each (the maximum)
        assertThat(capturedRequests.get(0).delete().objects()).hasSize(1000);
        assertThat(capturedRequests.get(1).delete().objects()).hasSize(1000);

        // Last request should have the remaining 2 objects
        assertThat(capturedRequests.get(2).delete().objects()).hasSize(2);

        // Verify that all original keys were included in the delete requests
        final Set<String> allDeletedKeys = capturedRequests.stream()
            .flatMap(req -> req.delete().objects().stream())
            .map(ObjectIdentifier::key)
            .collect(Collectors.toSet());

        final Set<String> originalKeys = keys.stream()
            .map(ObjectKey::toString)
            .collect(Collectors.toSet());

        // Verify all keys were included in the delete requests
        assertThat(allDeletedKeys).containsExactlyInAnyOrderElementsOf(originalKeys);
    }
}
