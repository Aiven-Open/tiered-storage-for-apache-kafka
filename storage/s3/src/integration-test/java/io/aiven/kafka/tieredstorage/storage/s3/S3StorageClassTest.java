/*
 * Copyright 2026 Aiven Oy
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
import java.util.HashMap;
import java.util.Map;

import io.aiven.kafka.tieredstorage.storage.TestObjectKey;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.regions.Region;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.absent;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.reset;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;

@WireMockTest
class S3StorageClassTest {
    private static final String BUCKET_NAME = "test-bucket";
    private static final String STORAGE_CLASS_HEADER = "x-amz-storage-class";
    private static final int UPLOAD_PART_SIZE = 5 * 1024 * 1024;
    private static final TestObjectKey OBJECT_KEY = new TestObjectKey("topic-0/0000.log");
    private static final String OBJECT_PATH = "/" + BUCKET_NAME + "/" + OBJECT_KEY.value();

    @BeforeEach
    void setUp() {
        reset();
        stubFor(put(urlPathEqualTo(OBJECT_PATH))
            .willReturn(aResponse().withStatus(200).withHeader("ETag", "\"etag-1\"")));
        stubFor(post(urlPathEqualTo(OBJECT_PATH))
            .withQueryParam("uploads", equalTo(""))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/xml")
                .withBody("<InitiateMultipartUploadResult><UploadId>upload-1</UploadId>"
                    + "</InitiateMultipartUploadResult>")));
        stubFor(post(urlPathEqualTo(OBJECT_PATH))
            .withQueryParam("uploadId", equalTo("upload-1"))
            .willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/xml")
                .withBody("<CompleteMultipartUploadResult><ETag>\"etag-final\"</ETag>"
                    + "</CompleteMultipartUploadResult>")));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   "})
    void omitsStorageClassHeaderWhenUnsetOrBlank(final String storageClass,
                                                 final WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        try (final S3Storage storage = configuredStorage(wmRuntimeInfo, storageClass)) {
            storage.upload(new ByteArrayInputStream(new byte[16]), OBJECT_KEY);
            storage.upload(new ByteArrayInputStream(new byte[UPLOAD_PART_SIZE + 1]), OBJECT_KEY);
        }

        verify(putRequestedFor(urlPathEqualTo(OBJECT_PATH))
            .withQueryParam("uploadId", absent())
            .withHeader(STORAGE_CLASS_HEADER, absent()));
        verify(postRequestedFor(urlPathEqualTo(OBJECT_PATH))
            .withQueryParam("uploads", equalTo(""))
            .withHeader(STORAGE_CLASS_HEADER, absent()));
    }

    @Test
    void sendsCustomStorageClassHeaderOnSingleAndMultipartUploads(
        final WireMockRuntimeInfo wmRuntimeInfo
    ) throws Exception {
        final String customStorageClass = "MAZ_STANDARD";
        try (final S3Storage storage = configuredStorage(wmRuntimeInfo, customStorageClass)) {
            storage.upload(new ByteArrayInputStream(new byte[16]), OBJECT_KEY);
            storage.upload(new ByteArrayInputStream(new byte[UPLOAD_PART_SIZE + 1]), OBJECT_KEY);
        }

        verify(putRequestedFor(urlPathEqualTo(OBJECT_PATH))
            .withQueryParam("uploadId", absent())
            .withHeader(STORAGE_CLASS_HEADER, equalTo(customStorageClass)));
        verify(postRequestedFor(urlPathEqualTo(OBJECT_PATH))
            .withQueryParam("uploads", equalTo(""))
            .withHeader(STORAGE_CLASS_HEADER, equalTo(customStorageClass)));
    }

    private S3Storage configuredStorage(final WireMockRuntimeInfo wmRuntimeInfo, final String storageClass) {
        final Map<String, Object> configs = new HashMap<>();
        configs.put("s3.bucket.name", BUCKET_NAME);
        configs.put("s3.region", Region.US_EAST_1.id());
        configs.put("s3.endpoint.url", wmRuntimeInfo.getHttpBaseUrl());
        configs.put("s3.path.style.access.enabled", true);
        configs.put("s3.multipart.upload.part.size", UPLOAD_PART_SIZE);
        configs.put("aws.credentials.provider.class", AnonymousCredentialsProvider.class.getName());
        if (storageClass != null) {
            configs.put("s3.storage.class", storageClass);
        }
        final S3Storage storage = new S3Storage();
        storage.configure(configs);
        return storage;
    }
}
