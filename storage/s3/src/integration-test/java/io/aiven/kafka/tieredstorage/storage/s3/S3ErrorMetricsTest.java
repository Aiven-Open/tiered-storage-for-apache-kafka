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

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.Random;

import io.aiven.kafka.tieredstorage.storage.StorageBackendException;
import io.aiven.kafka.tieredstorage.storage.TestObjectKey;

import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.S3Exception;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.common.ContentTypes.CONTENT_TYPE;
import static org.apache.http.entity.ContentType.APPLICATION_XML;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;

@WireMockTest
class S3ErrorMetricsTest {

    private static final Logger log = LoggerFactory.getLogger(S3ErrorMetricsTest.class);

    private static final String ERROR_RESPONSE_TEMPLATE = "<Error><Code>%s</Code></Error>";
    private static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();
    private static final String BUCKET_NAME = "test-bucket";
    private S3Storage storage;
    private ObjectName s3MetricsObjectName;
    private final Random random = new Random();
    private static final int UPLOAD_PART_SIZE = 25 * 1024 * 1024;

    @BeforeEach
    void setUp() throws MalformedObjectNameException {
        s3MetricsObjectName = ObjectName.getInstance("aiven.kafka.server.tieredstorage.s3:type=s3-client-metrics");
        storage = new S3Storage();
    }

    @ParameterizedTest
    @CsvSource({
        HttpStatusCode.INTERNAL_SERVER_ERROR + "," + (UPLOAD_PART_SIZE + 1) + ", server-errors",
        HttpStatusCode.INTERNAL_SERVER_ERROR + "," + (UPLOAD_PART_SIZE - 1) + ", server-errors",
        HttpStatusCode.THROTTLING + "," + (UPLOAD_PART_SIZE + 1) + ", throttling-errors",
        HttpStatusCode.THROTTLING + "," + (UPLOAD_PART_SIZE - 1) + ", throttling-errors",
    })
    void testS3ServerExceptions(final int statusCode,
                                final int uploadFileSize,
                                final String metricName,
                                final WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        log.info("test start with parameter: statusCode={}, uploadFileSize={}, metric={}",
                statusCode, uploadFileSize, metricName);
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", Region.US_EAST_1.id(),
            "s3.endpoint.url", wmRuntimeInfo.getHttpBaseUrl(),
            "s3.path.style.access.enabled", "true",
            "aws.credentials.provider.class", AnonymousCredentialsProvider.class.getName(),
            "s3.multipart.upload.part.size", UPLOAD_PART_SIZE
        );
        storage.configure(configs);

        stubFor(any(anyUrl())
            .willReturn(aResponse().withStatus(statusCode)
                .withHeader(CONTENT_TYPE, APPLICATION_XML.getMimeType())
                .withBody(String.format(ERROR_RESPONSE_TEMPLATE, statusCode))));

        uploadAndVerifyResult(uploadFileSize, statusCode, metricName);
        log.info("test end with parameter: statusCode={}, "
                + "uploadFileSize={}, metric={}", statusCode, uploadFileSize, metricName);

    }

    private void uploadAndVerifyResult(final int uploadFileSize,
                                       final int statusCode,
                                       final String metricName) throws Exception {
        try (final ByteArrayInputStream inputStream = newInputStreamWithSize(uploadFileSize)) {
            final StorageBackendException storageBackendException = catchThrowableOfType(
                StorageBackendException.class,
                () -> storage.upload(inputStream, new TestObjectKey("key"))
            );
            log.info("test run with parameter: statusCode={}, "
                    + "uploadFileSize={}, metric={}", statusCode, uploadFileSize, metricName);
            log.info("storageBackendException={}", storageBackendException);
            log.info("getCause={}", storageBackendException.getCause());
            log.info("getCauseCause={}", storageBackendException.getCause().getCause());
            log.info("getCauseCause={}",
                    ((S3Exception) storageBackendException.getCause().getCause()).statusCode());
            log.error("exception for: " + storageBackendException.getMessage(), storageBackendException);
            System.err.println(storageBackendException);
            storageBackendException.printStackTrace();

            assertThat(storageBackendException.getCause())
                .isInstanceOf(IOException.class)
                .cause()
                .isInstanceOf(S3Exception.class)
                .extracting(e -> ((S3Exception) e).statusCode())
                .isEqualTo(statusCode);
        }

        // Comparing to 4 since the SDK makes 3 retries by default.
        assertThat(MBEAN_SERVER.getAttribute(s3MetricsObjectName, metricName + "-total"))
            .isEqualTo(4.0);
        assertThat(MBEAN_SERVER.getAttribute(s3MetricsObjectName, metricName + "-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
    }

    @Test
    void testS3ServerExceptionsWhenUploadPartFail(final WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", Region.US_EAST_1.id(),
            "s3.endpoint.url", wmRuntimeInfo.getHttpBaseUrl(),
            "s3.path.style.access.enabled", "true",
            "aws.credentials.provider.class", AnonymousCredentialsProvider.class.getName()
        );
        storage.configure(configs);

        //stub with success response for the CreateMultipartUpload: POST /{Key}?uploads
        final String responseString = "<InitiateMultipartUploadResult>"
                                    + "<UploadId>testUploadId</UploadId>"
                                    + "</InitiateMultipartUploadResult>";
        stubFor(post(anyUrl())
            .willReturn(aResponse().withStatus(HttpStatusCode.OK)
                .withHeader(CONTENT_TYPE, APPLICATION_XML.getMimeType())
                .withBody(responseString)));

        //stub with error for the UploadPart: PUT /{Key}?partNumber=PartNumber&uploadId={Upload}
        final int errorStatusCode = HttpStatusCode.INTERNAL_SERVER_ERROR;
        stubFor(put(anyUrl())
            .willReturn(aResponse().withStatus(errorStatusCode)
                .withHeader(CONTENT_TYPE, APPLICATION_XML.getMimeType())
                .withBody(String.format(ERROR_RESPONSE_TEMPLATE, errorStatusCode))));

        //stub with success response for the AbortMultipartUpload: DELETE /{Key}?uploadId={Upload}
        stubFor(delete(anyUrl())
            .willReturn(aResponse().withStatus(HttpStatusCode.OK)));

        uploadAndVerifyResult(UPLOAD_PART_SIZE + 1, errorStatusCode, "server-errors");
    }

    private ByteArrayInputStream newInputStreamWithSize(final int size) {
        final byte[] buffer = new byte[size];
        random.nextBytes(buffer);
        return new ByteArrayInputStream(buffer);
    }

    @Test
    void testOtherExceptions(final WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", Region.US_EAST_1.id(),
            "s3.endpoint.url", wmRuntimeInfo.getHttpBaseUrl(),
            "s3.path.style.access.enabled", "true",
            "aws.credentials.provider.class", AnonymousCredentialsProvider.class.getName()
        );
        storage.configure(configs);

        final var statusCode = HttpStatusCode.OK;
        final var metricName = "other-errors";
        stubFor(any(anyUrl())
            .willReturn(aResponse()
                .withStatus(statusCode)
                .withHeader(CONTENT_TYPE, APPLICATION_XML.getMimeType())
                .withBody("unparsable_xml")));

        final ByteArrayInputStream inputStream = newInputStreamWithSize(UPLOAD_PART_SIZE + 1);
        assertThatThrownBy(() -> storage.upload(inputStream, new TestObjectKey("key")))
            .isInstanceOf(StorageBackendException.class)
            .hasMessageStartingWith("Failed to upload key");

        assertThat(MBEAN_SERVER.getAttribute(s3MetricsObjectName, metricName + "-total"))
            .isEqualTo(1.0);
        assertThat(MBEAN_SERVER.getAttribute(s3MetricsObjectName, metricName + "-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
    }

    @Test
    void apiCallAttemptTimeout(final WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", Region.US_EAST_1.id(),
            "s3.endpoint.url", wmRuntimeInfo.getHttpBaseUrl(),
            "s3.path.style.access.enabled", "true",
            "s3.api.call.attempt.timeout", 1,
            "aws.credentials.provider.class", AnonymousCredentialsProvider.class.getName()
        );
        storage.configure(configs);
        final var metricName = "configured-timeout-errors";

        stubFor(any(anyUrl()).willReturn(aResponse().withFixedDelay(100)));

        assertThatThrownBy(() -> storage.fetch(new TestObjectKey("key")))
            .isExactlyInstanceOf(StorageBackendException.class)
            .hasMessage("Failed to fetch key")
            .rootCause()
            .isInstanceOf(ApiCallAttemptTimeoutException.class)
            .hasMessageStartingWith(
                "HTTP request execution did not complete before the specified timeout configuration: 1 millis");

        // Comparing to 4 since the SDK makes 3 retries by default.
        assertThat(MBEAN_SERVER.getAttribute(s3MetricsObjectName, metricName + "-total"))
            .isEqualTo(4.0);
        assertThat(MBEAN_SERVER.getAttribute(s3MetricsObjectName, metricName + "-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
    }

    @Test
    void ioErrors(final WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", Region.US_EAST_1.id(),
            "s3.endpoint.url", wmRuntimeInfo.getHttpBaseUrl(),
            "s3.path.style.access.enabled", "true",
            "aws.credentials.provider.class", AnonymousCredentialsProvider.class.getName()
        );
        storage.configure(configs);

        final var metricName = "io-errors";

        stubFor(any(anyUrl())
            .willReturn(aResponse()
                .withStatus(HttpStatusCode.OK)
                .withFault(Fault.RANDOM_DATA_THEN_CLOSE)));

        assertThatThrownBy(() -> storage.fetch(new TestObjectKey("key")))
            .isExactlyInstanceOf(StorageBackendException.class)
            .hasMessage("Failed to fetch key")
            .hasCauseExactlyInstanceOf(SdkClientException.class)
            .cause()
            .hasMessageStartingWith("Unable to execute HTTP request: null");

        // Comparing to 4 since the SDK makes 3 retries by default.
        assertThat(MBEAN_SERVER.getAttribute(s3MetricsObjectName, metricName + "-total"))
            .isEqualTo(4.0);
        assertThat(MBEAN_SERVER.getAttribute(s3MetricsObjectName, metricName + "-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
    }
}
