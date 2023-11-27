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

import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.Map;

import io.aiven.kafka.tieredstorage.storage.TestObjectKey;

import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.S3Exception;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.any;
import static com.github.tomakehurst.wiremock.client.WireMock.anyUrl;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.common.ContentTypes.CONTENT_TYPE;
import static org.apache.http.entity.ContentType.APPLICATION_XML;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;

@WireMockTest
class S3ErrorMetricsTest {
    private static final String ERROR_RESPONSE_TEMPLATE = "<Error><Code>%s</Code></Error>";
    private static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();
    private static final String BUCKET_NAME = "test-bucket";
    private S3Storage storage;
    private ObjectName s3MetricsObjectName;

    @BeforeEach
    void setUp() throws MalformedObjectNameException {
        s3MetricsObjectName = ObjectName.getInstance("aiven.kafka.server.tieredstorage.s3:type=s3-client-metrics");
        storage = new S3Storage();
    }

    @ParameterizedTest
    @CsvSource({
        HttpStatusCode.INTERNAL_SERVER_ERROR + ", server-errors",
        HttpStatusCode.THROTTLING + ", throttling-errors",
    })
    void testS3ServerExceptions(final int statusCode,
                                final String metricName,
                                final WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", Region.US_EAST_1.id(),
            "s3.endpoint.url", wmRuntimeInfo.getHttpBaseUrl(),
            "aws.credentials.provider.class", AnonymousCredentialsProvider.class.getName()
        );
        storage.configure(configs);

        stubFor(any(anyUrl())
            .willReturn(aResponse().withStatus(statusCode)
                .withHeader(CONTENT_TYPE, APPLICATION_XML.getMimeType())
                .withBody(String.format(ERROR_RESPONSE_TEMPLATE, statusCode))));
        final S3Exception s3Exception = catchThrowableOfType(
            () -> storage.upload(InputStream.nullInputStream(), new TestObjectKey("key")),
            S3Exception.class);

        assertThat(s3Exception.statusCode()).isEqualTo(statusCode);

        // Comparing to 4 since the SDK makes 3 retries by default.
        assertThat(MBEAN_SERVER.getAttribute(s3MetricsObjectName, metricName + "-total"))
            .isEqualTo(4.0);
        assertThat(MBEAN_SERVER.getAttribute(s3MetricsObjectName, metricName + "-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
    }

    @Test
    void testOtherExceptions(final WireMockRuntimeInfo wmRuntimeInfo) throws Exception {
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", Region.US_EAST_1.id(),
            "s3.endpoint.url", wmRuntimeInfo.getHttpBaseUrl(),
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

        assertThatThrownBy(() -> storage.upload(InputStream.nullInputStream(), new TestObjectKey("key")))
            .isInstanceOf(SdkClientException.class)
            .hasMessage("Could not parse XML response.");

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
            "s3.api.call.attempt.timeout", 1,
            "aws.credentials.provider.class", AnonymousCredentialsProvider.class.getName()
        );
        storage.configure(configs);
        final var metricName = "configured-timeout-errors";

        stubFor(any(anyUrl()).willReturn(aResponse().withFixedDelay(100)));

        assertThatThrownBy(() -> storage.fetch(new TestObjectKey("key")))
            .isInstanceOf(ApiCallAttemptTimeoutException.class)
            .hasMessage("HTTP request execution did not complete before the specified timeout configuration: 1 millis");

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
            "aws.credentials.provider.class", AnonymousCredentialsProvider.class.getName()
        );
        storage.configure(configs);

        final var metricName = "io-errors";

        stubFor(any(anyUrl())
            .willReturn(aResponse()
                .withStatus(HttpStatusCode.OK)
                .withFault(Fault.RANDOM_DATA_THEN_CLOSE)));

        assertThatThrownBy(() -> storage.fetch(new TestObjectKey("key")))
            .isInstanceOf(SdkClientException.class)
            .hasMessage("Unable to execute HTTP request: null");

        // Comparing to 4 since the SDK makes 3 retries by default.
        assertThat(MBEAN_SERVER.getAttribute(s3MetricsObjectName, metricName + "-total"))
            .isEqualTo(4.0);
        assertThat(MBEAN_SERVER.getAttribute(s3MetricsObjectName, metricName + "-rate"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
    }
}
