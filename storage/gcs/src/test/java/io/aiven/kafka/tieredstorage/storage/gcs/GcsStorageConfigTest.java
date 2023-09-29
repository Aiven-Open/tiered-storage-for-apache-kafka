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

package io.aiven.kafka.tieredstorage.storage.gcs;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigException;

import com.google.auth.oauth2.GoogleCredentials;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
class GcsStorageConfigTest {
    @Test
    void minimalConfig() {
        final String bucketName = "b1";
        final Map<String, Object> configs = Map.of(
            "gcs.bucket.name", bucketName,
            "gcs.credentials.default", "true");
        final GcsStorageConfig config = new GcsStorageConfig(configs);
        assertThat(config.bucketName()).isEqualTo(bucketName);
        assertThat(config.endpointUrl()).isNull();
        assertThat(config.resumableUploadChunkSize()).isNull();

        final GoogleCredentials mockCredentials = GoogleCredentials.newBuilder().build();
        try (final MockedStatic<GoogleCredentials> googleCredentialsMockedStatic =
                 Mockito.mockStatic(GoogleCredentials.class)) {
            googleCredentialsMockedStatic.when(GoogleCredentials::getApplicationDefault).thenReturn(mockCredentials);
            assertThat(config.credentials()).isSameAs(mockCredentials);
        }
    }

    @Test
    void invalidEndpointUrl() {
        assertThatThrownBy(() -> new GcsStorageConfig(
            Map.of(
                "gcs.bucket.name", "bucket",
                "gcs.endpoint.url", "invalid_url")
        ))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value invalid_url for configuration gcs.endpoint.url: Must be a valid URL");

        assertThatThrownBy(() -> new GcsStorageConfig(
            Map.of(
                "gcs.bucket.name", "bucket",
                "gcs.endpoint.url", "ftp://invalid_url")
        ))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value ftp://invalid_url for configuration gcs.endpoint.url: "
                + "URL must have scheme from the list [http, https]");
    }

    @Test
    void emptyGcsBucketName() {
        assertThatThrownBy(() -> new GcsStorageConfig(Map.of("gcs.bucket.name", "")))
            .isInstanceOf(ConfigException.class)
            .hasMessage("gcs.bucket.name value must not be empty");
    }

    @Test
    void emptyJsonCredentials() {
        final var props = Map.of(
            "gcs.bucket.name", "bucket",
            "gcs.credentials.json", ""
        );
        assertThatThrownBy(() -> new GcsStorageConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("gcs.credentials.json value must not be empty");
    }

    @Test
    void emptyPathCredentials() {
        final var props = Map.of(
            "gcs.bucket.name", "bucket",
            "gcs.credentials.path", ""
        );
        assertThatThrownBy(() -> new GcsStorageConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value  for configuration gcs.credentials.path: String must be non-empty");
    }

    @Test
    void allCredentialsNull() {
        final var props = Map.of(
            "gcs.bucket.name", "bucket"
        );
        assertThatThrownBy(() -> new GcsStorageConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("All gcs.credentials.default, gcs.credentials.json, and gcs.credentials.path "
                + "cannot be null simultaneously.");
    }

    @ParameterizedTest
    @MethodSource("provideMoreThanOneCredentialsNonNull")
    void moreThanOneCredentialsNonNull(final Boolean defaultCredentials,
                                       final String credentialsJson,
                                       final String credentialsPath) {
        final Map<String, Object> props = new HashMap<>();
        props.put("gcs.bucket.name", "bucket");
        props.put("gcs.credentials.default", defaultCredentials);
        props.put("gcs.credentials.json", credentialsJson);
        props.put("gcs.credentials.path", credentialsPath);
        assertThatThrownBy(() -> new GcsStorageConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Only one of gcs.credentials.default, gcs.credentials.json, and gcs.credentials.path "
                + "can be non-null.");
    }

    private static Stream<Arguments> provideMoreThanOneCredentialsNonNull() {
        return Stream.of(
            Arguments.of(true, "json", "path"),
            Arguments.of(false, "json", "path"),
            Arguments.of(true, "json", null),
            Arguments.of(false, "json", null),
            Arguments.of(true, null, "path"),
            Arguments.of(false, null, "path"),
            Arguments.of(null, "json", "path")
        );
    }

    @Test
    void resumableUploadChunkSize() {
        final GcsStorageConfig config = new GcsStorageConfig(
            Map.of(
                "gcs.bucket.name", "b1",
                "gcs.credentials.default", "true",
                "gcs.resumable.upload.chunk.size", Integer.toString(10 * 1024 * 1024)
            )
        );
        assertThat(config.resumableUploadChunkSize()).isEqualTo(10 * 1024 * 1024);
    }

    @Test
    void invalidResumableUploadChunkSize() {
        final var configNotMultipleOf268KiB = Map.of(
            "gcs.bucket.name", "b1",
            "gcs.resumable.upload.chunk.size", Integer.toString(128 * 1024 * 3)
        );
        assertThatThrownBy(() -> new GcsStorageConfig(configNotMultipleOf268KiB))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 393216 for configuration gcs.resumable.upload.chunk.size: "
                + "Value must be a multiple of 256 KiB (262144 B)");

        final var configTooSmall = Map.of(
            "gcs.bucket.name", "b1",
            "gcs.resumable.upload.chunk.size", Integer.toString(256 * 1024 - 1)
        );
        assertThatThrownBy(() -> new GcsStorageConfig(configTooSmall))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 262143 for configuration gcs.resumable.upload.chunk.size: "
                + "Value must be at least 256 KiB (262144 B)");
    }
}
