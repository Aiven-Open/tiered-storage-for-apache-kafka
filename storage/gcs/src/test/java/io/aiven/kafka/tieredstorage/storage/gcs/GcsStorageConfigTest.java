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

import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import com.google.auth.oauth2.GoogleCredentials;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
        final Map<String, Object> configs = Map.of("gcs.bucket.name", bucketName);
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
    void emptyGcsBucketName() {
        assertThatThrownBy(() -> new GcsStorageConfig(Map.of("gcs.bucket.name", "")))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value  for configuration gcs.bucket.name: String must be non-empty");
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
    void jsonAndPathCredentialsSet() {
        final var props = Map.of(
            "gcs.bucket.name", "bucket",
            "gcs.credentials.default", "false",
            "gcs.credentials.json", "json",
            "gcs.credentials.path", "path");
        assertThatThrownBy(() -> new GcsStorageConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("gcs.credentials.json and gcs.credentials.path cannot be set together");
    }

    @Test
    void jsonAndDefaultCredentialsSet() {
        final var props = Map.of(
            "gcs.bucket.name", "bucket",
            "gcs.credentials.json", "json",
            "gcs.credentials.default", "true");
        assertThatThrownBy(() -> new GcsStorageConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("gcs.credentials.json and gcs.credentials.default cannot be set together");
    }

    @Test
    void pathAndDefaultCredentialsSet() {
        final var props = Map.of(
            "gcs.bucket.name", "bucket",
            "gcs.credentials.path", "path",
            "gcs.credentials.default", "true");
        assertThatThrownBy(() -> new GcsStorageConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("gcs.credentials.path and gcs.credentials.default cannot be set together");
    }

    @Test
    void resumableUploadChunkSize() {
        final GcsStorageConfig config = new GcsStorageConfig(
            Map.of(
                "gcs.bucket.name", "b1",
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
