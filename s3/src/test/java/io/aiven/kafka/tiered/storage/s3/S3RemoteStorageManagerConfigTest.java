/*
 * Copyright 2021 Aiven Oy
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

package io.aiven.kafka.tiered.storage.s3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3RemoteStorageManagerConfigTest {

    @Test
    void correctMinimalConfig() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("s3.bucket.name", "test_bucket");
        properties.put("s3.public_key_pem", "test_public_key");
        properties.put("s3.private_key_pem", "test_private_key");

        final S3RemoteStorageManagerConfig config = new S3RemoteStorageManagerConfig(properties);

        assertThat(config.s3BucketName()).isEqualTo("test_bucket");
        assertThat(config.s3Region()).isEqualTo(Regions.DEFAULT_REGION);
        assertThat(config.awsCredentialsProvider()).isNull();
        assertThat(config.publicKey()).isEqualTo("test_public_key");
        assertThat(config.privateKey()).isEqualTo("test_private_key");
        assertThat(config.ioBufferSize()).isEqualTo(8_192);
        assertThat(config.s3StorageUploadPartSize()).isEqualTo(1 << 19);
        assertThat(config.multiPartUploadPartSize()).isEqualTo(8_192);
        assertThat(config.encryptionMetadataCacheSize()).isEqualTo(1000);
        assertThat(config.encryptionMetadataCacheRetentionMs()).isEqualTo(1_800_000);
    }

    @Test
    void correctFullConfig() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("s3.bucket.name", "new_test_bucket");
        properties.put("s3.region", Regions.EU_NORTH_1.getName());
        properties.put("s3.credentials.provider.class",
                DefaultAWSCredentialsProviderChain.class.getName());
        properties.put("s3.public_key_pem", "new_test_public_key");
        properties.put("s3.private_key_pem", "new_test_private_key");
        properties.put("s3.io.buffer.size", "16384");
        properties.put("s3.upload.part.size", String.valueOf(1 << 10));
        properties.put("s3.multipart.upload.part.size", "16384");
        properties.put("s3.encryption.metadata.cache.size", "2000");
        properties.put("s3.encryption.metadata.cache.retention.ms", String.valueOf(3_600_000));

        final S3RemoteStorageManagerConfig config = new S3RemoteStorageManagerConfig(properties);

        assertThat(config.s3BucketName()).isEqualTo("new_test_bucket");
        assertThat(config.s3Region()).isEqualTo(Regions.EU_NORTH_1);
        assertThat(config.awsCredentialsProvider()).isNotNull();
        assertThat(config.publicKey()).isEqualTo("new_test_public_key");
        assertThat(config.privateKey()).isEqualTo("new_test_private_key");
        assertThat(config.ioBufferSize()).isEqualTo(16_384);
        assertThat(config.s3StorageUploadPartSize()).isEqualTo(1 << 10);
        assertThat(config.multiPartUploadPartSize()).isEqualTo(16_384);
        assertThat(config.encryptionMetadataCacheSize()).isEqualTo(2000);
        assertThat(config.encryptionMetadataCacheRetentionMs()).isEqualTo(3_600_000);
    }

    @Test
    void missingConfigPath() {
        final Map<String, String> properties = new HashMap<>();

        assertThatThrownBy(() -> new S3RemoteStorageManagerConfig(properties))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Missing required configuration \"s3.bucket.name\" which has no default value.");
    }

    @Test
    void invalidCredentialsProviderClass() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("s3.bucket.name", "test_bucket");
        properties.put("s3.public_key_pem", "test_public_key");
        properties.put("s3.private_key_pem", "test_private_key");
        properties.put("s3.credentials.provider.class", ArrayList.class.getName());

        assertThatThrownBy(() -> new S3RemoteStorageManagerConfig(properties))
                .isInstanceOf(KafkaException.class)
                .hasMessage(
                        "Invalid value class java.util.ArrayList for "
                                + "configuration s3.credentials.provider.class: "
                                + "Class must extend interface com.amazonaws.auth.AWSCredentialsProvider");


        properties.put("s3.credentials.provider.class", null);

        final S3RemoteStorageManagerConfig s3RemoteStorageManagerConfig = new S3RemoteStorageManagerConfig(properties);
        assertThat(s3RemoteStorageManagerConfig.awsCredentialsProvider()).isNull();

        properties.put("s3.credentials.provider.class", "invalid_provider");

        assertThatThrownBy(() -> new S3RemoteStorageManagerConfig(properties))
            .isInstanceOf(KafkaException.class)
            .hasMessage(
                "Invalid value invalid_provider for configuration s3.credentials.provider.class: "
                    + "Class invalid_provider could not be found.");
    }

    @Test
    void invalidCredentialsProvider() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("s3.bucket.name", "test_bucket");
        properties.put("s3.public_key_pem", "test_public_key");
        properties.put("s3.private_key_pem", "test_private_key");
        properties.put("s3.region", "test_string");

        assertThatThrownBy(() -> new S3RemoteStorageManagerConfig(properties))
                .isInstanceOf(KafkaException.class)
                .hasMessage("Invalid value test_string for configuration s3.region");

        properties.put("s3.region", null);

        assertThatThrownBy(() -> new S3RemoteStorageManagerConfig(properties))
                .isInstanceOf(KafkaException.class)
            .hasMessage("Invalid value null for configuration s3.region");
    }
}
