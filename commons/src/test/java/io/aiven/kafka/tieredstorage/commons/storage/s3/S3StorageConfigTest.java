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

package io.aiven.kafka.tieredstorage.commons.storage.s3;

import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3StorageConfigTest {

    // Test scenarios
    // - Minimal/Full configs
    @Test
    void minimalConfig() {
        final String bucketName = "b1";
        final Map<String, Object> configs = Map.of(S3StorageConfig.S3_BUCKET_NAME_CONFIG, bucketName);
        final S3StorageConfig config = new S3StorageConfig(configs);
        assertThat(config.bucketName()).isEqualTo(bucketName);
    }

    // - Credential provider scenarios
    //   - Without provider
    @Test
    void configWithoutProvider() {
        final String bucketName = "b1";
        final String region = Regions.US_EAST_2.getName();
        final String minioUrl = "http://minio";
        final Map<String, Object> configs = Map.of(
            S3StorageConfig.S3_BUCKET_NAME_CONFIG, bucketName,
            S3StorageConfig.S3_REGION_CONFIG, region,
            S3StorageConfig.S3_ENDPOINT_URL_CONFIG, minioUrl);
        final S3StorageConfig config = new S3StorageConfig(configs);
        assertThat(config.bucketName()).isEqualTo(bucketName);
        assertThat(config.getString(S3StorageConfig.S3_REGION_CONFIG)).isEqualTo(region);
        assertThat(config.getString(S3StorageConfig.S3_ENDPOINT_URL_CONFIG)).isEqualTo(minioUrl);
        assertThat(config.getClass(S3StorageConfig.AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG)).isNull();

        final AWSCredentialsProvider credentialsProvider = config.credentialsProvider();
        assertThat(credentialsProvider).isNull();
    }

    //   - With provider
    @Test
    void configWithProvider() {
        final String bucketName = "b1";
        final String region = Regions.US_EAST_2.getName();
        final String minioUrl = "http://minio";
        final String customConfigProvider = EnvironmentVariableCredentialsProvider.class.getName();
        final Map<String, Object> configs = Map.of(
            S3StorageConfig.S3_BUCKET_NAME_CONFIG, bucketName,
            S3StorageConfig.S3_REGION_CONFIG, region,
            S3StorageConfig.S3_ENDPOINT_URL_CONFIG, minioUrl,
            S3StorageConfig.AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG, customConfigProvider);
        final S3StorageConfig config = new S3StorageConfig(configs);
        assertThat(config.bucketName()).isEqualTo(bucketName);
        assertThat(config.getString(S3StorageConfig.S3_REGION_CONFIG)).isEqualTo(region);
        assertThat(config.getString(S3StorageConfig.S3_ENDPOINT_URL_CONFIG)).isEqualTo(minioUrl);
        assertThat(config.getClass(S3StorageConfig.AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG).getName())
            .isEqualTo(customConfigProvider);

        final AWSCredentialsProvider credentialsProvider = config.credentialsProvider();
        assertThat(credentialsProvider).isInstanceOf(EnvironmentVariableCredentialsProvider.class);
    }

    //   - With static configs
    @Test
    void configWithStaticCredentials() {
        final String bucketName = "b1";
        final String region = Regions.US_EAST_2.getName();
        final String minioUrl = "http://minio";
        final String username = "username";
        final String password = "password";
        final Map<String, Object> configs = Map.of(
            S3StorageConfig.S3_BUCKET_NAME_CONFIG, bucketName,
            S3StorageConfig.S3_REGION_CONFIG, region,
            S3StorageConfig.S3_ENDPOINT_URL_CONFIG, minioUrl,
            S3StorageConfig.AWS_ACCESS_KEY_ID_CONFIG, username,
            S3StorageConfig.AWS_SECRET_ACCESS_KEY_CONFIG, password);
        final S3StorageConfig config = new S3StorageConfig(configs);
        assertThat(config.bucketName()).isEqualTo(bucketName);
        assertThat(config.getString(S3StorageConfig.S3_REGION_CONFIG)).isEqualTo(region);
        assertThat(config.getString(S3StorageConfig.S3_ENDPOINT_URL_CONFIG)).isEqualTo(minioUrl);
        assertThat(config.getPassword(S3StorageConfig.AWS_ACCESS_KEY_ID_CONFIG).value()).isEqualTo(username);
        assertThat(config.getPassword(S3StorageConfig.AWS_SECRET_ACCESS_KEY_CONFIG).value()).isEqualTo(password);

        final AWSCredentialsProvider credentialsProvider = config.credentialsProvider();
        assertThat(credentialsProvider).isInstanceOf(AWSStaticCredentialsProvider.class);
    }

    //   - With missing static config
    @Test
    void configWithMissingStaticConfig() {
        final String bucketName = "b1";
        final String region = Regions.US_EAST_2.getName();
        final String minioUrl = "http://minio";
        final String username = "username";
        final String password = "password";
        assertThatThrownBy(() -> new S3StorageConfig(Map.of(
            S3StorageConfig.S3_BUCKET_NAME_CONFIG, bucketName,
            S3StorageConfig.S3_REGION_CONFIG, region,
            S3StorageConfig.S3_ENDPOINT_URL_CONFIG, minioUrl,
            S3StorageConfig.AWS_ACCESS_KEY_ID_CONFIG, username)))
            .isInstanceOf(ConfigException.class)
            .hasMessage("aws.access.key.id and aws.secret.access.key must be defined together");
        assertThatThrownBy(() -> new S3StorageConfig(Map.of(
            S3StorageConfig.S3_BUCKET_NAME_CONFIG, bucketName,
            S3StorageConfig.S3_REGION_CONFIG, region,
            S3StorageConfig.S3_ENDPOINT_URL_CONFIG, minioUrl,
            S3StorageConfig.AWS_SECRET_ACCESS_KEY_CONFIG, password)))
            .isInstanceOf(ConfigException.class)
            .hasMessage("aws.access.key.id and aws.secret.access.key must be defined together");
    }

    //   - With conflict between static and custom
    @Test
    void configWithConflictBetweenCustomProviderAndStaticCredentials() {
        final String bucketName = "b1";
        final String region = Regions.US_EAST_2.getName();
        final String minioUrl = "http://minio";
        final String customConfigProvider = EnvironmentVariableCredentialsProvider.class.getName();
        final String username = "username";
        final String password = "password";
        final Map<String, Object> configs = Map.of(
            S3StorageConfig.S3_BUCKET_NAME_CONFIG, bucketName,
            S3StorageConfig.S3_REGION_CONFIG, region,
            S3StorageConfig.S3_ENDPOINT_URL_CONFIG, minioUrl,
            S3StorageConfig.AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG, customConfigProvider,
            S3StorageConfig.AWS_ACCESS_KEY_ID_CONFIG, username,
            S3StorageConfig.AWS_SECRET_ACCESS_KEY_CONFIG, password);
        assertThatThrownBy(() -> new S3StorageConfig(configs))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Either  static credential pair aws.access.key.id and aws.secret.access.key "
                + "must be set together, "
                + "or a custom provider class aws.credentials.provider.class. "
                + "If both are null, default S3 credentials provider is used.");
    }

    // - Failing configs scenarios
    @Test
    void shouldRequireS3BucketName() {
        assertThatThrownBy(() -> new S3StorageConfig(Map.of()))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Missing required configuration \"s3.bucket.name\" which has no default value.");
    }

    // - S3 client creation
    @Test
    void shouldCreateClientWithMinimalConfig() {
        final String bucketName = "b1";
        final Map<String, Object> configs = Map.of(S3StorageConfig.S3_BUCKET_NAME_CONFIG, bucketName);
        final S3StorageConfig config = new S3StorageConfig(configs);
        assertThat(config.bucketName()).isEqualTo(bucketName);

        final AmazonS3 s3 = config.s3Client();
        assertThat(s3).isNotNull();
    }
}
