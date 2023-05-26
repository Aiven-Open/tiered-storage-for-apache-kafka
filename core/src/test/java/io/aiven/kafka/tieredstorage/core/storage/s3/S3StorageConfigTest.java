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

import java.net.URL;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import org.junit.jupiter.api.Test;

import static io.aiven.kafka.tieredstorage.core.storage.s3.S3StorageConfig.S3_REGION_DEFAULT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class S3StorageConfigTest {

    // Test scenarios
    // - Minimal/Full configs
    @Test
    void minimalConfig() {
        final String bucketName = "b1";
        final Map<String, Object> configs = Map.of("s3.bucket.name", bucketName);
        final S3StorageConfig config = new S3StorageConfig(configs);
        assertThat(config.bucketName()).isEqualTo(bucketName);

        final AWSCredentialsProvider credentialsProvider = config.credentialsProvider();
        assertThat(credentialsProvider).isNull();
        final AmazonS3 s3 = config.s3Client();
        assertThat(s3.getRegionName()).isEqualTo(S3_REGION_DEFAULT);
        final String expectedHost = "s3." + S3_REGION_DEFAULT + ".amazonaws.com";
        assertThat(s3.getUrl(bucketName, "test")).hasHost(expectedHost);
        assertThat(config.pathStyleAccessEnabled()).isNull();
    }

    // - Credential provider scenarios
    //   - Without provider
    @Test
    void configWithoutProvider() {
        final String bucketName = "b1";
        final String region = Regions.US_EAST_2.getName();
        final String minioUrl = "http://minio";
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", bucketName,
            "s3.region", region,
            "s3.endpoint.url", minioUrl,
            "s3.path.style.access.enabled", true
        );
        final S3StorageConfig config = new S3StorageConfig(configs);
        assertThat(config.bucketName()).isEqualTo(bucketName);
        assertThat(config.getString("s3.region")).isEqualTo(region);
        assertThat(config.getString("s3.endpoint.url")).isEqualTo(minioUrl);

        final AWSCredentialsProvider credentialsProvider = config.credentialsProvider();
        assertThat(credentialsProvider).isNull();
        final AmazonS3 s3 = config.s3Client();
        assertThat(s3.getRegionName()).isEqualTo(region);
        final URL test = s3.getUrl(bucketName, "test");
        assertThat(test).hasHost("minio");
        assertThat(config.pathStyleAccessEnabled()).isTrue();
    }

    //   - With provider
    @Test
    void configWithProvider() {
        final String bucketName = "b1";
        final String region = Regions.US_EAST_2.getName();
        final String minioUrl = "http://minio";
        final String customConfigProvider = EnvironmentVariableCredentialsProvider.class.getName();
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", bucketName,
            "s3.region", region,
            "s3.endpoint.url", minioUrl,
            "s3.path.style.access.enabled", false,
            "aws.credentials.provider.class", customConfigProvider);
        final S3StorageConfig config = new S3StorageConfig(configs);
        assertThat(config.bucketName()).isEqualTo(bucketName);
        assertThat(config.getString("s3.region")).isEqualTo(region);
        assertThat(config.getString("s3.endpoint.url")).isEqualTo(minioUrl);
        assertThat(config.getClass("aws.credentials.provider.class").getName())
            .isEqualTo(customConfigProvider);

        final AWSCredentialsProvider credentialsProvider = config.credentialsProvider();
        assertThat(credentialsProvider).isInstanceOf(EnvironmentVariableCredentialsProvider.class);
        final AmazonS3 s3 = config.s3Client();
        assertThat(s3.getRegionName()).isEqualTo(region);
        assertThat(s3.getUrl(bucketName, "test")).hasHost("minio");
        assertThat(config.pathStyleAccessEnabled()).isFalse();
    }

    //   - With static credentials
    @Test
    void configWithStaticCredentials() {
        final String bucketName = "b1";
        final String region = Regions.US_EAST_2.getName();
        final String minioUrl = "http://minio";
        final String username = "username";
        final String password = "password";
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", bucketName,
            "s3.region", region,
            "s3.endpoint.url", minioUrl,
            "aws.access.key.id", username,
            "aws.secret.access.key", password);
        final S3StorageConfig config = new S3StorageConfig(configs);
        assertThat(config.bucketName()).isEqualTo(bucketName);
        assertThat(config.getString("s3.region")).isEqualTo(region);
        assertThat(config.getString("s3.endpoint.url")).isEqualTo(minioUrl);
        assertThat(config.getPassword("aws.access.key.id").value()).isEqualTo(username);
        assertThat(config.getPassword("aws.secret.access.key").value()).isEqualTo(password);

        final AWSCredentialsProvider credentialsProvider = config.credentialsProvider();
        assertThat(credentialsProvider).isInstanceOf(AWSStaticCredentialsProvider.class);
        final AWSStaticCredentialsProvider staticCredentialsProvider =
            (AWSStaticCredentialsProvider) credentialsProvider;
        assertThat(staticCredentialsProvider.getCredentials().getAWSAccessKeyId()).isEqualTo(username);
        assertThat(staticCredentialsProvider.getCredentials().getAWSSecretKey()).isEqualTo(password);
        final AmazonS3 s3 = config.s3Client();
        assertThat(s3.getRegionName()).isEqualTo(region);
        assertThat(s3.getUrl(bucketName, "test")).hasHost("minio");
    }

    //   - With missing static credentials
    @Test
    void configWithMissingStaticConfig() {
        final String bucketName = "b1";
        final String region = Regions.US_EAST_2.getName();
        final String minioUrl = "http://minio";
        final String username = "username";
        final String password = "password";
        assertThatThrownBy(() -> new S3StorageConfig(Map.of(
            "s3.bucket.name", bucketName,
            "s3.region", region,
            "s3.endpoint.url", minioUrl,
            "aws.access.key.id", username)))
            .isInstanceOf(ConfigException.class)
            .hasMessage("aws.access.key.id and aws.secret.access.key must be defined together");
        assertThatThrownBy(() -> new S3StorageConfig(Map.of(
            "s3.bucket.name", bucketName,
            "s3.region", region,
            "s3.endpoint.url", minioUrl,
            "aws.secret.access.key", password)))
            .isInstanceOf(ConfigException.class)
            .hasMessage("aws.access.key.id and aws.secret.access.key must be defined together");
    }

    //   - With empty static credentials
    @Test
    void configWithEmptyStaticConfig() {
        final String bucketName = "b1";
        assertThatThrownBy(() -> new S3StorageConfig(Map.of(
            "s3.bucket.name", bucketName,
            "aws.access.key.id", "")))
            .isInstanceOf(ConfigException.class)
            .hasMessage("aws.access.key.id value must not be empty");
        assertThatThrownBy(() -> new S3StorageConfig(Map.of(
            "s3.bucket.name", bucketName,
            "aws.secret.access.key", "")))
            .isInstanceOf(ConfigException.class)
            .hasMessage("aws.secret.access.key value must not be empty");
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
            "s3.bucket.name", bucketName,
            "s3.region", region,
            "s3.endpoint.url", minioUrl,
            "aws.credentials.provider.class", customConfigProvider,
            "aws.access.key.id", username,
            "aws.secret.access.key", password);
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
        final Map<String, Object> configs = Map.of("s3.bucket.name", bucketName);
        final S3StorageConfig config = new S3StorageConfig(configs);
        assertThat(config.bucketName()).isEqualTo(bucketName);

        final AmazonS3 s3 = config.s3Client();
        assertThat(s3).isNotNull();
    }
}
