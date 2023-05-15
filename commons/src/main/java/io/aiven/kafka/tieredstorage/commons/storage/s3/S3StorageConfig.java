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
import java.util.Objects;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class S3StorageConfig extends AbstractConfig {

    public static final String S3_BUCKET_NAME_CONFIG = "s3.bucket.name";
    private static final String S3_BUCKET_NAME_DOC = "S3 bucket to store log segments";
    public static final String S3_ENDPOINT_URL_CONFIG = "s3.endpoint.url";
    private static final String S3_ENDPOINT_URL_DOC = "Custom S3 endpoint URL. "
        + "To be used with custom S3-compatible backends (e.g. minio).";
    public static final String S3_REGION_CONFIG = "s3.region";
    static final String S3_REGION_DEFAULT = Regions.DEFAULT_REGION.getName();
    private static final String S3_REGION_DOC = "AWS region where S3 bucket is placed";

    public static final String AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG = "aws.credentials.provider.class";
    private static final String AWS_CREDENTIALS_PROVIDER_CLASS_DOC = "AWS credentials provider. "
        + "If not set, AWS SDK uses the default "
        + "com.amazonaws.services.s3.S3CredentialsProviderChain";
    public static final String AWS_ACCESS_KEY_ID_CONFIG = "aws.access.key.id";
    private static final String AWS_ACCESS_KEY_ID_DOC = "AWS access key ID. "
        + "To be used when static credentials are provided.";
    public static final String AWS_SECRET_ACCESS_KEY_CONFIG = "aws.secret.access.key";
    private static final String AWS_SECRET_ACCESS_KEY_DOC = "AWS secret access key. "
        + "To be used when static credentials are provided.";

    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef()
            .define(
                S3_BUCKET_NAME_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                S3_BUCKET_NAME_DOC)
            .define(
                S3_ENDPOINT_URL_CONFIG,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.LOW,
                S3_ENDPOINT_URL_DOC)
            .define(
                S3_REGION_CONFIG,
                ConfigDef.Type.STRING,
                S3_REGION_DEFAULT,
                new RegionValidator(),
                ConfigDef.Importance.MEDIUM,
                S3_REGION_DOC)
            .define(
                AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG,
                ConfigDef.Type.CLASS,
                null,
                new CredentialsProviderValidator(),
                ConfigDef.Importance.LOW,
                AWS_CREDENTIALS_PROVIDER_CLASS_DOC)
            .define(
                AWS_ACCESS_KEY_ID_CONFIG,
                ConfigDef.Type.PASSWORD,
                null,
                new NonEmptyPassword(),
                ConfigDef.Importance.MEDIUM,
                AWS_ACCESS_KEY_ID_DOC)
            .define(
                AWS_SECRET_ACCESS_KEY_CONFIG,
                ConfigDef.Type.PASSWORD,
                null,
                new NonEmptyPassword(),
                ConfigDef.Importance.MEDIUM,
                AWS_SECRET_ACCESS_KEY_DOC);
    }

    public S3StorageConfig(final Map<String, Object> props) {
        super(CONFIG, props);
        validate();
    }

    private void validate() {
        if (Objects.nonNull(getPassword(AWS_ACCESS_KEY_ID_CONFIG))
            ^ Objects.nonNull(getPassword(AWS_SECRET_ACCESS_KEY_CONFIG))) {
            throw new ConfigException(AWS_ACCESS_KEY_ID_CONFIG
                + " and "
                + AWS_SECRET_ACCESS_KEY_CONFIG
                + " must be defined together");
        }
        if (Objects.nonNull(getClass(AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG))
            && Objects.nonNull(getPassword(AWS_ACCESS_KEY_ID_CONFIG))) {
            throw new ConfigException("Either "
                + " static credential pair "
                + AWS_ACCESS_KEY_ID_CONFIG + " and " + AWS_SECRET_ACCESS_KEY_CONFIG
                + " must be set together, or a custom provider class "
                + AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG
                + ". If both are null, default S3 credentials provider is used.");
        }
    }

    AmazonS3 s3Client() {
        final AmazonS3ClientBuilder s3ClientBuilder = AmazonS3ClientBuilder.standard();
        final String s3ServiceEndpoint = getString(S3_ENDPOINT_URL_CONFIG);
        final String region = getString(S3_REGION_CONFIG);
        if (Objects.isNull(s3ServiceEndpoint)) {
            s3ClientBuilder.withRegion(region);
        } else {
            final AwsClientBuilder.EndpointConfiguration endpointConfiguration =
                new AwsClientBuilder.EndpointConfiguration(s3ServiceEndpoint, region);
            s3ClientBuilder.withEndpointConfiguration(endpointConfiguration);
        }
        final AWSCredentialsProvider credentialsProvider = credentialsProvider();
        if (Objects.nonNull(credentialsProvider)) {
            s3ClientBuilder.setCredentials(credentialsProvider);
        }
        return s3ClientBuilder.build();
    }

    AWSCredentialsProvider credentialsProvider() {
        @SuppressWarnings("unchecked") final Class<? extends AWSCredentialsProvider> providerClass =
            (Class<? extends AWSCredentialsProvider>) getClass(AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG);
        if (Objects.isNull(providerClass)) {
            final boolean areCredentialsProvided =
                Objects.nonNull(getPassword(AWS_ACCESS_KEY_ID_CONFIG))
                    && Objects.nonNull(getPassword(AWS_SECRET_ACCESS_KEY_CONFIG));
            if (areCredentialsProvided) {
                final AWSCredentials staticCredentials = new BasicAWSCredentials(
                    getPassword(AWS_ACCESS_KEY_ID_CONFIG).value(),
                    getPassword(AWS_SECRET_ACCESS_KEY_CONFIG).value()
                );
                return new AWSStaticCredentialsProvider(staticCredentials);
            } else {
                return null; // to use S3 default provider chain. no public constructor
            }
        } else {
            return getConfiguredInstance(AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG, AWSCredentialsProvider.class);
        }
    }

    public String bucketName() {
        return getString(S3_BUCKET_NAME_CONFIG);
    }


    private static class RegionValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            final String regionStr = (String) value;
            try {
                Regions.fromName(regionStr);
            } catch (final IllegalArgumentException e) {
                throw new ConfigException(name, value);
            }
        }
    }

    private static class CredentialsProviderValidator implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            if (value == null) {
                return;
            }

            final Class<?> providerClass = (Class<?>) value;
            if (!AWSCredentialsProvider.class.isAssignableFrom(providerClass)) {
                throw new ConfigException(name, value, "Class must extend " + AWSCredentialsProvider.class);
            }
        }
    }

    private static class NonEmptyPassword implements ConfigDef.Validator {
        @Override
        public void ensureValid(final String name, final Object value) {
            if (Objects.isNull(value)) {
                return;
            }
            final var pwd = (Password) value;
            if (pwd.value() == null || pwd.value().isBlank()) {
                throw new ConfigException(name + " value must not be empty");
            }
        }
    }
}
