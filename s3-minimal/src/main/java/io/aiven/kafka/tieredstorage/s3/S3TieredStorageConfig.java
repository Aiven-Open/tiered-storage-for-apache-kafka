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

package io.aiven.kafka.tieredstorage.s3;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class S3TieredStorageConfig extends AbstractConfig {

    public static final String S3_BUCKET_NAME_CONFIG = "s3.bucket.name";
    private static final String S3_BUCKET_NAME_DOC = "The S3 Bucket.";

    public static final String S3_FETCH_MAX_BYTES_CONFIG = "s3.fetch.max.bytes";
    public static final int S3_FETCH_MAX_BYTES_DEFAULT = 10_000_000; // 10MB
    private static final String S3_FETCH_MAX_BYTES_DOC = "Max bytes to fetch when no boundaries are provided.";

    public static final String S3_OBJECT_PREFIX_CONFIG = "s3.object.prefix";

    private static final String S3_OBJECT_PREFIX_DOC = "The S3 prefix";
    public static final String S3_ENDPOINT_URL_CONFIG = "s3.endpoint.url";

    private static final String S3_ENDPOINT_URL_DOC = "Custom S3 Endpoint URL";

    private static final String GROUP_AWS = "AWS";
    public static final String AWS_REGION_CONFIG = "s3.aws.region";
    private static final String AWS_REGION_DEFAULT = Regions.DEFAULT_REGION.getName();
    private static final String AWS_REGION_DOC = "The AWS region.";
    public static final String AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG = "s3.aws.credentials.provider.class";
    private static final Class<? extends AWSCredentialsProvider> AWS_CREDENTIALS_PROVIDER_CLASS_DEFAULT =
        DefaultAWSCredentialsProviderChain.class;
    private static final String AWS_CREDENTIALS_PROVIDER_CLASS_DOC = "The credentials provider to use for "
        + "authentication to AWS. If not set, AWS SDK uses the default "
        + "com.amazonaws.auth.DefaultAWSCredentialsProviderChain";
    public static final String AWS_ACCESS_KEY_ID_CONFIG = "s3.aws.access.key.id";
    private static final String AWS_ACCESS_KEY_ID_DOC = "AWS Access Key ID";

    public static final String AWS_SECRET_ACCESS_KEY_CONFIG = "s3.aws.secret.access.key";
    private static final String AWS_SECRET_ACCESS_KEY_DOC = "AWS Secret Access Key";

    private static final ConfigDef CONFIG = new ConfigDef();

    static {
        CONFIG
            .define(
                S3_BUCKET_NAME_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                S3_BUCKET_NAME_DOC)
            .define(
                S3_FETCH_MAX_BYTES_CONFIG,
                ConfigDef.Type.INT,
                S3_FETCH_MAX_BYTES_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                S3_FETCH_MAX_BYTES_DOC
            )
            .define(
                S3_OBJECT_PREFIX_CONFIG,
                ConfigDef.Type.STRING,
                "",
                ConfigDef.Importance.MEDIUM,
                S3_OBJECT_PREFIX_DOC)
            .define(
                S3_ENDPOINT_URL_CONFIG,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.LOW,
                S3_ENDPOINT_URL_DOC);

        // AWS specific configs
        int awsGroupCounter = 0;
        CONFIG
            .define(
                AWS_REGION_CONFIG,
                ConfigDef.Type.STRING,
                AWS_REGION_DEFAULT,
                new RegionValidator(),
                ConfigDef.Importance.MEDIUM,
                AWS_REGION_DOC,
                GROUP_AWS,
                awsGroupCounter++,
                ConfigDef.Width.NONE,
                AWS_REGION_CONFIG
            )
            .define(
                AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG,
                ConfigDef.Type.CLASS,
                AWS_CREDENTIALS_PROVIDER_CLASS_DEFAULT,
                new CredentialsProviderValidator(),
                ConfigDef.Importance.LOW,
                AWS_CREDENTIALS_PROVIDER_CLASS_DOC,
                GROUP_AWS,
                awsGroupCounter++,
                ConfigDef.Width.NONE,
                AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG,
                new ConfigDef.Recommender() {
                    @Override
                    public List<Object> validValues(final String s, final Map<String, Object> map) {
                        return Collections.emptyList();
                    }

                    @Override
                    public boolean visible(final String s, final Map<String, Object> map) {
                        final String credentialsProvider = (String) map.get("S3_CREDENTIALS_PROVIDER_CLASS_CONFIG");
                        try {
                            if (Class.forName(credentialsProvider)
                                .isAssignableFrom(AWSStaticCredentialsProvider.class)) {
                                return s.equals(AWS_ACCESS_KEY_ID_CONFIG) || s.equals(AWS_SECRET_ACCESS_KEY_CONFIG);
                            }
                        } catch (final ClassNotFoundException e) {
                            e.printStackTrace();
                        }
                        return false;
                    }
                })
            .define(
                AWS_ACCESS_KEY_ID_CONFIG,
                ConfigDef.Type.PASSWORD,
                null,
                new NonEmptyPassword(),
                ConfigDef.Importance.MEDIUM,
                AWS_ACCESS_KEY_ID_DOC,
                GROUP_AWS,
                awsGroupCounter++,
                ConfigDef.Width.NONE,
                AWS_ACCESS_KEY_ID_CONFIG)
            .define(
                AWS_SECRET_ACCESS_KEY_CONFIG,
                ConfigDef.Type.PASSWORD,
                null,
                new NonEmptyPassword(),
                ConfigDef.Importance.MEDIUM,
                AWS_SECRET_ACCESS_KEY_DOC,
                GROUP_AWS,
                awsGroupCounter++,
                ConfigDef.Width.NONE,
                AWS_SECRET_ACCESS_KEY_CONFIG
            );
    }

    public S3TieredStorageConfig(final Map<String, ?> props) {
        super(CONFIG, props);
    }

    AmazonS3 s3Client() {
        return s3Client(awsCredentialsProvider());
    }

    AmazonS3 s3Client(final AWSCredentialsProvider credentialsProvider) {
        AmazonS3ClientBuilder s3ClientBuilder = AmazonS3ClientBuilder.standard();
        final String s3ServiceEndpoint = getString(S3_ENDPOINT_URL_CONFIG);
        final String region = getString(AWS_REGION_CONFIG);
        if (s3ServiceEndpoint == null) {
            s3ClientBuilder = s3ClientBuilder.withRegion(region);
        } else {
            final AwsClientBuilder.EndpointConfiguration endpointConfiguration =
                new AwsClientBuilder.EndpointConfiguration(s3ServiceEndpoint, region);
            s3ClientBuilder = s3ClientBuilder.withEndpointConfiguration(endpointConfiguration)
                .withPathStyleAccessEnabled(true);
        }
        s3ClientBuilder.setCredentials(credentialsProvider);
        return s3ClientBuilder.build();
    }

    public AWSCredentialsProvider awsCredentialsProvider() {
        try {
            @SuppressWarnings("unchecked") final Class<? extends AWSCredentialsProvider> providerClass =
                (Class<? extends AWSCredentialsProvider>) getClass(AWS_CREDENTIALS_PROVIDER_CLASS_CONFIG);
            if (providerClass == null) {
                return null;
            }
            if (providerClass.isAssignableFrom(AWSStaticCredentialsProvider.class)) {
                return new AWSStaticCredentialsProvider(awsCredentials());
            }
            return providerClass.getDeclaredConstructor().newInstance();
        } catch (final ReflectiveOperationException e) {
            throw new KafkaException(e);
        }
    }

    public AWSCredentials awsCredentials() {
        return new BasicAWSCredentials(getPassword(AWS_ACCESS_KEY_ID_CONFIG).value(),
            getPassword(AWS_SECRET_ACCESS_KEY_CONFIG).value());
    }

    public String bucketName() {
        return getString(S3_BUCKET_NAME_CONFIG);
    }

    public String objectPrefix() {
        return getString(S3_OBJECT_PREFIX_CONFIG);
    }

    public int fetchMaxBytes() {
        return getInt(S3_FETCH_MAX_BYTES_CONFIG);
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
                throw new ConfigException(name, pwd, "Password must be non-empty");
            }

        }

    }
}
