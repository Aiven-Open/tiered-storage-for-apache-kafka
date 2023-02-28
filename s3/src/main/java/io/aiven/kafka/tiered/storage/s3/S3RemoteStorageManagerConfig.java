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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;

/**
 * A configuration for {@link S3RemoteStorageManager}.
 */
public class S3RemoteStorageManagerConfig extends AbstractConfig {
    private static final String GROUP_AWS = "AWS";

    public static final String S3_BUCKET_NAME_CONFIG = "s3.bucket.name";
    private static final String S3_BUCKET_NAME_DOC = "The S3 Bucket.";

    public static final String S3_PREFIX = "s3.prefix";

    private static final String S3_PREFIX_DOC = "The S3 prefix";

    public static final String S3_REGION_CONFIG = "s3.region";
    private static final String S3_REGION_DEFAULT = Regions.DEFAULT_REGION.getName();
    private static final String S3_REGION_DOC = "The AWS region.";

    public static final String S3_CREDENTIALS_PROVIDER_CLASS_CONFIG = "s3.credentials.provider.class";
    private static final Class<? extends AWSCredentialsProvider> S3_CREDENTIALS_PROVIDER_CLASS_DEFAULT = null;
    private static final String S3_CREDENTIALS_PROVIDER_CLASS_DOC = "The credentials provider to use for "
            + "authentication to AWS. If not set, AWS SDK uses the default "
            + "com.amazonaws.auth.DefaultAWSCredentialsProviderChain";

    public static final String PUBLIC_KEY = "s3.public_key_pem";
    private static final String PUBLIC_KEY_DOC = "Public key for storage encryption";

    public static final String PRIVATE_KEY = "s3.private_key_pem";
    private static final String PRIVATE_KEY_DOC = "Private key for storage encryption";

    public static final String IO_BUFFER_SIZE = "s3.io.buffer.size";
    private static final int IO_BUFFER_SIZE_DEFAULT = 8_192;
    private static final String IO_BUFFER_SIZE_DOC = "Buffer size for uploading";

    public static final String S3_STORAGE_UPLOAD_PART_SIZE = "s3.upload.part.size";
    private static final int S3_STORAGE_UPLOAD_PART_SIZE_DEFAULT = 524288;
    private static final String S3_STORAGE_UPLOAD_PART_SIZE_DOC = "S3 upload part size";

    public static final String MULTIPART_UPLOAD_PART_SIZE = "s3.multipart.upload.part.size";
    private static final int MULTIPART_UPLOAD_PART_SIZE_DEFAULT = 8_192;
    private static final String MULTIPART_UPLOAD_PART_SIZE_DOC = "S3 upload part size";

    public static final String ENCRYPTION_METADATA_CACHE_SIZE = "s3.encryption.metadata.cache.size";
    public static final long ENCRYPTION_METADATA_CACHE_SIZE_DEFAULT = 1000;
    private static final String ENCRYPTION_METADATA_CACHE_SIZE_DOC = "Size of encryption metadata cache";

    public static final String ENCRYPTION_METADATA_CACHE_RETENTION_MS = "s3.encryption.metadata.cache.retention.ms";
    public static final long ENCRYPTION_METADATA_CACHE_RETENTION_MS_DEFAULT = 1_800_000;
    private static final String ENCRYPTION_METADATA_CACHE_RETENTION_MS_DOC =
            "Retention time for encryption metadata cache";

    public static final String AWS_ACCESS_KEY_ID = "s3.client.aws_access_key_id";
    private static final String AWS_ACCESS_KEY_ID_DOC = "AWS Access Key ID";

    public static final String AWS_SECRET_ACCESS_KEY = "s3.client.aws_secret_access_key";
    private static final String AWS_SECRET_ACCESS_KEY_DOC = "AWS Secret Access Key";

    private static final ConfigDef CONFIG;


    static {
        CONFIG = new ConfigDef();

        CONFIG.define(
            S3_BUCKET_NAME_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            new ConfigDef.NonEmptyString(),
            ConfigDef.Importance.HIGH,
            S3_BUCKET_NAME_DOC
        );

        CONFIG.define(
            S3_PREFIX,
            ConfigDef.Type.STRING,
            "",
            ConfigDef.Importance.MEDIUM,
            S3_PREFIX_DOC
        );

        CONFIG.define(
            S3_REGION_CONFIG,
            ConfigDef.Type.STRING,
            S3_REGION_DEFAULT,
            new RegionValidator(),
            ConfigDef.Importance.MEDIUM,
            S3_REGION_DOC
        );

        CONFIG.define(
            PUBLIC_KEY,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            new ConfigDef.NonEmptyString(),
            ConfigDef.Importance.HIGH,
            PUBLIC_KEY_DOC
        );

        CONFIG.define(
            PRIVATE_KEY,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            new ConfigDef.NonEmptyString(),
            ConfigDef.Importance.HIGH,
            PRIVATE_KEY_DOC
        );

        CONFIG.define(
            IO_BUFFER_SIZE,
            ConfigDef.Type.INT,
            IO_BUFFER_SIZE_DEFAULT,
            ConfigDef.Range.between(1024, Integer.MAX_VALUE),
            ConfigDef.Importance.HIGH,
            IO_BUFFER_SIZE_DOC
        );

        CONFIG.define(
            S3_STORAGE_UPLOAD_PART_SIZE,
            ConfigDef.Type.INT,
            S3_STORAGE_UPLOAD_PART_SIZE_DEFAULT,
            ConfigDef.Range.between(1024, Integer.MAX_VALUE),
            ConfigDef.Importance.HIGH,
            S3_STORAGE_UPLOAD_PART_SIZE_DOC
        );

        CONFIG.define(
            MULTIPART_UPLOAD_PART_SIZE,
            ConfigDef.Type.INT,
            MULTIPART_UPLOAD_PART_SIZE_DEFAULT,
            ConfigDef.Range.between(1024, Integer.MAX_VALUE),
            ConfigDef.Importance.HIGH,
            MULTIPART_UPLOAD_PART_SIZE_DOC
        );

        CONFIG.define(
                ENCRYPTION_METADATA_CACHE_SIZE,
                ConfigDef.Type.LONG,
                ENCRYPTION_METADATA_CACHE_SIZE_DEFAULT,
                ConfigDef.Range.between(0, Long.MAX_VALUE),
                ConfigDef.Importance.LOW,
                ENCRYPTION_METADATA_CACHE_SIZE_DOC
        );

        CONFIG.define(
                ENCRYPTION_METADATA_CACHE_RETENTION_MS,
                ConfigDef.Type.LONG,
                ENCRYPTION_METADATA_CACHE_RETENTION_MS_DEFAULT,
                ConfigDef.Range.between(0, Long.MAX_VALUE),
                ConfigDef.Importance.LOW,
                ENCRYPTION_METADATA_CACHE_RETENTION_MS_DOC
        );

        int awsGroupCounter = 0;
        CONFIG.define(
            S3_CREDENTIALS_PROVIDER_CLASS_CONFIG,
            ConfigDef.Type.CLASS,
            S3_CREDENTIALS_PROVIDER_CLASS_DEFAULT,
            new CredentialsProviderValidator(),
            ConfigDef.Importance.LOW,
            S3_CREDENTIALS_PROVIDER_CLASS_DOC,
            GROUP_AWS,
            awsGroupCounter++,
            ConfigDef.Width.NONE,
            S3_CREDENTIALS_PROVIDER_CLASS_CONFIG,
            new ConfigDef.Recommender() {
                @Override
                public List<Object> validValues(final String s, final Map<String, Object> map) {
                    return Collections.emptyList();
                }

                @Override
                public boolean visible(final String s, final Map<String, Object> map) {
                    final String credentialsProvider = (String) map.get("S3_CREDENTIALS_PROVIDER_CLASS_CONFIG");
                    try {
                        if (Class.forName(credentialsProvider).isAssignableFrom(AWSStaticCredentialsProvider.class)) {
                            return s.equals(AWS_ACCESS_KEY_ID) || s.equals(AWS_SECRET_ACCESS_KEY);
                        }
                    } catch (final ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                    return false;
                }
            }
        );

        CONFIG.define(
                AWS_ACCESS_KEY_ID,
                ConfigDef.Type.PASSWORD,
                null,
                new NonEmptyPassword(),
                ConfigDef.Importance.MEDIUM,
                AWS_ACCESS_KEY_ID_DOC,
                GROUP_AWS,
                awsGroupCounter++,
                ConfigDef.Width.NONE,
                AWS_ACCESS_KEY_ID
        );

        CONFIG.define(
                AWS_SECRET_ACCESS_KEY,
                ConfigDef.Type.PASSWORD,
                null,
                new NonEmptyPassword(),
                ConfigDef.Importance.MEDIUM,
                AWS_SECRET_ACCESS_KEY_DOC,
                GROUP_AWS,
                awsGroupCounter++,
                ConfigDef.Width.NONE,
                AWS_SECRET_ACCESS_KEY
        );
    }

    public S3RemoteStorageManagerConfig(final Map<String, ?> props) {
        super(CONFIG, props);
    }

    public String s3BucketName() {
        return getString(S3_BUCKET_NAME_CONFIG);
    }

    public String s3Prefix() {
        return getString(S3_PREFIX);
    }

    public Regions s3Region() {
        final String regionStr = getString(S3_REGION_CONFIG);
        return Regions.fromName(regionStr);
    }

    public String publicKey() {
        return getString(PUBLIC_KEY);
    }

    public String privateKey() {
        return getString(PRIVATE_KEY);
    }

    public int ioBufferSize() {
        return getInt(IO_BUFFER_SIZE);
    }

    public int s3StorageUploadPartSize() {
        return getInt(S3_STORAGE_UPLOAD_PART_SIZE);
    }

    public int multiPartUploadPartSize() {
        return getInt(MULTIPART_UPLOAD_PART_SIZE);
    }

    public long encryptionMetadataCacheSize() {
        return getLong(ENCRYPTION_METADATA_CACHE_SIZE);
    }

    public long encryptionMetadataCacheRetentionMs() {
        return getLong(ENCRYPTION_METADATA_CACHE_RETENTION_MS);
    }

    public AWSCredentialsProvider awsCredentialsProvider() {
        try {
            @SuppressWarnings("unchecked")
            final Class<? extends AWSCredentialsProvider> providerClass = (Class<? extends AWSCredentialsProvider>)
                getClass(S3_CREDENTIALS_PROVIDER_CLASS_CONFIG);
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
        return new BasicAWSCredentials(getPassword(AWS_ACCESS_KEY_ID).value(),
                getPassword(AWS_SECRET_ACCESS_KEY).value());
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
}
