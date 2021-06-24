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

import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Regions;

/**
 * A configuration for {@link S3RemoteStorageManager}.
 */
public class S3RemoteStorageManagerConfig extends AbstractConfig {
    private static final String REMOTE_STORAGE_MANAGER_CONFIG_PREFIX = "remote.log.storage.";
    public static final String S3_BUCKET_NAME_CONFIG = REMOTE_STORAGE_MANAGER_CONFIG_PREFIX + "s3.bucket.name";
    private static final String S3_BUCKET_NAME_DOC = "The S3 Bucket.";

    public static final String S3_REGION_CONFIG = REMOTE_STORAGE_MANAGER_CONFIG_PREFIX + "s3.region";
    private static final String S3_REGION_DEFAULT = Regions.DEFAULT_REGION.getName();
    private static final String S3_REGION_DOC = "The AWS region.";

    public static final String S3_CREDENTIALS_PROVIDER_CLASS_CONFIG =
            REMOTE_STORAGE_MANAGER_CONFIG_PREFIX + "s3.credentials.provider.class";
    private static final Class<? extends AWSCredentialsProvider> S3_CREDENTIALS_PROVIDER_CLASS_DEFAULT = null;
    private static final String S3_CREDENTIALS_PROVIDER_CLASS_DOC = "The credentials provider to use for "
            + "authentication to AWS. If not set, AWS SDK uses the default "
            + "com.amazonaws.auth.DefaultAWSCredentialsProviderChain";

    public static final String PUBLIC_KEY = REMOTE_STORAGE_MANAGER_CONFIG_PREFIX + "s3.public_key_pem";
    private static final String PUBLIC_KEY_DOC = "Public key for storage encryption";

    public static final String PRIVATE_KEY = REMOTE_STORAGE_MANAGER_CONFIG_PREFIX + "s3.private_key_pem";
    private static final String PRIVATE_KEY_DOC = "Private key for storage encryption";

    public static final String IO_BUFFER_SIZE = REMOTE_STORAGE_MANAGER_CONFIG_PREFIX + "s3.io.buffer.size";
    private static final int IO_BUFFER_SIZE_DEFAULT = 8_192;
    private static final String IO_BUFFER_SIZE_DOC = "Buffer size for uploading";

    public static final String S3_STORAGE_UPLOAD_PART_SIZE =
            REMOTE_STORAGE_MANAGER_CONFIG_PREFIX + "s3.upload.part.size";
    private static final int S3_STORAGE_UPLOAD_PART_SIZE_DEFAULT = 524288;
    private static final String S3_STORAGE_UPLOAD_PART_SIZE_DOC = "S3 upload part size";

    public static final String MULTIPART_UPLOAD_PART_SIZE =
            REMOTE_STORAGE_MANAGER_CONFIG_PREFIX + "s3.multipart.upload.part.size";
    private static final int MULTIPART_UPLOAD_PART_SIZE_DEFAULT = 8_192;
    private static final String MULTIPART_UPLOAD_PART_SIZE_DOC = "S3 upload part size";

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
            S3_REGION_CONFIG,
            ConfigDef.Type.STRING,
            S3_REGION_DEFAULT,
            new RegionValidator(),
            ConfigDef.Importance.MEDIUM,
            S3_REGION_DOC
        );

        CONFIG.define(
            S3_CREDENTIALS_PROVIDER_CLASS_CONFIG,
            ConfigDef.Type.CLASS,
            S3_CREDENTIALS_PROVIDER_CLASS_DEFAULT,
            new CredentialsProviderValidator(),
            ConfigDef.Importance.LOW,
            S3_CREDENTIALS_PROVIDER_CLASS_DOC
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
    }

    public S3RemoteStorageManagerConfig(final Map<String, ?> props) {
        super(CONFIG, props);
    }

    public String s3BucketName() {
        return getString(S3_BUCKET_NAME_CONFIG);
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

    public AWSCredentialsProvider awsCredentialsProvider() {
        try {
            @SuppressWarnings("unchecked")
            final Class<? extends AWSCredentialsProvider> providerClass = (Class<? extends AWSCredentialsProvider>)
                getClass(S3_CREDENTIALS_PROVIDER_CLASS_CONFIG);
            if (providerClass == null) {
                return null;
            }
            return providerClass.getDeclaredConstructor().newInstance();
        } catch (final ReflectiveOperationException e) {
            throw new KafkaException(e);
        }
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

            if (Stream.of(providerClass.getDeclaredConstructors())
                    .noneMatch(constructor -> constructor.getParameterCount() == 0)) {
                throw new ConfigException(name, value, "Class must have no args constructor");
            }
        }
    }
}
