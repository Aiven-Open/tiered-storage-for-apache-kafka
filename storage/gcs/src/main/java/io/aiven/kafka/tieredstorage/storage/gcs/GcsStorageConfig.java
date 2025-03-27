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

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import io.aiven.kafka.tieredstorage.config.validators.NonEmptyPassword;
import io.aiven.kafka.tieredstorage.config.validators.ValidUrl;
import io.aiven.kafka.tieredstorage.storage.proxy.ProxyConfig;

import com.google.auth.Credentials;

public class GcsStorageConfig extends AbstractConfig {
    static final String GCS_BUCKET_NAME_CONFIG = "gcs.bucket.name";
    private static final String GCS_BUCKET_NAME_DOC = "GCS bucket to store log segments";

    static final String GCS_ENDPOINT_URL_CONFIG = "gcs.endpoint.url";
    private static final String GCS_ENDPOINT_URL_DOC = "Custom GCS endpoint URL. "
        + "To be used with custom GCS-compatible backends.";

    private static final String GCS_RESUMABLE_UPLOAD_CHUNK_SIZE_CONFIG = "gcs.resumable.upload.chunk.size";
    private static final String GCS_RESUMABLE_UPLOAD_CHUNK_SIZE_DOC = "The chunk size for resumable upload. "
        + "Must be a multiple of 256 KiB (256 x 1024 bytes). "
        + "Larger chunk sizes typically make uploads faster, but requires bigger memory buffers. "
        + "The recommended minimum for GCS is 8 MiB. The SDK default is 15 MiB, "
        + "`see <https://cloud.google.com/storage/docs/resumable-uploads#java>`_. "
        + "The smaller the chunk size, the more calls to GCS are needed to upload a file; increasing costs. "
        + "The higher the chunk size, the more memory is needed to buffer the chunk.";
    static final int GCS_RESUMABLE_UPLOAD_CHUNK_SIZE_DEFAULT = 25 * 1024 * 1024; // 25MiB


    static final String GCP_CREDENTIALS_JSON_CONFIG = "gcs.credentials.json";
    static final String GCP_CREDENTIALS_PATH_CONFIG = "gcs.credentials.path";
    static final String GCP_CREDENTIALS_DEFAULT_CONFIG = "gcs.credentials.default";

    private static final String GCP_CREDENTIALS_JSON_DOC = "GCP credentials as a JSON string. "
        + "Cannot be set together with \"" + GCP_CREDENTIALS_PATH_CONFIG + "\" "
        + "or \"" + GCP_CREDENTIALS_DEFAULT_CONFIG + "\"";
    private static final String GCP_CREDENTIALS_PATH_DOC = "The path to a GCP credentials file. "
        + "Cannot be set together with \"" + GCP_CREDENTIALS_JSON_CONFIG + "\" "
        + "or \"" + GCP_CREDENTIALS_DEFAULT_CONFIG + "\"";
    private static final String GCP_CREDENTIALS_DEFAULT_DOC = "Use the default GCP credentials. "
        + "Cannot be set together with \"" + GCP_CREDENTIALS_JSON_CONFIG + "\" "
        + "or \"" + GCP_CREDENTIALS_PATH_CONFIG + "\"";

    public static ConfigDef configDef() {
        return new ConfigDef()
            .define(
                GCS_BUCKET_NAME_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                GCS_BUCKET_NAME_DOC)
            .define(
                GCS_ENDPOINT_URL_CONFIG,
                ConfigDef.Type.STRING,
                null,
                new ValidUrl(),
                ConfigDef.Importance.LOW,
                GCS_ENDPOINT_URL_DOC)
            .define(
                GCS_RESUMABLE_UPLOAD_CHUNK_SIZE_CONFIG,
                ConfigDef.Type.INT,
                GCS_RESUMABLE_UPLOAD_CHUNK_SIZE_DEFAULT,
                new ResumableUploadChunkSizeValidator(),
                ConfigDef.Importance.MEDIUM,
                GCS_RESUMABLE_UPLOAD_CHUNK_SIZE_DOC)
            .define(
                GCP_CREDENTIALS_JSON_CONFIG,
                ConfigDef.Type.PASSWORD,
                null,
                new NonEmptyPassword(),
                ConfigDef.Importance.MEDIUM,
                GCP_CREDENTIALS_JSON_DOC)
            .define(
                GCP_CREDENTIALS_PATH_CONFIG,
                ConfigDef.Type.STRING,
                null,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.MEDIUM,
                GCP_CREDENTIALS_PATH_DOC)
            .define(
                GCP_CREDENTIALS_DEFAULT_CONFIG,
                ConfigDef.Type.BOOLEAN,
                null,
                ConfigDef.Importance.MEDIUM,
                GCP_CREDENTIALS_DEFAULT_DOC);
    }

    private ProxyConfig proxyConfig = null;

    public GcsStorageConfig(final Map<String, ?> props) {
        super(configDef(), props);
        validate();

        final Map<String, ?> proxyProps = this.originalsWithPrefix(ProxyConfig.PROXY_PREFIX, true);
        if (!proxyProps.isEmpty()) {
            this.proxyConfig = new ProxyConfig(proxyProps);
        }
    }

    ProxyConfig proxyConfig() {
        return proxyConfig;
    }

    private void validate() {
        final String credentialsJson = getPassword(GCP_CREDENTIALS_JSON_CONFIG) == null
            ? null
            : getPassword(GCP_CREDENTIALS_JSON_CONFIG).value();

        try {
            CredentialsBuilder.validate(
                getBoolean(GCP_CREDENTIALS_DEFAULT_CONFIG),
                credentialsJson,
                getString(GCP_CREDENTIALS_PATH_CONFIG)
            );
        } catch (final IllegalArgumentException e) {
            final String message = e.getMessage()
                .replace("credentialsPath", GCP_CREDENTIALS_PATH_CONFIG)
                .replace("credentialsJson", GCP_CREDENTIALS_JSON_CONFIG)
                .replace("defaultCredentials", GCP_CREDENTIALS_DEFAULT_CONFIG);
            throw new ConfigException(message);
        }
    }

    String bucketName() {
        return getString(GCS_BUCKET_NAME_CONFIG);
    }

    String endpointUrl() {
        return getString(GCS_ENDPOINT_URL_CONFIG);
    }

    Integer resumableUploadChunkSize() {
        return getInt(GCS_RESUMABLE_UPLOAD_CHUNK_SIZE_CONFIG);
    }

    Credentials credentials() {
        final Boolean defaultCredentials = getBoolean(GCP_CREDENTIALS_DEFAULT_CONFIG);
        final Password credentialsJsonPwd = getPassword(GCP_CREDENTIALS_JSON_CONFIG);
        final String credentialsJson = credentialsJsonPwd == null ? null : credentialsJsonPwd.value();
        final String credentialsPath = getString(GCP_CREDENTIALS_PATH_CONFIG);
        try {
            return CredentialsBuilder.build(defaultCredentials, credentialsJson, credentialsPath);
        } catch (final IOException e) {
            throw new ConfigException("Failed to create GCS credentials: " + e.getMessage());
        }
    }

    private static class ResumableUploadChunkSizeValidator implements ConfigDef.Validator {
        private static final long MULTIPLIER = 256L * 1024;

        @Override
        public void ensureValid(final String name, final Object value) {
            if (value == null) {
                return;
            }
            final int intValue = (int) value;
            if (intValue < MULTIPLIER) {
                throw new ConfigException(name, value, "Value must be at least 256 KiB (" + MULTIPLIER + " B)");
            }
            if (intValue % MULTIPLIER != 0) {
                throw new ConfigException(name, value, "Value must be a multiple of 256 KiB (" + MULTIPLIER + " B)");
            }
        }

        @Override
        public String toString() {
            return "[256 KiB...] values multiple of " + MULTIPLIER + " bytes";
        }
    }
}
