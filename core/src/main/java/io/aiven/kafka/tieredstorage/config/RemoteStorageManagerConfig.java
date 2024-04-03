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

package io.aiven.kafka.tieredstorage.config;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Utils;

import io.aiven.kafka.tieredstorage.fetch.manifest.MemorySegmentManifestCache;
import io.aiven.kafka.tieredstorage.metadata.SegmentCustomMetadataField;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

public class RemoteStorageManagerConfig extends AbstractConfig {
    private static final String STORAGE_PREFIX = "storage.";
    private static final String FETCH_INDEXES_CACHE_PREFIX = "fetch.indexes.cache.";

    private static final String STORAGE_BACKEND_CLASS_CONFIG = STORAGE_PREFIX + "backend.class";
    private static final String STORAGE_BACKEND_CLASS_DOC = "The storage backend implementation class";

    private static final String OBJECT_KEY_PREFIX_CONFIG = "key.prefix";
    private static final String OBJECT_KEY_PREFIX_DOC = "The object storage path prefix";

    private static final String OBJECT_KEY_PREFIX_MASK_CONFIG = "key.prefix.mask";
    private static final String OBJECT_KEY_PREFIX_MASK_DOC = "Whether to mask path prefix in logs";

    private static final String SEGMENT_MANIFEST_CACHE_PREFIX = "segment.manifest.cache.";

    private static final String CHUNK_SIZE_CONFIG = "chunk.size";
    private static final String CHUNK_SIZE_DOC = "The chunk size of log files";

    private static final String COMPRESSION_ENABLED_CONFIG = "compression.enabled";
    private static final String COMPRESSION_ENABLED_DOC = "Whether to enable compression";

    private static final String COMPRESSION_HEURISTIC_ENABLED_CONFIG = "compression.heuristic.enabled";
    private static final String COMPRESSION_HEURISTIC_ENABLED_DOC = "Whether to use compression heuristics "
        + "when compression is enabled";

    private static final String ENCRYPTION_CONFIG = "encryption.enabled";
    private static final String ENCRYPTION_DOC = "Whether to enable encryption";
    // TODO add possibility to pass keys as strings


    public static final String METRICS_NUM_SAMPLES_CONFIG = CommonClientConfigs.METRICS_NUM_SAMPLES_CONFIG;
    private static final String METRICS_NUM_SAMPLES_DOC = CommonClientConfigs.METRICS_NUM_SAMPLES_DOC;

    public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_CONFIG;
    private static final String METRICS_SAMPLE_WINDOW_MS_DOC = CommonClientConfigs.METRICS_SAMPLE_WINDOW_MS_DOC;

    public static final String METRICS_RECORDING_LEVEL_CONFIG = CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG;
    private static final String METRICS_RECORDING_LEVEL_DOC = CommonClientConfigs.METRICS_RECORDING_LEVEL_DOC;

    private static final String CUSTOM_METADATA_FIELDS_INCLUDE_CONFIG = "custom.metadata.fields.include";
    private static final String CUSTOM_METADATA_FIELDS_INCLUDE_DOC = "Custom Metadata to be stored along "
        + "Remote Log Segment metadata on Remote Log Metadata Manager back-end. "
        + "Allowed values: " + Arrays.toString(SegmentCustomMetadataField.names());

    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef();

        // TODO checkers

        CONFIG.define(
            STORAGE_BACKEND_CLASS_CONFIG,
            ConfigDef.Type.CLASS,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Importance.HIGH,
            STORAGE_BACKEND_CLASS_DOC
        );

        CONFIG.define(
            OBJECT_KEY_PREFIX_CONFIG,
            ConfigDef.Type.STRING,
            "",
            new ConfigDef.NonNullValidator(),
            ConfigDef.Importance.HIGH,
            OBJECT_KEY_PREFIX_DOC
        );

        CONFIG.define(
            OBJECT_KEY_PREFIX_MASK_CONFIG,
            ConfigDef.Type.BOOLEAN,
            false,
            ConfigDef.Importance.LOW,
            OBJECT_KEY_PREFIX_MASK_DOC
        );

        CONFIG.define(
            CHUNK_SIZE_CONFIG,
            ConfigDef.Type.INT,
            ConfigDef.NO_DEFAULT_VALUE,
            //TODO figure out sensible limit because Integer.Max_VALUE leads to overflow during encryption
            ConfigDef.Range.between(1, Integer.MAX_VALUE / 2),
            ConfigDef.Importance.HIGH,
            CHUNK_SIZE_DOC
        );

        CONFIG.define(
            COMPRESSION_ENABLED_CONFIG,
            ConfigDef.Type.BOOLEAN,
            false,
            ConfigDef.Importance.HIGH,
            COMPRESSION_ENABLED_DOC
        );
        CONFIG.define(
            COMPRESSION_HEURISTIC_ENABLED_CONFIG,
            ConfigDef.Type.BOOLEAN,
            false,
            ConfigDef.Importance.HIGH,
            COMPRESSION_HEURISTIC_ENABLED_DOC
        );

        CONFIG.define(
            ENCRYPTION_CONFIG,
            ConfigDef.Type.BOOLEAN,
            false,
            ConfigDef.Importance.HIGH,
            ENCRYPTION_DOC
        );

        CONFIG.define(
            METRICS_SAMPLE_WINDOW_MS_CONFIG,
            ConfigDef.Type.LONG,
            30000,
            atLeast(1),
            ConfigDef.Importance.LOW,
            METRICS_SAMPLE_WINDOW_MS_DOC);
        CONFIG.define(
            METRICS_NUM_SAMPLES_CONFIG,
            ConfigDef.Type.INT,
            2,
            atLeast(1),
            ConfigDef.Importance.LOW,
            METRICS_NUM_SAMPLES_DOC);
        CONFIG.define(
            METRICS_RECORDING_LEVEL_CONFIG,
            ConfigDef.Type.STRING,
            Sensor.RecordingLevel.INFO.toString(),
            in(Sensor.RecordingLevel.INFO.toString(),
                Sensor.RecordingLevel.DEBUG.toString(),
                Sensor.RecordingLevel.TRACE.toString()),
            ConfigDef.Importance.LOW,
            METRICS_RECORDING_LEVEL_DOC);

        CONFIG.define(CUSTOM_METADATA_FIELDS_INCLUDE_CONFIG,
            ConfigDef.Type.LIST,
            "",
            ConfigDef.ValidList.in(SegmentCustomMetadataField.names()),
            ConfigDef.Importance.LOW,
            CUSTOM_METADATA_FIELDS_INCLUDE_DOC);
    }

    /**
     * Internal config for encryption.
     *
     * <p>It's needed for more convenient dynamic config definition.
     */
    private static class EncryptionConfig extends AbstractConfig {
        private static final String ENCRYPTION_KEY_PAIR_ID_CONFIG = "encryption.key.pair.id";
        private static final String ENCRYPTION_KEY_PAIR_ID_DOC =
            "The ID of the key pair to be used for encryption";

        private static final String ENCRYPTION_KEY_PAIRS_CONFIG = "encryption.key.pairs";
        private static final String ENCRYPTION_KEY_PAIRS_DOC = "The list of encryption key pair IDs";

        private static final String ENCRYPTION_PUBLIC_KEY_FILE_DOC = "The path to the RSA public key file";
        private static final String ENCRYPTION_PRIVATE_KEY_FILE_DOC = "The path to the RSA private key file";

        private EncryptionConfig(final ConfigDef configDef, final Map<String, ?> props) {
            super(configDef, props);
        }

        Path encryptionPublicKeyFile(final String keyPairId) {
            return Path.of(getString(publicKeyFileConfig(keyPairId)));
        }

        Path encryptionPrivateKeyFile(final String keyPairId) {
            return Path.of(getString(privateKeyFileConfig(keyPairId)));
        }

        public static EncryptionConfig create(final Map<String, ?> props) {
            final ConfigDef configDef = new ConfigDef();
            // First, define the active key ID and key ID list fields, they are required always.
            configDef.define(
                ENCRYPTION_KEY_PAIR_ID_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Importance.HIGH,
                ENCRYPTION_KEY_PAIR_ID_DOC
            );
            configDef.define(
                ENCRYPTION_KEY_PAIRS_CONFIG,
                ConfigDef.Type.LIST,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Importance.HIGH,
                ENCRYPTION_KEY_PAIRS_DOC
            );
            final EncryptionConfig interimEncryptionConfig = new EncryptionConfig(configDef, props);

            // Check that the active ID is present in the list.
            if (!interimEncryptionConfig.keyPairIds().contains(interimEncryptionConfig.activeKeyPairId())) {
                throw new ConfigException(
                    "Encryption key '" + interimEncryptionConfig.activeKeyPairId() + "' must be provided");
            }

            // Then, define key fields dynamically based on the key pair IDs provided above.
            // See e.g. the ConnectorConfig.enrich in the Kafka code.
            for (final String keyPairId : interimEncryptionConfig.keyPairIds()) {
                configDef.define(
                    publicKeyFileConfig(keyPairId),
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    ENCRYPTION_PUBLIC_KEY_FILE_DOC
                );
                configDef.define(
                    privateKeyFileConfig(keyPairId),
                    ConfigDef.Type.STRING,
                    ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH,
                    ENCRYPTION_PRIVATE_KEY_FILE_DOC
                );
            }

            return new EncryptionConfig(configDef, props);
        }

        String activeKeyPairId() {
            return getString(ENCRYPTION_KEY_PAIR_ID_CONFIG);
        }

        List<String> keyPairIds() {
            return getList(ENCRYPTION_KEY_PAIRS_CONFIG);
        }

        private static String publicKeyFileConfig(final String keyPairId) {
            return "encryption.key.pairs." + keyPairId + ".public.key.file";
        }

        private static String privateKeyFileConfig(final String keyPairId) {
            return "encryption.key.pairs." + keyPairId + ".private.key.file";
        }
    }

    private final EncryptionConfig encryptionConfig;

    public RemoteStorageManagerConfig(final Map<String, ?> props) {
        super(CONFIG, props);
        encryptionConfig = encryptionEnabled() ? EncryptionConfig.create(props) : null;
        validate();
    }

    private void validate() {
        validateCompression();
    }

    private void validateCompression() {
        if (getBoolean(COMPRESSION_HEURISTIC_ENABLED_CONFIG) && !getBoolean(COMPRESSION_ENABLED_CONFIG)) {
            throw new ConfigException(
                COMPRESSION_ENABLED_CONFIG + " must be enabled if " + COMPRESSION_HEURISTIC_ENABLED_CONFIG + " is");
        }
    }

    public StorageBackend storage() {
        final Class<?> storageClass = getClass(STORAGE_BACKEND_CLASS_CONFIG);
        final StorageBackend storage = Utils.newInstance(storageClass, StorageBackend.class);
        storage.configure(this.originalsWithPrefix(STORAGE_PREFIX));
        return storage;
    }

    public String keyPrefix() {
        return getString(OBJECT_KEY_PREFIX_CONFIG);
    }

    public boolean keyPrefixMask() {
        return getBoolean(OBJECT_KEY_PREFIX_MASK_CONFIG);
    }

    public int chunkSize() {
        return getInt(CHUNK_SIZE_CONFIG);
    }

    public boolean compressionEnabled() {
        return getBoolean(COMPRESSION_ENABLED_CONFIG);
    }

    public boolean compressionHeuristicEnabled() {
        return getBoolean(COMPRESSION_HEURISTIC_ENABLED_CONFIG);
    }

    public boolean encryptionEnabled() {
        return getBoolean(ENCRYPTION_CONFIG);
    }

    public String encryptionKeyPairId() {
        if (!encryptionEnabled()) {
            return null;
        }
        return encryptionConfig.activeKeyPairId();
    }

    public Map<String, KeyPairPaths> encryptionKeyRing() {
        if (!encryptionEnabled()) {
            return null;
        }

        final Map<String, KeyPairPaths> result = new HashMap<>();
        for (final String keyPairId : encryptionConfig.keyPairIds()) {
            final KeyPairPaths keyPair = new KeyPairPaths(
                encryptionConfig.encryptionPublicKeyFile(keyPairId),
                encryptionConfig.encryptionPrivateKeyFile(keyPairId));
            result.put(keyPairId, keyPair);
        }
        return result;
    }

    public Set<SegmentCustomMetadataField> customMetadataKeysIncluded() {
        return getList(CUSTOM_METADATA_FIELDS_INCLUDE_CONFIG).stream()
            .map(SegmentCustomMetadataField::valueOf)
            .collect(Collectors.toSet());
    }

    public Map<String, ?> fetchIndexesCacheConfigs() {
        return originalsWithPrefix(FETCH_INDEXES_CACHE_PREFIX);
    }

    public CacheConfig segmentManifestCacheConfigs() {
        final var configs = originalsWithPrefix(SEGMENT_MANIFEST_CACHE_PREFIX);
        return MemorySegmentManifestCache.CONFIG_BUILDER.build(configs);
    }
}
