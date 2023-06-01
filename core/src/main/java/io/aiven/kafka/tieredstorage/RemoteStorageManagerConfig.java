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

package io.aiven.kafka.tieredstorage;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

import io.aiven.kafka.tieredstorage.cache.ChunkCache;
import io.aiven.kafka.tieredstorage.cache.UnboundInMemoryChunkCache;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;

public class RemoteStorageManagerConfig extends AbstractConfig {
    private static final String STORAGE_PREFIX = "storage.";

    private static final String STORAGE_BACKEND_CLASS_NAME_CONFIG = STORAGE_PREFIX + "backend.class.name";
    private static final String STORAGE_BACKEND_CLASS_NAME_DOC = "The storage back-end implementation class name "
        + "to instantiate";

    private static final String OBJECT_KEY_PREFIX_CONFIG = "key.prefix";
    private static final String OBJECT_KEY_PREFIX_DOC = "The object storage path prefix";

    private static final String SEGMENT_MANIFEST_CACHE_PREFIX = "segment.manifest.cache.";
    private static final String SEGMENT_MANIFEST_CACHE_SIZE_CONFIG = SEGMENT_MANIFEST_CACHE_PREFIX + "size";
    private static final Long SEGMENT_MANIFEST_CACHE_SIZE_DEFAULT = 1000L;  // TODO consider a better default
    private static final String SEGMENT_MANIFEST_CACHE_SIZE_DOC =
        "The size in items of the segment manifest cache. "
            + "Use -1 for \"unbounded\". The default is 1000.";

    public static final String SEGMENT_MANIFEST_CACHE_RETENTION_MS_CONFIG = SEGMENT_MANIFEST_CACHE_PREFIX
        + "retention.ms";
    public static final long SEGMENT_MANIFEST_CACHE_RETENTION_MS_DEFAULT = 3_600_000;  // 1 hour
    private static final String SEGMENT_MANIFEST_CACHE_RETENTION_MS_DOC =
        "The retention time for the segment manifest cache. "
            + "Use -1 for \"forever\". The default is 3_600_000 (1 hour).";

    private static final String CHUNK_SIZE_CONFIG = "chunk.size";
    private static final String CHUNK_SIZE_DOC = "The chunk size of log files";

    private static final String CHUNK_CACHE_PREFIX = "chunk.cache.";
    private static final String CHUNK_CACHE_CONFIG = CHUNK_CACHE_PREFIX + "class";
    private static final String CHUNK_CACHE_DOC = "The chunk cache implementation";

    private static final String COMPRESSION_ENABLED_CONFIG = "compression.enabled";
    private static final String COMPRESSION_ENABLED_DOC = "Whether to enable compression";

    private static final String COMPRESSION_HEURISTIC_ENABLED_CONFIG = "compression.heuristic.enabled";
    private static final String COMPRESSION_HEURISTIC_ENABLED_DOC = "Whether to use compression heuristics "
        + "when compression is enabled";

    private static final String ENCRYPTION_CONFIG = "encryption.enabled";
    private static final String ENCRYPTION_DOC = "Whether to enable encryption";
    private static final String ENCRYPTION_PUBLIC_KEY_FILE_CONFIG = "encryption.public.key.file";
    private static final String ENCRYPTION_PUBLIC_KEY_FILE_DOC = "The path to the RSA public key file";
    private static final String ENCRYPTION_PRIVATE_KEY_FILE_CONFIG = "encryption.private.key.file";
    private static final String ENCRYPTION_PRIVATE_KEY_FILE_DOC = "The path to the RSA private key file";
    // TODO add possibility to pass keys as strings

    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef();

        // TODO checkers

        CONFIG.define(
            STORAGE_BACKEND_CLASS_NAME_CONFIG,
            ConfigDef.Type.CLASS,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Importance.HIGH,
            STORAGE_BACKEND_CLASS_NAME_DOC
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
            SEGMENT_MANIFEST_CACHE_SIZE_CONFIG,
            ConfigDef.Type.LONG,
            SEGMENT_MANIFEST_CACHE_SIZE_DEFAULT,
            ConfigDef.Range.atLeast(-1L),
            ConfigDef.Importance.LOW,
            SEGMENT_MANIFEST_CACHE_SIZE_DOC
        );
        CONFIG.define(
            SEGMENT_MANIFEST_CACHE_RETENTION_MS_CONFIG,
            ConfigDef.Type.LONG,
            SEGMENT_MANIFEST_CACHE_RETENTION_MS_DEFAULT,
            ConfigDef.Range.atLeast(-1L),
            ConfigDef.Importance.LOW,
            SEGMENT_MANIFEST_CACHE_RETENTION_MS_DOC
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
            CHUNK_CACHE_CONFIG,
            ConfigDef.Type.CLASS,
            UnboundInMemoryChunkCache.class,
            ConfigDef.Importance.MEDIUM,
            CHUNK_CACHE_DOC
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
            ENCRYPTION_PUBLIC_KEY_FILE_CONFIG,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            ENCRYPTION_PUBLIC_KEY_FILE_DOC
        );
        CONFIG.define(
            ENCRYPTION_PRIVATE_KEY_FILE_CONFIG,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            ENCRYPTION_PRIVATE_KEY_FILE_DOC
        );
    }


    RemoteStorageManagerConfig(final Map<String, ?> props) {
        super(CONFIG, props);
        validate();
    }

    private void validate() {
        validateEncryption();
        validateCompression();
        validateCaching();
    }

    private void validateCompression() {
        if (getBoolean(COMPRESSION_HEURISTIC_ENABLED_CONFIG) && !getBoolean(COMPRESSION_ENABLED_CONFIG)) {
            throw new ConfigException(
                COMPRESSION_ENABLED_CONFIG + " must be enabled if " + COMPRESSION_HEURISTIC_ENABLED_CONFIG + " is");
        }
    }

    private void validateCaching() {
        final Class<?> chunkCacheClass = getClass(CHUNK_CACHE_CONFIG);
        if (chunkCacheClass != null && !ChunkCache.class.isAssignableFrom(chunkCacheClass)) {
            throw new ConfigException(
                CHUNK_CACHE_CONFIG + " must be an implementation of " + ChunkCache.class.getCanonicalName());
        }
    }

    private void validateEncryption() {
        if (getBoolean(ENCRYPTION_CONFIG) && getString(ENCRYPTION_PUBLIC_KEY_FILE_CONFIG) == null) {
            throw new ConfigException(
                ENCRYPTION_PUBLIC_KEY_FILE_CONFIG + " must be provided if encryption is enabled");
        }
        if (getBoolean(ENCRYPTION_CONFIG) && getString(ENCRYPTION_PRIVATE_KEY_FILE_CONFIG) == null) {
            throw new ConfigException(
                ENCRYPTION_PRIVATE_KEY_FILE_CONFIG + " must be provided if encryption is enabled");
        }
    }

    StorageBackend storage() {
        final Class<?> storageClass = getClass(STORAGE_BACKEND_CLASS_NAME_CONFIG);
        final StorageBackend storage = Utils.newInstance(storageClass, StorageBackend.class);
        storage.configure(this.originalsWithPrefix(STORAGE_PREFIX));
        return storage;
    }

    Optional<Long> segmentManifestCacheSize() {
        final long rawValue = getLong(SEGMENT_MANIFEST_CACHE_SIZE_CONFIG);
        if (rawValue == -1) {
            return Optional.empty();
        }
        return Optional.of(rawValue);
    }

    Optional<Duration> segmentManifestCacheRetention() {
        final long rawValue = getLong(SEGMENT_MANIFEST_CACHE_RETENTION_MS_CONFIG);
        if (rawValue == -1) {
            return Optional.empty();
        }
        return Optional.of(Duration.ofMillis(rawValue));
    }

    ChunkCache chunkCache() {
        final Class<?> chunkCacheClass = getClass(CHUNK_CACHE_CONFIG);
        if (chunkCacheClass != null) {
            final ChunkCache chunkCache = Utils.newInstance(chunkCacheClass, ChunkCache.class);
            chunkCache.configure(this.originalsWithPrefix(CHUNK_CACHE_PREFIX));
            return chunkCache;
        } else {
            return null;
        }
    }

    String keyPrefix() {
        return getString(OBJECT_KEY_PREFIX_CONFIG);
    }

    int chunkSize() {
        return getInt(CHUNK_SIZE_CONFIG);
    }

    boolean compressionEnabled() {
        return getBoolean(COMPRESSION_ENABLED_CONFIG);
    }

    boolean compressionHeuristicEnabled() {
        return getBoolean(COMPRESSION_HEURISTIC_ENABLED_CONFIG);
    }

    boolean encryptionEnabled() {
        return getBoolean(ENCRYPTION_CONFIG);
    }

    Path encryptionPublicKeyFile() {
        final String value = getString(ENCRYPTION_PUBLIC_KEY_FILE_CONFIG);
        if (value == null) {
            return null;
        }
        return Path.of(value);
    }

    Path encryptionPrivateKeyFile() {
        final String value = getString(ENCRYPTION_PRIVATE_KEY_FILE_CONFIG);
        if (value == null) {
            return null;
        }
        return Path.of(value);
    }
}
