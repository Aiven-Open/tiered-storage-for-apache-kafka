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
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.tieredstorage.metadata.SegmentCustomMetadataField;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

class RemoteStorageManagerConfigTest {
    @Test
    void minimalConfig() {
        final var config = new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
                "chunk.size", "123"
            )
        );
        assertThat(config.storage()).isInstanceOf(StorageBackend.class);
        assertThat(config.segmentManifestCacheConfigs().cacheSize()).hasValue(1000L);
        assertThat(config.segmentManifestCacheConfigs().cacheRetention()).hasValue(Duration.ofHours(1));
        assertThat(config.chunkSize()).isEqualTo(123);
        assertThat(config.compressionEnabled()).isFalse();
        assertThat(config.compressionHeuristicEnabled()).isFalse();
        assertThat(config.encryptionEnabled()).isFalse();
        assertThat(config.encryptionKeyPairId()).isNull();
        assertThat(config.encryptionKeyRing()).isNull();
        assertThat(config.keyPrefix()).isEmpty();
        assertThat(config.keyPrefixMask()).isFalse();
        assertThat(config.customMetadataKeysIncluded()).isEmpty();
    }

    @Test
    void segmentManifestCacheSizeUnbounded() {
        final var config = new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
                "chunk.size", "123",
                "segment.manifest.cache.size", "-1"
            )
        );
        assertThat(config.segmentManifestCacheConfigs().cacheSize()).isEmpty();
    }

    @Test
    void segmentManifestCacheSizeBounded() {
        final var config = new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
                "chunk.size", "123",
                "segment.manifest.cache.size", "42"
            )
        );
        assertThat(config.segmentManifestCacheConfigs().cacheSize()).hasValue(42L);
    }

    @Test
    void segmentManifestCacheRetentionForever() {
        final var config = new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
                "chunk.size", "123",
                "segment.manifest.cache.retention.ms", "-1"
            )
        );
        assertThat(config.segmentManifestCacheConfigs().cacheRetention()).isEmpty();
    }

    @Test
    void segmentManifestCacheRetentionLimited() {
        final var config = new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
                "chunk.size", "123",
                "segment.manifest.cache.retention.ms", "42"
            )
        );
        assertThat(config.segmentManifestCacheConfigs().cacheRetention()).hasValue(Duration.ofMillis(42));
    }

    @Test
    void compression() {
        final var config = new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
                "chunk.size", "123",
                "compression.enabled", "true",
                "compression.heuristic.enabled", "true"
            )
        );
        assertThat(config.compressionEnabled()).isTrue();
        assertThat(config.compressionHeuristicEnabled()).isTrue();
    }

    @Test
    void encryption() {
        final var config = new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
                "chunk.size", "123",
                "encryption.enabled", "true",
                "encryption.key.pair.id", "k2",
                "encryption.key.pairs", "k1,k2",
                "encryption.key.pairs.k1.public.key.file", "k1_public.key",
                "encryption.key.pairs.k1.private.key.file", "k1_private.key",
                "encryption.key.pairs.k2.public.key.file", "k2_public.key",
                "encryption.key.pairs.k2.private.key.file", "k2_private.key"
            )
        );
        assertThat(config.encryptionEnabled()).isTrue();
        assertThat(config.encryptionKeyPairId()).isEqualTo("k2");
        assertThat(config.encryptionKeyRing()).containsExactly(
            entry("k1", new KeyPairPaths(Path.of("k1_public.key"), Path.of("k1_private.key"))),
            entry("k2", new KeyPairPaths(Path.of("k2_public.key"), Path.of("k2_private.key")))
        );
    }

    @Test
    void encryptionEnabledKeyPairIdIsNotProvided() {
        final var config = Map.of(
            "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
            "chunk.size", "123",
            "encryption.enabled", "true"
        );
        assertThatThrownBy(() -> new RemoteStorageManagerConfig(config))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Missing required configuration \"encryption.key.pair.id\" which has no default value.");
    }

    @Test
    void encryptionEnabledKeyPairsAreNotProvided() {
        final var config = Map.of(
            "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
            "chunk.size", "123",
            "encryption.enabled", "true",
            "encryption.key.pair.id", "k1"
        );
        assertThatThrownBy(() -> new RemoteStorageManagerConfig(config))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Missing required configuration \"encryption.key.pairs\" which has no default value.");
    }

    @Test
    void encryptionEnabledActiveKeyNotDefined() {
        // The key defined in `encryption.key.pair.id` is not present in `encryption.key.pairs`.
        final var config = Map.of(
            "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
            "chunk.size", "123",
            "encryption.enabled", "true",
            "encryption.key.pair.id", "k1",
            "encryption.key.pairs", ""
        );
        assertThatThrownBy(() -> new RemoteStorageManagerConfig(config))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Encryption key 'k1' must be provided");
    }

    @Test
    void encryptionEnabledMissingPublicKey() {
        final var config = Map.of(
            "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
            "chunk.size", "123",
            "encryption.enabled", "true",
            "encryption.key.pair.id", "k1",
            "encryption.key.pairs", "k1"
        );
        assertThatThrownBy(() -> new RemoteStorageManagerConfig(config))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Missing required configuration "
                + "\"encryption.key.pairs.k1.public.key.file\" which has no default value.");
    }

    @Test
    void encryptionEnabledMissingPrivateKey() {
        final var config = Map.of(
            "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
            "chunk.size", "123",
            "encryption.enabled", "true",
            "encryption.key.pair.id", "k1",
            "encryption.key.pairs", "k1",
            "encryption.key.pairs.k1.public.key.file", "k1_public.key"
        );
        assertThatThrownBy(() -> new RemoteStorageManagerConfig(config))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Missing required configuration "
                + "\"encryption.key.pairs.k1.private.key.file\" which has no default value.");
    }

    @Test
    void objectStorageFactoryIncorrectClass() {
        assertThatThrownBy(() -> new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class", "x"
            )
        )).isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value x for configuration storage.backend.class: Class x could not be found.");
    }

    @Test
    void invalidKeyPrefix() {
        final HashMap<String, Object> props = new HashMap<>();
        props.put("storage.backend.class", NoopStorageBackend.class);
        props.put("key.prefix", null);

        assertThatThrownBy(() -> new RemoteStorageManagerConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value null for configuration key.prefix: entry must be non null");
    }

    @Test
    void validKeyPrefix() {
        final String testPrefix = "test_prefix";
        final HashMap<String, Object> props = new HashMap<>();
        props.put("storage.backend.class", NoopStorageBackend.class);
        props.put("chunk.size", "123");
        props.put("key.prefix", testPrefix);
        final RemoteStorageManagerConfig config = new RemoteStorageManagerConfig(props);
        assertThat(config.keyPrefix()).isEqualTo(testPrefix);
    }

    @Test
    void missingRequiredFields() {
        assertThatThrownBy(() -> new RemoteStorageManagerConfig(Map.of()))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Missing required configuration \"storage.backend.class\" which has no default value.");

        assertThatThrownBy(() -> new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class", NoopStorageBackend.class.getCanonicalName()
            )
        )).isInstanceOf(ConfigException.class)
            .hasMessage("Missing required configuration \"chunk.size\" which has no default value.");
    }

    @Test
    void objectStorageFactoryIsConfigured() {
        final var config = new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
                "storage.config1", "aaa",
                "storage.config2", "123",
                "storage.config3", "true",
                "chunk.size", "123"
            )
        );
        final NoopStorageBackend factory = (NoopStorageBackend) config.storage();
        assertThat(factory.configureCalled).isTrue();
        assertThat(factory.configuredWith).isEqualTo(new NoopStorageBackend.Config(Map.of(
            "backend.class", NoopStorageBackend.class.getCanonicalName(),
            "config1", "aaa",
            "config2", "123",
            "config3", "true"
        )));
    }

    @Test
    void invalidChunkSizeRange() {
        assertThatThrownBy(() -> new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
                "chunk.size", "0"
            )
        )).isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 0 for configuration chunk.size: Value must be at least 1");

        assertThatThrownBy(() -> new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
                "chunk.size", Long.toString((long) Integer.MAX_VALUE + 1)
            )
        )).isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 2147483648 for configuration chunk.size: Not a number of type INT");
    }


    @Test
    void invalidCompressionConfig() {
        assertThatThrownBy(() -> new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
                "chunk.size", "123",
                "compression.enabled", "false",
                "compression.heuristic.enabled", "true"
            )))
            .isInstanceOf(ConfigException.class)
            .hasMessage("compression.enabled must be enabled if compression.heuristic.enabled is");
    }

    @Test
    void validCustomMetadataFieldsUppercase() {
        final var config = new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
                "chunk.size", "123",
                "custom.metadata.fields.include", "REMOTE_SIZE,OBJECT_PREFIX,OBJECT_KEY"
            )
        );
        assertThat(config.customMetadataKeysIncluded()).containsExactlyInAnyOrder(
            SegmentCustomMetadataField.REMOTE_SIZE,
            SegmentCustomMetadataField.OBJECT_KEY,
            SegmentCustomMetadataField.OBJECT_PREFIX
        );
    }

    @Test
    void invalidCustomMetadataFields() {
        assertThatThrownBy(() -> new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
                "chunk.size", "123",
                "custom.metadata.fields.include", "unknown"
            )))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value unknown for configuration custom.metadata.fields.include: "
                + "String must be one of: REMOTE_SIZE, OBJECT_PREFIX, OBJECT_KEY");
    }

    @Test
    void keyPrefixMasking() {
        final var config = new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
                "chunk.size", "123",
                "key.prefix.mask", "true"
            )
        );
        assertThat(config.keyPrefixMask()).isTrue();
    }
}
