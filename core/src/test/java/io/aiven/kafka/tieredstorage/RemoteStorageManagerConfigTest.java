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
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.tieredstorage.cache.TestChunkCache;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RemoteStorageManagerConfigTest {
    @Test
    void minimalConfig() {
        final var config = new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class.name", NoopStorageBackend.class.getCanonicalName(),
                "chunk.size", "123"
            )
        );
        assertThat(config.storage()).isInstanceOf(StorageBackend.class);
        assertThat(config.segmentManifestCacheSize()).hasValue(1000L);
        assertThat(config.segmentManifestCacheRetention()).hasValue(Duration.ofHours(1));
        assertThat(config.chunkSize()).isEqualTo(123);
        assertThat(config.compressionEnabled()).isFalse();
        assertThat(config.compressionHeuristicEnabled()).isFalse();
        assertThat(config.encryptionEnabled()).isFalse();
        assertThat(config.encryptionPrivateKeyFile()).isNull();
        assertThat(config.encryptionPublicKeyFile()).isNull();
        assertThat(config.keyPrefix()).isEmpty();
    }

    @Test
    void segmentManifestCacheSizeUnbounded() {
        final var config = new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class.name", NoopStorageBackend.class.getCanonicalName(),
                "chunk.size", "123",
                "segment.manifest.cache.size", "-1"
            )
        );
        assertThat(config.segmentManifestCacheSize()).isEmpty();
    }

    @Test
    void segmentManifestCacheSizeBounded() {
        final var config = new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class.name", NoopStorageBackend.class.getCanonicalName(),
                "chunk.size", "123",
                "segment.manifest.cache.size", "42"
            )
        );
        assertThat(config.segmentManifestCacheSize()).hasValue(42L);
    }

    @Test
    void segmentManifestCacheRetentionForever() {
        final var config = new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class.name", NoopStorageBackend.class.getCanonicalName(),
                "chunk.size", "123",
                "segment.manifest.cache.retention.ms", "-1"
            )
        );
        assertThat(config.segmentManifestCacheRetention()).isEmpty();
    }

    @Test
    void segmentManifestCacheRetentionLimited() {
        final var config = new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class.name", NoopStorageBackend.class.getCanonicalName(),
                "chunk.size", "123",
                "segment.manifest.cache.retention.ms", "42"
            )
        );
        assertThat(config.segmentManifestCacheRetention()).hasValue(Duration.ofMillis(42));
    }

    @Test
    void compression() {
        final var config = new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class.name", NoopStorageBackend.class.getCanonicalName(),
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
                "storage.backend.class.name", NoopStorageBackend.class.getCanonicalName(),
                "chunk.size", "123",
                "encryption.enabled", "true",
                "encryption.public.key.file", "public.key",
                "encryption.private.key.file", "private.key"
            )
        );
        assertThat(config.encryptionEnabled()).isTrue();
        assertThat(config.encryptionPrivateKeyFile()).isEqualTo(Path.of("private.key"));
        assertThat(config.encryptionPublicKeyFile()).isEqualTo(Path.of("public.key"));
    }

    @Test
    void rsaKeysMustBeProvided() {
        final var config1 = Map.of(
            "storage.backend.class.name", NoopStorageBackend.class.getCanonicalName(),
            "chunk.size", "123",
            "encryption.enabled", "true"
        );
        assertThatThrownBy(() -> new RemoteStorageManagerConfig(config1))
            .isInstanceOf(ConfigException.class)
            .hasMessage("encryption.public.key.file must be provided if encryption is enabled");

        final var config2 = Map.of(
            "storage.backend.class.name", NoopStorageBackend.class.getCanonicalName(),
            "chunk.size", "123",
            "encryption.enabled", "true",
            "encryption.public.key.file", "public.key"
        );
        assertThatThrownBy(() -> new RemoteStorageManagerConfig(config2))
            .isInstanceOf(ConfigException.class)
            .hasMessage("encryption.private.key.file must be provided if encryption is enabled");
    }

    @Test
    void objectStorageFactoryIncorrectClass() {
        assertThatThrownBy(() -> new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class.name", "x"
            )
        )).isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value x for configuration storage.backend.class.name: Class x could not be found.");
    }

    @Test
    void invalidKeyPrefix() {
        final HashMap<String, Object> props = new HashMap<>();
        props.put("storage.backend.class.name", NoopStorageBackend.class);
        props.put("key.prefix", null);

        assertThatThrownBy(() -> new RemoteStorageManagerConfig(props))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value null for configuration key.prefix: entry must be non null");
    }

    @Test
    void validKeyPrefix() {
        final String testPrefix = "test_prefix";
        final HashMap<String, Object> props = new HashMap<>();
        props.put("storage.backend.class.name", NoopStorageBackend.class);
        props.put("chunk.size", "123");
        props.put("key.prefix", testPrefix);
        final RemoteStorageManagerConfig config = new RemoteStorageManagerConfig(props);
        assertThat(config.keyPrefix()).isEqualTo(testPrefix);
    }

    @Test
    void missingRequiredFields() {
        assertThatThrownBy(() -> new RemoteStorageManagerConfig(Map.of()))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Missing required configuration \"storage.backend.class.name\" which has no default value.");

        assertThatThrownBy(() -> new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class.name", NoopStorageBackend.class.getCanonicalName()
            )
        )).isInstanceOf(ConfigException.class)
            .hasMessage("Missing required configuration \"chunk.size\" which has no default value.");
    }

    @Test
    void objectStorageFactoryIsConfigured() {
        final var config = new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class.name", NoopStorageBackend.class.getCanonicalName(),
                "storage.config1", "aaa",
                "storage.config2", "123",
                "storage.config3", "true",
                "chunk.size", "123"
            )
        );
        final NoopStorageBackend factory = (NoopStorageBackend) config.storage();
        assertThat(factory.configureCalled).isTrue();
        assertThat(factory.configuredWith).isEqualTo(new NoopStorageBackend.Config(Map.of(
            "backend.class.name", NoopStorageBackend.class.getCanonicalName(),
            "config1", "aaa",
            "config2", "123",
            "config3", "true"
        )));
    }

    @Test
    void invalidChunkSizeRange() {
        assertThatThrownBy(() -> new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class.name", NoopStorageBackend.class.getCanonicalName(),
                "chunk.size", "0"
            )
        )).isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 0 for configuration chunk.size: Value must be at least 1");

        assertThatThrownBy(() -> new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class.name", NoopStorageBackend.class.getCanonicalName(),
                "chunk.size", Long.toString((long) Integer.MAX_VALUE + 1)
            )
        )).isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 2147483648 for configuration chunk.size: Not a number of type INT");
    }

    @Test
    void invalidChunkCacheClass() {
        final HashMap<String, Object> props1 = new HashMap<>();
        props1.put("storage.backend.class.name", NoopStorageBackend.class);
        props1.put("chunk.size", 123);
        props1.put("chunk.cache.class", "x");

        assertThatThrownBy(() -> new RemoteStorageManagerConfig(props1))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value x for configuration chunk.cache.class: Class x could not be found.");

        final HashMap<String, Object> props2 = new HashMap<>();
        props2.put("storage.backend.class.name", NoopStorageBackend.class);
        props2.put("chunk.size", 123);
        props2.put("chunk.cache.class", Object.class);

        assertThatThrownBy(() -> new RemoteStorageManagerConfig(props2))
            .isInstanceOf(ConfigException.class)
            .hasMessage("chunk.cache.class must be an implementation "
                + "of io.aiven.kafka.tieredstorage.cache.ChunkCache");
    }

    @Test
    void disabledChunkCache() {
        final HashMap<String, Object> props = new HashMap<>();
        props.put("storage.backend.class.name", NoopStorageBackend.class);
        props.put("chunk.size", 123);
        props.put("chunk.cache.class", null);

        final RemoteStorageManagerConfig config = new RemoteStorageManagerConfig(props);
        assertThat(config.chunkCache()).isNull();
    }

    @Test
    void chuckCacheIsConfigured() {
        final var config = new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class.name", NoopStorageBackend.class.getCanonicalName(),
                "chunk.size", "123",
                "chunk.cache.class", TestChunkCache.class.getCanonicalName(),
                "chunk.cache.config1", "aaa",
                "chunk.cache.config2", "123",
                "chunk.cache.config3", "true"
            )
        );
        final TestChunkCache chunkCache = (TestChunkCache) config.chunkCache();
        assertThat(chunkCache.configureCalled).isTrue();
        assertThat(chunkCache.configuredWith).isEqualTo(Map.of(
            "class", TestChunkCache.class.getCanonicalName(),
            "config1", "aaa",
            "config2", "123",
            "config3", "true"
        ));
    }

    @Test
    void invalidCompressionConfig() {
        assertThatThrownBy(() -> new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class.name", NoopStorageBackend.class.getCanonicalName(),
                "chunk.size", "123",
                "compression.enabled", "false",
                "compression.heuristic.enabled", "true"
            )))
            .isInstanceOf(ConfigException.class)
            .hasMessage("compression.enabled must be enabled if compression.heuristic.enabled is");
    }
}
