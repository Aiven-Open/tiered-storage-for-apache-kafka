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

package io.aiven.kafka.tieredstorage.commons;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.tieredstorage.commons.cache.TestChunkCache;
import io.aiven.kafka.tieredstorage.commons.storage.ObjectStorageFactory;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UniversalRemoteStorageManagerConfigTest {
    @Test
    void minimalConfig() {
        final var config = new UniversalRemoteStorageManagerConfig(
            Map.of(
                "object.storage.factory", TestObjectStorageFactory.class.getCanonicalName(),
                "chunk.size", "123"
            )
        );
        assertThat(config.objectStorageFactory()).isInstanceOf(ObjectStorageFactory.class);
        assertThat(config.chunkSize()).isEqualTo(123);
        assertThat(config.compressionEnabled()).isFalse();
        assertThat(config.encryptionEnabled()).isFalse();
        assertThat(config.encryptionPrivateKeyFile()).isNull();
        assertThat(config.encryptionPublicKeyFile()).isNull();
        assertThat(config.keyPrefix()).isEmpty();
    }

    @Test
    void compression() {
        final var config = new UniversalRemoteStorageManagerConfig(
            Map.of(
                "object.storage.factory", TestObjectStorageFactory.class.getCanonicalName(),
                "chunk.size", "123",
                "compression.enabled", "true"
            )
        );
        assertThat(config.compressionEnabled()).isTrue();
    }

    @Test
    void encryption() {
        final var config = new UniversalRemoteStorageManagerConfig(
            Map.of(
                "object.storage.factory", TestObjectStorageFactory.class.getCanonicalName(),
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
            "object.storage.factory", TestObjectStorageFactory.class.getCanonicalName(),
            "chunk.size", "123",
            "encryption.enabled", "true"
        );
        assertThatThrownBy(() -> new UniversalRemoteStorageManagerConfig(config1))
            .isInstanceOf(ConfigException.class)
            .hasMessage("encryption.public.key.file must be provided if encryption is enabled");

        final var config2 = Map.of(
            "object.storage.factory", TestObjectStorageFactory.class.getCanonicalName(),
            "chunk.size", "123",
            "encryption.enabled", "true",
            "encryption.public.key.file", "public.key"
        );
        assertThatThrownBy(() -> new UniversalRemoteStorageManagerConfig(config2))
            .isInstanceOf(ConfigException.class)
            .hasMessage("encryption.private.key.file must be provided if encryption is enabled");
    }

    @Test
    void objectStorageFactoryIncorrectClass() {
        assertThatThrownBy(() -> new UniversalRemoteStorageManagerConfig(
            Map.of(
                "object.storage.factory", "x"
            )
        )).isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value x for configuration object.storage.factory: Class x could not be found.");
    }

    @Test
    void invalidKeyPrefix() {
        assertThatThrownBy(() -> new UniversalRemoteStorageManagerConfig(
            new HashMap<>() {{
                    put("object.storage.factory", TestObjectStorageFactory.class);
                    put("key.prefix", null);
                }}
        )).isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value null for configuration key.prefix: entry must be non null");
    }

    @Test
    void validKeyPrefix() {
        final String testPrefix = "test_prefix";
        final UniversalRemoteStorageManagerConfig config =
            new UniversalRemoteStorageManagerConfig(
                new HashMap<>() {{
                        put("object.storage.factory", TestObjectStorageFactory.class);
                        put("chunk.size", "123");
                        put("key.prefix", testPrefix);
                    }}
            );
        assertThat(config.keyPrefix()).isEqualTo(testPrefix);
    }

    @Test
    void missingRequiredFields() {
        assertThatThrownBy(() -> new UniversalRemoteStorageManagerConfig(Map.of()))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Missing required configuration \"object.storage.factory\" which has no default value.");

        assertThatThrownBy(() -> new UniversalRemoteStorageManagerConfig(
            Map.of(
                "object.storage.factory", TestObjectStorageFactory.class.getCanonicalName()
            )
        )).isInstanceOf(ConfigException.class)
            .hasMessage("Missing required configuration \"chunk.size\" which has no default value.");
    }

    @Test
    void objectStorageFactoryIsConfigured() {
        final var config = new UniversalRemoteStorageManagerConfig(
            Map.of(
                "object.storage.factory", TestObjectStorageFactory.class.getCanonicalName(),
                "object.storage.config1", "aaa",
                "object.storage.config2", "123",
                "object.storage.config3", "true",
                "chunk.size", "123"
            )
        );
        final TestObjectStorageFactory factory = (TestObjectStorageFactory) config.objectStorageFactory();
        assertThat(factory.configureCalled).isTrue();
        assertThat(factory.configuredWith).isEqualTo(new TestObjectStorageFactory.Config(Map.of(
            "factory", TestObjectStorageFactory.class.getCanonicalName(),
            "config1", "aaa",
            "config2", "123",
            "config3", "true"
        )));
    }

    @Test
    void invalidChunkSizeRange() {
        assertThatThrownBy(() -> new UniversalRemoteStorageManagerConfig(
            Map.of(
                "object.storage.factory", TestObjectStorageFactory.class.getCanonicalName(),
                "chunk.size", "0"
            )
        )).isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 0 for configuration chunk.size: Value must be at least 1");

        assertThatThrownBy(() -> new UniversalRemoteStorageManagerConfig(
            Map.of(
                "object.storage.factory", TestObjectStorageFactory.class.getCanonicalName(),
                "chunk.size", Long.toString((long) Integer.MAX_VALUE + 1)
            )
        )).isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 2147483648 for configuration chunk.size: Not a number of type INT");
    }

    @Test
    void invalidChunkCacheClass() {
        assertThatThrownBy(() -> new UniversalRemoteStorageManagerConfig(
            new HashMap<>() {{
                    put("object.storage.factory", TestObjectStorageFactory.class);
                    put("chunk.size", 123);
                    put("chunk.cache.class", "x");
                }}
        )).isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value x for configuration chunk.cache.class: Class x could not be found.");
        assertThatThrownBy(() -> new UniversalRemoteStorageManagerConfig(
            new HashMap<>() {{
                    put("object.storage.factory", TestObjectStorageFactory.class);
                    put("chunk.size", 123);
                    put("chunk.cache.class", Object.class);
                }}
        )).isInstanceOf(ConfigException.class)
            .hasMessage("chunk.cache.class must be an implementation "
                + "of io.aiven.kafka.tieredstorage.commons.cache.ChunkCache");
    }

    @Test
    void disabledChunkCache() {
        final UniversalRemoteStorageManagerConfig config = new UniversalRemoteStorageManagerConfig(
            new HashMap<>() {{
                    put("object.storage.factory", TestObjectStorageFactory.class);
                    put("chunk.size", 123);
                    put("chunk.cache.class", null);
                }}
        );
        assertThat(config.chunkCache()).isNull();
    }

    @Test
    void chuckCacheIsConfigured() {
        final var config = new UniversalRemoteStorageManagerConfig(
            Map.of(
                "object.storage.factory", TestObjectStorageFactory.class.getCanonicalName(),
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
}
