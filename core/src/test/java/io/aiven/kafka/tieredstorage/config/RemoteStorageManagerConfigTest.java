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

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.header.Headers;

import io.aiven.kafka.tieredstorage.iceberg.NamespaceAwareCachingCatalog;
import io.aiven.kafka.tieredstorage.iceberg.StructureProvider;
import io.aiven.kafka.tieredstorage.manifest.SegmentFormat;
import io.aiven.kafka.tieredstorage.metadata.SegmentCustomMetadataField;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

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
        assertThat(config.segmentFormat()).isEqualTo(SegmentFormat.KAFKA);
        assertThat(config.storage()).isInstanceOf(StorageBackend.class);
        assertThat(config.chunkSize()).isEqualTo(123);
        assertThat(config.compressionEnabled()).isFalse();
        assertThat(config.compressionHeuristicEnabled()).isFalse();
        assertThat(config.encryptionEnabled()).isFalse();
        assertThat(config.encryptionKeyPairId()).isNull();
        assertThat(config.encryptionKeyRing()).isNull();
        assertThat(config.keyPrefix()).isEmpty();
        assertThat(config.keyPrefixMask()).isFalse();
        assertThat(config.customMetadataKeysIncluded()).isEmpty();
        assertThat(config.uploadRateLimit()).isEmpty();
        assertThat(config.structureProvider()).isNull();
        assertThat(config.icebergCatalog()).isNull();
    }

    @ParameterizedTest
    @MethodSource("validSegmentFormatArgs")
    void validSegmentFormat(final String format, final SegmentFormat expectedFormat) {
        final Map<String, String> conf = new HashMap<>(
            Map.of(
                "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
                "chunk.size", "123"
            )
        );
        if (format != null) {
            conf.put("segment.format", format);
        }
        final var config = new RemoteStorageManagerConfig(conf);
        assertThat(config.segmentFormat()).isEqualTo(expectedFormat);
    }

    static Stream<Arguments> validSegmentFormatArgs() {
        return Stream.of(
            Arguments.of("kafka", SegmentFormat.KAFKA),
            Arguments.of("iceberg", SegmentFormat.ICEBERG),
            Arguments.of(null, SegmentFormat.KAFKA)
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {"KafkA", "aaa"})
    void invalidSegmentFormat(final String format) {
        final Map<String, String> config = Map.of(
            "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
            "chunk.size", "123",
            "segment.format", format
        );
        final String expectedErrorMessage = String.format(
            "Invalid value %s for configuration segment.format: String must be one of: kafka, iceberg", format);
        assertThatThrownBy(() -> new RemoteStorageManagerConfig(config))
            .isInstanceOf(ConfigException.class)
            .hasMessage(expectedErrorMessage);
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

    @Test
    void uploadRateLimitInvalid() {
        assertThatThrownBy(() ->
            new RemoteStorageManagerConfig(
                Map.of(
                    "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
                    "chunk.size", "123",
                    "upload.rate.limit.bytes.per.second", "122"
                )
            ))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 122 for configuration upload.rate.limit.bytes.per.second: "
                + "Value must be at least 1048576");
    }

    @Test
    void uploadRateLimitValid() {
        final int limit = 1024 * 1024;
        final var config = new RemoteStorageManagerConfig(
            Map.of(
                "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
                "chunk.size", "123",
                "upload.rate.limit.bytes.per.second", Integer.toString(limit)
            )
        );
        assertThat(config.uploadRateLimit()).hasValue(limit);
    }

    @Nested
    class StructureProviders {
        @Test
        void structureProvider() {
            final var config = new RemoteStorageManagerConfig(
                Map.of(
                    "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
                    "chunk.size", "123",
                    "structure.provider.class", TestStructureProvider.class.getTypeName(),
                    "structure.provider.a", "aaa",
                    "structure.provider.a.b", "123",
                    "structure.provider.a.b.c", "true"
                )
            );
            final StructureProvider structureProvider = config.structureProvider();
            assertThat(structureProvider).isInstanceOf(TestStructureProvider.class);
            assertThat(structureProvider)
                .extracting(o -> ((TestStructureProvider) o).configs)
                .isEqualTo(Map.of(
                    "class", TestStructureProvider.class.getTypeName(),
                    "a", "aaa",
                    "a.b", "123",
                    "a.b.c", "true"
                ));
        }

        public static class TestStructureProvider implements StructureProvider {
            Map<String, Object> configs;

            @SuppressWarnings("unchecked")
            @Override
            public void configure(final Map<String, ?> configs) {
                this.configs = (Map<String, Object>) configs;
            }

            @Override
            public ParsedSchema getSchemaById(final int schemaId) {
                return null;
            }

            @Override
            public ByteBuffer serializeKey(final String topic, final Headers headers, final Object value) {
                return ByteBuffer.allocate(0);
            }

            @Override
            public ByteBuffer serializeValue(final String topic, final Headers headers, final Object record) {
                return ByteBuffer.allocate(0);
            }

            @Override
            public Object deserializeKey(final String topic, final Headers headers, final byte[] data) {
                return null;
            }

            @Override
            public Object deserializeValue(final String topic, final Headers headers, final byte[] data) {
                return null;
            }
        }
    }

    @Nested
    class Iceberg {
        @Test
        void namespaceDefined() {
            final var config = new RemoteStorageManagerConfig(
                Map.of(
                    "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
                    "chunk.size", "123",
                    "iceberg.namespace", "test"
                )
            );
            assertThat(config.icebergNamespace()).isEqualTo(Namespace.of("test"));
        }

        @Test
        void namespaceNotDefined() {
            final var config = new RemoteStorageManagerConfig(
                Map.of(
                    "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
                    "chunk.size", "123"
                )
            );
            assertThat(config.icebergNamespace()).isEqualTo(Namespace.empty());
        }

        @Test
        void catalogInitializedWithDefaultCaching() {
            final var config = new RemoteStorageManagerConfig(
                Map.of(
                    "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
                    "chunk.size", "123",
                    "iceberg.catalog.class", TestIcebergCatalog.class.getTypeName(),
                    "iceberg.catalog.a", "aaa",
                    "iceberg.catalog.a.b", "123",
                    "iceberg.catalog.a.b.c", "true"
                )
            );
            final Catalog catalog = config.icebergCatalog();
            assertThat(catalog).isInstanceOf(NamespaceAwareCachingCatalog.class);
            
            final Catalog originalCatalog = ((NamespaceAwareCachingCatalog) catalog).getOriginalCatalog();
            
            assertThat(originalCatalog).isInstanceOf(TestIcebergCatalog.class);
            assertThat(originalCatalog)
                .extracting(o -> ((TestIcebergCatalog) o).configs)
                .isEqualTo(Map.of(
                    "class", TestIcebergCatalog.class.getTypeName(),
                    "a", "aaa",
                    "a.b", "123",
                    "a.b.c", "true"
                ));
        }

        @Test
        void catalogInitializedWithCachingDisabled() {
            final var config = new RemoteStorageManagerConfig(
                Map.of(
                    "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
                    "chunk.size", "123",
                    "iceberg.catalog.class", TestIcebergCatalog.class.getTypeName(),
                    "iceberg.catalog.cache.enabled", "false",
                    "iceberg.catalog.a", "aaa",
                    "iceberg.catalog.a.b", "123",
                    "iceberg.catalog.a.b.c", "true"
                )
            );
            final Catalog catalog = config.icebergCatalog();
            assertThat(catalog).isInstanceOf(TestIcebergCatalog.class);

            assertThat(catalog)
                .extracting(o -> ((TestIcebergCatalog) o).configs)
                .isEqualTo(Map.of(
                    "class", TestIcebergCatalog.class.getTypeName(),
                    "cache.enabled", "false",
                    "a", "aaa",
                    "a.b", "123",
                    "a.b.c", "true"
                ));
        }

        @Test
        void catalogInitializedWithCustomCacheSettings() {
            final var config = new RemoteStorageManagerConfig(
                Map.of(
                    "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
                    "chunk.size", "123",
                    "iceberg.catalog.class", TestIcebergCatalog.class.getTypeName(),
                    "iceberg.catalog.cache.enabled", "true",
                    "iceberg.catalog.cache.expiration.ms", "300000",  // 5 minutes
                    "iceberg.catalog.a", "aaa"
                )
            );
            final Catalog catalog = config.icebergCatalog();
            assertThat(catalog).isInstanceOf(NamespaceAwareCachingCatalog.class);
            
            final Catalog originalCatalog = ((NamespaceAwareCachingCatalog) catalog).getOriginalCatalog();
            assertThat(originalCatalog).isInstanceOf(TestIcebergCatalog.class);
            
            assertThat(originalCatalog)
                .extracting(o -> ((TestIcebergCatalog) o).configs)
                .extracting(m -> m.get("cache.enabled"))
                .isEqualTo("true");
        }

        public static class TestIcebergCatalog implements Catalog {
            Map<String, String> configs;

            @Override
            public void initialize(final String name, final Map<String, String> properties) {
                this.configs = properties;
            }

            @Override
            public List<TableIdentifier> listTables(final Namespace namespace) {
                return List.of();
            }

            @Override
            public boolean dropTable(final TableIdentifier identifier, final boolean purge) {
                return false;
            }

            @Override
            public void renameTable(final TableIdentifier from, final TableIdentifier to) {

            }

            @Override
            public Table loadTable(final TableIdentifier identifier) {
                return null;
            }
        }

        @ParameterizedTest
        @NullSource
        @MethodSource
        void catalogNonStringConfigsAreNotAllowed(final Object value) {
            final Map<String, Object> configs = new HashMap<>(Map.of(
                "storage.backend.class", NoopStorageBackend.class.getCanonicalName(),
                "chunk.size", "123",
                "iceberg.catalog.class", TestIcebergCatalog.class.getTypeName()
            ));
            configs.put("iceberg.catalog.a", value);
            final var config = new RemoteStorageManagerConfig(configs);
            assertThatThrownBy(config::icebergCatalog)
                .isInstanceOf(ConfigException.class)
                .hasMessage("Unknown type of a KV pair in Iceberg config: a %s", value);
        }

        static Stream<Arguments> catalogNonStringConfigsAreNotAllowed() {
            return Stream.of(
                Arguments.of(123),
                Arguments.of(12.3)
            );
        }
    }
}
