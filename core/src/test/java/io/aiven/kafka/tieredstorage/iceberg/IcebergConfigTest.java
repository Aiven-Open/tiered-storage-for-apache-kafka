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

package io.aiven.kafka.tieredstorage.iceberg;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.header.Headers;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IcebergConfigTest {
    @Nested
    class StructureProviders {
        @Test
        void structureProvider() {
            final var config = new IcebergConfig(
                Map.of(
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
            final var config = new IcebergConfig(
                Map.of(
                    "iceberg.namespace", "test"
                )
            );
            assertThat(config.icebergNamespace()).isEqualTo(Namespace.of("test"));
        }

        @Test
        void namespaceNotDefined() {
            final var config = new IcebergConfig(
                Map.of()
            );
            assertThat(config.icebergNamespace()).isEqualTo(Namespace.empty());
        }

        @Test
        void catalogInitializedWithDefaultCaching() {
            final var config = new IcebergConfig(
                Map.of(
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
            final var config = new IcebergConfig(
                Map.of(
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
            final var config = new IcebergConfig(
                Map.of(
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

        public static class TestIcebergCatalog extends InMemoryCatalog {
            Map<String, String> configs;

            @Override
            public void initialize(final String name, final Map<String, String> properties) {
                super.initialize(name, properties);
                this.configs = properties;
            }
        }

        @ParameterizedTest
        @NullSource
        @MethodSource
        void catalogNonStringConfigsAreNotAllowed(final Object value) {
            final Map<String, Object> configs = new HashMap<>(Map.of(
                "iceberg.catalog.class", TestIcebergCatalog.class.getTypeName()
            ));
            configs.put("iceberg.catalog.a", value);
            final var config = new IcebergConfig(configs);
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
