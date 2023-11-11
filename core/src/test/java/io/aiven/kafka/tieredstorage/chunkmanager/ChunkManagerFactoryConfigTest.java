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

package io.aiven.kafka.tieredstorage.chunkmanager;

import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.tieredstorage.chunkmanager.cache.ChunkCache;
import io.aiven.kafka.tieredstorage.chunkmanager.cache.NoOpChunkCache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChunkManagerFactoryConfigTest {

    @Test
    void invalidCacheClass() {
        assertThatThrownBy(() -> new ChunkManagerFactoryConfig(Map.of("chunk.cache.class", "java.lang.Object")))
                .isInstanceOf(ConfigException.class)
                .hasMessage("chunk.cache.class should be a subclass of " + ChunkCache.class.getCanonicalName());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "io.aiven.kafka.tieredstorage.chunkmanager.cache.InMemoryChunkCache",
        "io.aiven.kafka.tieredstorage.chunkmanager.cache.DiskBasedChunkCache"
    })
    void validCacheClass(final String cacheClass) {
        final ChunkManagerFactoryConfig config = new ChunkManagerFactoryConfig(
                Map.of("chunk.cache.class", cacheClass)
        );
        assertThat(config.cacheClass().getCanonicalName()).isEqualTo(cacheClass);
    }

    @Test
    void defaultConfig() {
        final ChunkManagerFactoryConfig config = new ChunkManagerFactoryConfig(Map.of());
        assertThat(config.cacheClass()).isSameAs(NoOpChunkCache.class);
        assertThat(config.cachePrefetchingSize()).isEqualTo(0);
    }

    @Test
    void invalidPrefetchingSize() {
        assertThatThrownBy(() -> new ChunkManagerFactoryConfig(
            Map.of(
                "chunk.cache.class", "io.aiven.kafka.tieredstorage.chunkmanager.cache.InMemoryChunkCache",
                "chunk.cache.prefetch.max.size", "-1"
            )
        )).isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value -1 for configuration chunk.cache.prefetch.max.size: Value must be at least 0");
    }
}
