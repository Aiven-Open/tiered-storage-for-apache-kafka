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

package io.aiven.kafka.tieredstorage.fetch;

import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.tieredstorage.fetch.cache.ChunkCache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChunkManagerFactoryConfigTest {

    @Test
    void invalidCacheClass() {
        assertThatThrownBy(() -> new ChunkManagerFactoryConfig(Map.of("fetch.chunk.cache.class", "java.lang.Object")))
                .isInstanceOf(ConfigException.class)
                .hasMessage("fetch.chunk.cache.class should be a subclass of " + ChunkCache.class.getCanonicalName());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "io.aiven.kafka.tieredstorage.fetch.cache.MemoryChunkCache",
        "io.aiven.kafka.tieredstorage.fetch.cache.DiskChunkCache"
    })
    void validCacheClass(final String cacheClass) {
        final ChunkManagerFactoryConfig config = new ChunkManagerFactoryConfig(
                Map.of("fetch.chunk.cache.class", cacheClass)
        );
        assertThat(config.cacheClass().getCanonicalName()).isEqualTo(cacheClass);
    }

    @Test
    void defaultConfig() {
        final ChunkManagerFactoryConfig config = new ChunkManagerFactoryConfig(Map.of());
        assertThat(config.cacheClass()).isNull();
    }
}
