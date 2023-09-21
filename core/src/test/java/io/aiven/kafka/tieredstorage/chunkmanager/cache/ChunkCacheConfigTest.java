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

package io.aiven.kafka.tieredstorage.chunkmanager.cache;

import java.time.Duration;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChunkCacheConfigTest {

    @Test
    void defaults() {
        final ChunkCacheConfig config = new ChunkCacheConfig(
                new ConfigDef(),
                Map.of("size", "-1")
        );

        assertThat(config.cacheRetention()).hasValue(Duration.ofMinutes(10));
    }

    @Test
    void cacheSizeUnbounded() {
        final ChunkCacheConfig config = new ChunkCacheConfig(
                new ConfigDef(),
                Map.of("size", "-1")
        );
        assertThat(config.cacheSize()).isEmpty();
    }

    @Test
    void cacheSizeBounded() {
        final ChunkCacheConfig config = new ChunkCacheConfig(
                new ConfigDef(),
                Map.of("size", "1024")
        );
        assertThat(config.cacheSize()).hasValue(1024L);
    }

    @Test
    void invalidCacheSize() {
        assertThatThrownBy(() -> new ChunkCacheConfig(
                new ConfigDef(),
                Map.of("size", "-2")
        )).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value -2 for configuration size: Value must be at least -1");
    }

    @Test
    void cacheSizeUnspecified() {
        assertThatThrownBy(() -> new ChunkCacheConfig(
                new ConfigDef(),
                Map.of()
        )).isInstanceOf(ConfigException.class)
                .hasMessage("Missing required configuration \"size\" which has no default value.");
    }

    @Test
    void cacheRetentionForever() {
        final ChunkCacheConfig config = new ChunkCacheConfig(
                new ConfigDef(),
                Map.of(
                        "retention.ms", "-1",
                        "size", "-1"
                )
        );
        assertThat(config.cacheRetention()).isEmpty();
    }

    @Test
    void cacheRetentionLimited() {
        final ChunkCacheConfig config = new ChunkCacheConfig(
                new ConfigDef(),
                Map.of(
                        "retention.ms", "60000",
                        "size", "-1"
                )
        );
        assertThat(config.cacheRetention()).hasValue(Duration.ofMillis(60000));
    }

    @Test
    void invalidRetention() {
        assertThatThrownBy(() -> new ChunkCacheConfig(
                new ConfigDef(),
                Map.of(
                        "retention.ms", "-2",
                        "size", "-1"
                )
        )).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value -2 for configuration retention.ms: Value must be at least -1");
    }

    @Test
    void invalidPrefetchingSize() {
        assertThatThrownBy(() -> new ChunkCacheConfig(
                new ConfigDef(),
                Map.of(
                    "size", "-1",
                    "prefetching.bytes", "-1"
                )
        )).isInstanceOf(ConfigException.class)
                .hasMessage("Invalid value -1 for configuration prefetching.bytes: Value must be at least 0");
    }
}
