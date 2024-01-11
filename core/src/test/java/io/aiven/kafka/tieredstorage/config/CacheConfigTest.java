/*
 * Copyright 2024 Aiven Oy
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

import java.time.Duration;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CacheConfigTest {

    @Test
    void cacheUnboundedSize() {
        final CacheConfig config = new CacheConfig(
            Map.of("size", "-1")
        );

        assertThat(config.cacheSize()).isNotPresent();
        assertThat(config.cacheRetention()).hasValue(Duration.ofMinutes(10));
    }

    @Test
    void cacheUnboundedWithDefaultSize() {
        final CacheConfig config = new CacheConfig(
            Map.of(),
            -1
        );

        assertThat(config.cacheSize()).isNotPresent();
        assertThat(config.cacheRetention()).hasValue(Duration.ofMinutes(10));
    }

    @Test
    void cacheSizeBounded() {
        final CacheConfig config = new CacheConfig(
            Map.of("size", "1024")
        );
        assertThat(config.cacheSize()).hasValue(1024L);
    }

    @Test
    void cacheSizeBoundedWithDefaultSize() {
        final CacheConfig config = new CacheConfig(
            Map.of(),
            1024
        );
        assertThat(config.cacheSize()).hasValue(1024L);
    }

    @Test
    void invalidCacheSize() {
        assertThatThrownBy(() -> new CacheConfig(
            Map.of("size", "-2")
        )).isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value -2 for configuration size: Value must be at least -1");

        assertThatThrownBy(() -> new CacheConfig(
            Map.of(),
            -2
        )).isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value -2 for configuration size: Value must be at least -1");
    }

    @Test
    void cacheSizeUnspecified() {
        assertThatThrownBy(() -> new CacheConfig(
            Map.of()
        )).isInstanceOf(ConfigException.class)
            .hasMessage("Missing required configuration \"size\" which has no default value.");
    }

    @Test
    void cacheRetentionForever() {
        final CacheConfig config = new CacheConfig(
            Map.of(
                "retention.ms", "-1",
                "size", "-1"
            )
        );
        assertThat(config.cacheRetention()).isNotPresent();
    }

    @Test
    void cacheRetentionLimited() {
        final CacheConfig config = new CacheConfig(
            Map.of(
                "retention.ms", "60000",
                "size", "-1"
            )
        );
        assertThat(config.cacheRetention()).hasValue(Duration.ofMillis(60000));
    }

    @Test
    void invalidRetention() {
        assertThatThrownBy(() -> new CacheConfig(
            Map.of(
                "retention.ms", "-2",
                "size", "-1"
            )
        )).isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value -2 for configuration retention.ms: Value must be at least -1");
    }
}
