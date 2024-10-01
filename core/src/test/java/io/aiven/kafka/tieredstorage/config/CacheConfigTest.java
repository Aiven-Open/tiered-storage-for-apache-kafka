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
        final CacheConfig config = new CacheConfig(CacheConfig.defBuilder().build(), Map.of("size", "-1"));

        assertThat(config.cacheSize()).isNotPresent();
        assertThat(config.cacheRetention()).hasValue(Duration.ofMinutes(10));
        // other defaults
        assertThat(config.threadPoolSize()).isEmpty();
        assertThat(config.getTimeout()).hasSeconds(10);
    }

    @Test
    void cacheUnboundedWithDefaultSize() {
        final CacheConfig config = new CacheConfig(
            CacheConfig.defBuilder().withDefaultSize(-1L).build(),
            Map.of()
        );

        assertThat(config.cacheSize()).isNotPresent();
        assertThat(config.cacheRetention()).hasValue(Duration.ofMinutes(10));
        // other defaults
        assertThat(config.threadPoolSize()).isEmpty();
        assertThat(config.getTimeout()).hasSeconds(10);
    }

    @Test
    void cacheSizeBounded() {
        final CacheConfig config = new CacheConfig(
            CacheConfig.defBuilder().build(),
            Map.of("size", "1024")
        );
        assertThat(config.cacheSize()).hasValue(1024L);
    }

    @Test
    void cacheSizeBoundedWithDefaultSize() {
        final CacheConfig config = new CacheConfig(
            CacheConfig.defBuilder().withDefaultSize(1024L).build(),
            Map.of()
        );
        assertThat(config.cacheSize()).hasValue(1024L);
    }

    @Test
    void cacheSizeBoundedWithDefaultRetentionMs() {
        final CacheConfig config = new CacheConfig(
            CacheConfig.defBuilder()
                .withDefaultSize(-1L)
                .withDefaultRetentionMs(3_600_000L)
                .build(),
            Map.of()
        );
        assertThat(config.cacheRetention()).hasValue(Duration.ofHours(1));
    }

    @Test
    void invalidCacheSize() {
        assertThatThrownBy(() -> new CacheConfig(CacheConfig.defBuilder().build(), Map.of("size", "-2")))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value -2 for configuration size: Value must be at least -1");

        assertThatThrownBy(() -> new CacheConfig(CacheConfig.defBuilder().withDefaultSize(-2L).build(), Map.of()))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value -2 for configuration size: Value must be at least -1");
    }

    @Test
    void cacheSizeUnspecified() {
        assertThatThrownBy(() -> new CacheConfig(CacheConfig.defBuilder().build(), Map.of()))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Missing required configuration \"size\" which has no default value.");
    }

    @Test
    void cacheRetentionForever() {
        final Map<String, String> configs = Map.of(
            "retention.ms", "-1",
            "size", "-1"
        );
        final CacheConfig config = new CacheConfig(CacheConfig.defBuilder().build(), configs);
        assertThat(config.cacheRetention()).isNotPresent();
    }

    @Test
    void cacheRetentionLimited() {
        final Map<String, String> configs = Map.of(
            "retention.ms", "60000",
            "size", "-1"
        );
        final CacheConfig config = new CacheConfig(CacheConfig.defBuilder().build(), configs);
        assertThat(config.cacheRetention()).hasValue(Duration.ofMillis(60000));
    }

    @Test
    void invalidRetention() {
        final Map<String, String> configs = Map.of(
            "retention.ms", "-2",
            "size", "-1"
        );
        assertThatThrownBy(() -> new CacheConfig(CacheConfig.defBuilder().build(), configs))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value -2 for configuration retention.ms: Value must be at least -1");
    }

    @Test
    void setOtherConfigs() {
        final CacheConfig config = new CacheConfig(
            CacheConfig.defBuilder()
                .withDefaultSize(-1L)
                .build(), 
            Map.of(
                "thread.pool.size", 16,
                "get.timeout.ms", Duration.ofSeconds(20).toMillis()
            )
        );
        assertThat(config.threadPoolSize()).hasValue(16);
        assertThat(config.getTimeout()).hasSeconds(20);
    }
}
