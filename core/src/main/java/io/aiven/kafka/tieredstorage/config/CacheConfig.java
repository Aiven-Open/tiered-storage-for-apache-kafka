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
import java.util.Optional;
import java.util.OptionalLong;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

public class CacheConfig extends AbstractConfig {
    private static final String CACHE_SIZE_CONFIG = "size";
    private static final String CACHE_SIZE_DOC = "Cache size in bytes, where \"-1\" represents unbounded cache";
    private static final String CACHE_RETENTION_CONFIG = "retention.ms";
    private static final String CACHE_RETENTION_DOC = "Cache retention time ms, "
        + "where \"-1\" represents infinite retention";
    private static final long DEFAULT_CACHE_RETENTION_MS = 600_000;

    private static ConfigDef addCacheConfigs(
        final OptionalLong maybeDefaultSize,
        final OptionalLong maybeDefaultRetention
    ) {
        final ConfigDef configDef = new ConfigDef();
        Object defaultValue = NO_DEFAULT_VALUE;
        if (maybeDefaultSize.isPresent()) {
            defaultValue = maybeDefaultSize.getAsLong();
        }
        configDef.define(
            CACHE_SIZE_CONFIG,
            ConfigDef.Type.LONG,
            defaultValue,
            ConfigDef.Range.between(-1L, Long.MAX_VALUE),
            ConfigDef.Importance.MEDIUM,
            CACHE_SIZE_DOC
        );
        configDef.define(
            CACHE_RETENTION_CONFIG,
            ConfigDef.Type.LONG,
            maybeDefaultRetention.orElse(DEFAULT_CACHE_RETENTION_MS),
            ConfigDef.Range.between(-1L, Long.MAX_VALUE),
            ConfigDef.Importance.MEDIUM,
            CACHE_RETENTION_DOC
        );
        return configDef;
    }

    private CacheConfig(
        final Map<String, ?> props,
        final OptionalLong defaultSize,
        final OptionalLong defaultRetention
    ) {
        super(addCacheConfigs(defaultSize, defaultRetention), props);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public Optional<Long> cacheSize() {
        final Long rawValue = getLong(CACHE_SIZE_CONFIG);
        if (rawValue == -1) {
            return Optional.empty();
        }
        return Optional.of(rawValue);
    }

    public Optional<Duration> cacheRetention() {
        final Long rawValue = getLong(CACHE_RETENTION_CONFIG);
        if (rawValue == -1) {
            return Optional.empty();
        }
        return Optional.of(Duration.ofMillis(rawValue));
    }

    public static class Builder {
        private OptionalLong maybeCacheSize = OptionalLong.empty();
        private OptionalLong maybeCacheRetention = OptionalLong.empty();

        public Builder withDefaultSize(final long size) {
            this.maybeCacheSize = OptionalLong.of(size);
            return this;
        }

        public Builder withDefaultRetention(final long retention) {
            this.maybeCacheRetention = OptionalLong.of(retention);
            return this;
        }

        public CacheConfig build(final Map<String, ?> configs) {
            return new CacheConfig(configs, maybeCacheSize, maybeCacheRetention);
        }
    }
}
