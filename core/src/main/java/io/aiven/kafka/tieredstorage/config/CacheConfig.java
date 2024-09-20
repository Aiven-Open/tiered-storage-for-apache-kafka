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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

public class CacheConfig extends AbstractConfig {
    private static final String CACHE_SIZE_CONFIG = "size";
    private static final String CACHE_SIZE_DOC = "Cache size in bytes, where \"-1\" represents unbounded cache";
    private static final String CACHE_RETENTION_CONFIG = "retention.ms";
    private static final String CACHE_RETENTION_DOC = "Cache retention time ms, "
        + "where \"-1\" represents infinite retention";
    private static final String CACHE_FETCH_THREAD_POOL_SIZE_CONFIG = "thread.pool.size";
    private static final String CACHE_FETCH_THREAD_POOL_SIZE_DOC = "Size for the thread pool used to "
        + "schedule asynchronous fetching tasks, default to number of processors.";
    private static final String CACHE_FETCH_TIMEOUT_MS_CONFIG = "get.timeout.ms";
    private static final String CACHE_FETCH_TIMEOUT_MS_DOC = "When getting an object from the fetch, "
        + "how long to wait before timing out. Defaults to 10 sec.";

    static final long CACHE_RETENTION_MS_DEFAULT = 600_000;

    private static ConfigDef configDef(
        final ConfigDef configDef,
        final Object defaultSize,
        final long defaultRetentionMs
    ) {
        configDef.define(
            CACHE_SIZE_CONFIG,
            ConfigDef.Type.LONG,
            defaultSize,
            ConfigDef.Range.between(-1L, Long.MAX_VALUE),
            ConfigDef.Importance.MEDIUM,
            CACHE_SIZE_DOC
        );
        configDef.define(
            CACHE_RETENTION_CONFIG,
            ConfigDef.Type.LONG,
            defaultRetentionMs,
            ConfigDef.Range.between(-1L, Long.MAX_VALUE),
            ConfigDef.Importance.MEDIUM,
            CACHE_RETENTION_DOC
        );
        configDef.define(
            CACHE_FETCH_THREAD_POOL_SIZE_CONFIG,
            ConfigDef.Type.INT,
            0,
            ConfigDef.Range.between(0, 1024),
            ConfigDef.Importance.LOW,
            CACHE_FETCH_THREAD_POOL_SIZE_DOC
        );
        configDef.define(
            CACHE_FETCH_TIMEOUT_MS_CONFIG,
            ConfigDef.Type.LONG,
            Duration.ofSeconds(10).toMillis(),
            ConfigDef.Range.between(1, Long.MAX_VALUE),
            ConfigDef.Importance.LOW,
            CACHE_FETCH_TIMEOUT_MS_DOC
        );
        return configDef;
    }

    CacheConfig(
        final ConfigDef configDef,
        final Map<String, ?> props,
        final Object defaultSize,
        final long defaultRetentionMs
    ) {
        super(configDef(configDef, defaultSize, defaultRetentionMs), props);
    }

    public static Builder newBuilder(final Map<String, ?> configs) {
        return new Builder(configs);
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

    public Optional<Integer> threadPoolSize() {
        final Integer rawValue = getInt(CACHE_FETCH_THREAD_POOL_SIZE_CONFIG);
        if (rawValue == 0) {
            return Optional.empty();
        }
        return Optional.of(rawValue);
    }

    public Duration getTimeout() {
        return Duration.ofMillis(getLong(CACHE_FETCH_TIMEOUT_MS_CONFIG));
    }

    public static class Builder {
        private final Map<String, ?> props;
        private long defaultRetentionMs = CACHE_RETENTION_MS_DEFAULT;
        private Object maybeDefaultSize = NO_DEFAULT_VALUE;

        public Builder(final Map<String, ?> props) {
            this.props = props;
        }

        public Builder withDefaultSize(final long defaultSize) {
            this.maybeDefaultSize = defaultSize;
            return this;
        }

        public Builder withDefaultRetentionMs(final long retentionMs) {
            this.defaultRetentionMs = retentionMs;
            return this;
        }

        public CacheConfig build() {
            return new CacheConfig(new ConfigDef(), props, maybeDefaultSize, defaultRetentionMs);
        }
    }
}
