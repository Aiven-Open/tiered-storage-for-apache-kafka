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
import java.util.Optional;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

public class ChunkCacheConfig extends AbstractConfig {
    private static final String CACHE_SIZE_CONFIG = "size";
    private static final String CACHE_SIZE_DOC = "Cache size in bytes, where \"-1\" represents unbounded cache";
    private static final String CACHE_RETENTION_CONFIG = "retention.ms";
    private static final String CACHE_RETENTION_DOC = "Cache retention time ms, "
            + "where \"-1\" represents infinite retention";
    private static final long DEFAULT_CACHE_RETENTION_MS = 600_000;

    private static final String CACHE_PREFETCHING_SIZE_CONFIG = "prefetching.bytes";
    private static final String CACHE_PREFETCHING_SIZE_DOC =
        "The amount of data that should be eagerly prefetched and cached";

    private static final int CACHE_PREFETCHING_SIZE_DEFAULT = 0; //TODO find out what it should be

    private static ConfigDef addCacheConfigs(final ConfigDef configDef) {
        configDef.define(
                CACHE_SIZE_CONFIG,
                ConfigDef.Type.LONG,
                NO_DEFAULT_VALUE,
                ConfigDef.Range.between(-1L, Long.MAX_VALUE),
                ConfigDef.Importance.MEDIUM,
                CACHE_SIZE_DOC
        );
        configDef.define(
                CACHE_RETENTION_CONFIG,
                ConfigDef.Type.LONG,
                DEFAULT_CACHE_RETENTION_MS,
                ConfigDef.Range.between(-1L, Long.MAX_VALUE),
                ConfigDef.Importance.MEDIUM,
                CACHE_RETENTION_DOC
        );
        configDef.define(
                CACHE_PREFETCHING_SIZE_CONFIG,
                ConfigDef.Type.INT,
                CACHE_PREFETCHING_SIZE_DEFAULT,
                ConfigDef.Range.between(0, Integer.MAX_VALUE),
                ConfigDef.Importance.MEDIUM,
                CACHE_PREFETCHING_SIZE_DOC
        );
        return configDef;
    }

    public ChunkCacheConfig(final ConfigDef configDef, final Map<String, ?> props) {
        super(addCacheConfigs(configDef), props);
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

    public int cachePrefetchingSize() {
        return getInt(CACHE_PREFETCHING_SIZE_CONFIG);
    }
}
