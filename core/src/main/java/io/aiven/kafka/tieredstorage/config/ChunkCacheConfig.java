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

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;

public class ChunkCacheConfig extends CacheConfig {
    private static final String CACHE_PREFETCH_MAX_SIZE_CONFIG = "prefetch.max.size";
    private static final String CACHE_PREFETCH_MAX_SIZE_DOC =
        "The amount of data that should be eagerly prefetched and cached";
    private static final int CACHE_PREFETCHING_SIZE_DEFAULT = 0; //TODO find out what it should be

    public static final ConfigDef configDef(final ConfigDef baseConfig) {
        baseConfig.define(
            CACHE_PREFETCH_MAX_SIZE_CONFIG,
            ConfigDef.Type.INT,
            CACHE_PREFETCHING_SIZE_DEFAULT,
            ConfigDef.Range.between(0, Integer.MAX_VALUE),
            ConfigDef.Importance.MEDIUM,
            CACHE_PREFETCH_MAX_SIZE_DOC
        );
        return CacheConfig.defBuilder(baseConfig)
            .withDefaultRetentionMs(ChunkCacheConfig.CACHE_RETENTION_MS_DEFAULT)
            .build();
    }

    public ChunkCacheConfig(final ConfigDef configDef, final Map<String, ?> props) {
        super(configDef, props);
    }

    public ChunkCacheConfig(final Map<String, ?> props) {
        super(configDef(new ConfigDef()), props);
    }

    public int cachePrefetchingSize() {
        return getInt(CACHE_PREFETCH_MAX_SIZE_CONFIG);
    }
}
