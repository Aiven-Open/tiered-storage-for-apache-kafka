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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import io.aiven.kafka.tieredstorage.chunkmanager.cache.ChunkCache;
import io.aiven.kafka.tieredstorage.chunkmanager.cache.NoOpChunkCache;
import io.aiven.kafka.tieredstorage.config.validators.Subclass;

public class ChunkManagerFactoryConfig extends AbstractConfig {

    protected static final String CHUNK_CACHE_PREFIX = "chunk.cache.";
    public static final String CHUNK_CACHE_CONFIG = CHUNK_CACHE_PREFIX + "class";
    private static final String CHUNK_CACHE_DOC = "The chunk cache implementation";

    private static final String CACHE_PREFETCH_MAX_SIZE_CONFIG = CHUNK_CACHE_PREFIX + "prefetch.max.size";
    private static final String CACHE_PREFETCH_MAX_SIZE_DOC =
        "The amount of data that should be eagerly prefetched and cached";
    private static final int CACHE_PREFETCHING_SIZE_DEFAULT = 0; //TODO find out what it should be

    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef();

        CONFIG.define(
            CHUNK_CACHE_CONFIG,
            ConfigDef.Type.CLASS,
            NoOpChunkCache.class,
            Subclass.of(ChunkCache.class),
            ConfigDef.Importance.MEDIUM,
            CHUNK_CACHE_DOC
        );
        CONFIG.define(
            CACHE_PREFETCH_MAX_SIZE_CONFIG,
            ConfigDef.Type.INT,
            CACHE_PREFETCHING_SIZE_DEFAULT,
            ConfigDef.Range.between(0, Integer.MAX_VALUE),
            ConfigDef.Importance.MEDIUM,
            CACHE_PREFETCH_MAX_SIZE_DOC
        );
    }

    public ChunkManagerFactoryConfig(final Map<?, ?> originals) {
        super(CONFIG, originals);
    }

    @SuppressWarnings("unchecked")
    public Class<ChunkCache<?>> cacheClass() {
        return (Class<ChunkCache<?>>) getClass(CHUNK_CACHE_CONFIG);
    }

    public int cachePrefetchingSize() {
        return getInt(CACHE_PREFETCH_MAX_SIZE_CONFIG);
    }
}
