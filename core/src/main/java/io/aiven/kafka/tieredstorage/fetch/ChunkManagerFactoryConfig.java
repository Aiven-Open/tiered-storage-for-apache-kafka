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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import io.aiven.kafka.tieredstorage.config.validators.Subclass;
import io.aiven.kafka.tieredstorage.fetch.cache.ChunkCache;

public class ChunkManagerFactoryConfig extends AbstractConfig {

    protected static final String FETCH_CHUNK_CACHE_PREFIX = "fetch.chunk.cache.";
    public static final String FETCH_CHUNK_CACHE_CONFIG = FETCH_CHUNK_CACHE_PREFIX + "class";
    private static final String FETCH_CHUNK_CACHE_DOC = "The fetch chunk cache implementation";

    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef();

        CONFIG.define(
            FETCH_CHUNK_CACHE_CONFIG,
            ConfigDef.Type.CLASS,
            null,
            Subclass.of(ChunkCache.class),
            ConfigDef.Importance.MEDIUM,
            FETCH_CHUNK_CACHE_DOC
        );
    }

    public ChunkManagerFactoryConfig(final Map<?, ?> originals) {
        super(CONFIG, originals);
    }

    @SuppressWarnings("unchecked")
    public Class<ChunkCache<?>> cacheClass() {
        return (Class<ChunkCache<?>>) getClass(FETCH_CHUNK_CACHE_CONFIG);
    }
}
