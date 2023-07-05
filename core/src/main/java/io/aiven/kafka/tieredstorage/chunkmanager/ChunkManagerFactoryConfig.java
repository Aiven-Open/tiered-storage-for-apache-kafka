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
import org.apache.kafka.common.config.ConfigException;

import io.aiven.kafka.tieredstorage.chunkmanager.cache.ChunkCache;

public class ChunkManagerFactoryConfig extends AbstractConfig {

    protected static final String CHUNK_CACHE_PREFIX = "chunk.cache.";
    private static final String CHUNK_CACHE_CONFIG = CHUNK_CACHE_PREFIX + "class";
    private static final String CHUNK_CACHE_DOC = "The chunk cache implementation";

    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef();

        CONFIG.define(
                CHUNK_CACHE_CONFIG,
                ConfigDef.Type.CLASS,
                null,
                Subclass.of(ChunkManager.class),
                ConfigDef.Importance.MEDIUM,
                CHUNK_CACHE_DOC
        );
    }

    public ChunkManagerFactoryConfig(final Map<?, ?> originals) {
        super(CONFIG, originals);
    }

    @SuppressWarnings("unchecked")
    public Class<ChunkCache<?>> cacheClass() {
        return (Class<ChunkCache<?>>) getClass(CHUNK_CACHE_CONFIG);
    }

    public static class Subclass implements ConfigDef.Validator {
        private final Class<?> parentClass;

        public static Subclass of(final Class<?> parentClass) {
            return new Subclass(parentClass);
        }

        public Subclass(final Class<?> parentClass) {
            this.parentClass = parentClass;
        }

        @Override
        public void ensureValid(final String name, final Object value) {
            if (value != null && !(parentClass.isAssignableFrom((Class<?>) value))) {
                throw new ConfigException(name + " should be a subclass of " + ChunkCache.class.getCanonicalName());
            }
        }

    }
}
