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

import org.apache.kafka.common.Configurable;

import io.aiven.kafka.tieredstorage.config.ChunkManagerFactoryConfig;
import io.aiven.kafka.tieredstorage.fetch.cache.ChunkCache;
import io.aiven.kafka.tieredstorage.security.AesEncryptionProvider;
import io.aiven.kafka.tieredstorage.storage.ObjectFetcher;

public class ChunkManagerFactory implements Configurable {
    private ChunkManagerFactoryConfig config;

    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = new ChunkManagerFactoryConfig(configs);
    }

    public ChunkManager initChunkManager(final ObjectFetcher fileFetcher,
                                         final AesEncryptionProvider aesEncryptionProvider) {
        final DefaultChunkManager defaultChunkManager = new DefaultChunkManager(fileFetcher, aesEncryptionProvider);
        if (config.cacheClass() != null) {
            try {
                final ChunkCache<?> chunkCache = config
                    .cacheClass()
                    .getDeclaredConstructor(ChunkManager.class)
                    .newInstance(defaultChunkManager);
                chunkCache.configure(config.originalsWithPrefix(ChunkManagerFactoryConfig.FETCH_CHUNK_CACHE_PREFIX));
                return chunkCache;
            } catch (final ReflectiveOperationException e) {
                throw new RuntimeException(e);
            }
        } else {
            return defaultChunkManager;
        }
    }
}
