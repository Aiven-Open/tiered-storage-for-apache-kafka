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

package io.aiven.kafka.tieredstorage.fetch.cache;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;

import io.aiven.kafka.tieredstorage.fetch.FetchManager;
import io.aiven.kafka.tieredstorage.fetch.FetchPartKey;

import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Weigher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryFetchCache extends FetchCache<byte[]> {
    private static final Logger log = LoggerFactory.getLogger(InMemoryFetchCache.class);

    public InMemoryFetchCache(final FetchManager fetchManager) {
        super(fetchManager);
    }

    @Override
    public InputStream readCachedPartContent(final byte[] cachedChunk) {
        return new ByteArrayInputStream(cachedChunk);
    }

    @Override
    public byte[] cachePartContent(final FetchPartKey fetchPartKey, final InputStream chunk) throws IOException {
        try (chunk) {
            return chunk.readAllBytes();
        }
    }

    @Override
    public RemovalListener<FetchPartKey, byte[]> removalListener() {
        return (key, content, cause) -> log.debug("Deleted cached value for key {} from cache."
                + " The reason of the deletion is {}", key, cause);
    }

    @Override
    public Weigher<FetchPartKey, byte[]> weigher() {
        return (key, value) -> value.length;
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        final FetchCacheConfig config = new FetchCacheConfig(new ConfigDef(), configs);
        this.cache = buildCache(config);
    }
}
