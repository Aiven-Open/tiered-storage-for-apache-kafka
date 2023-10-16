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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;

import io.aiven.kafka.tieredstorage.chunkmanager.ChunkKey;
import io.aiven.kafka.tieredstorage.chunkmanager.ChunkManager;

import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Weigher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryChunkCache extends ChunkCache<byte[]> {
    private static final Logger log = LoggerFactory.getLogger(InMemoryChunkCache.class);

    public InMemoryChunkCache(final ChunkManager chunkManager) {
        super(chunkManager);
    }

    @Override
    public ByteBuffer cachedChunkToInputStream(final byte[] cachedChunk) {
        return ByteBuffer.wrap(cachedChunk);
    }

    @Override
    public byte[] cacheChunk(final ChunkKey chunkKey, final ByteBuffer chunk) throws IOException {
        return chunk.array();
    }

    @Override
    public RemovalListener<ChunkKey, byte[]> removalListener() {
        return (key, content, cause) -> log.debug("Deleted cached value for key {} from cache."
                + " The reason of the deletion is {}", key, cause);
    }

    @Override
    public Weigher<ChunkKey, byte[]> weigher() {
        return (key, value) -> value.length;
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        final ChunkCacheConfig config = new ChunkCacheConfig(new ConfigDef(), configs);
        this.cache = buildCache(config);
    }
}
