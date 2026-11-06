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

package io.aiven.kafka.tieredstorage.fetch.cache;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import io.aiven.kafka.tieredstorage.config.ChunkCacheConfig;
import io.aiven.kafka.tieredstorage.fetch.ChunkKey;
import io.aiven.kafka.tieredstorage.fetch.ChunkManager;

import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Weigher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectMemoryChunkCache extends ChunkCache<RefCountedByteBuffer> {
    private static final Logger log = LoggerFactory.getLogger(DirectMemoryChunkCache.class);

    private static final int DEFAULT_BUFFER_SIZE = 4 * 1024 * 1024;
    private static final long DEFAULT_MAX_POOL_SIZE = 512;

    private DirectByteBufferPool bufferPool;
    private DirectMemoryChunkCacheMetrics poolMetrics;

    public DirectMemoryChunkCache(final ChunkManager chunkManager) {
        super(chunkManager);
    }

    @Override
    public InputStream cachedChunkToInputStream(final RefCountedByteBuffer cachedChunk) {
        cachedChunk.retain();
        return new ByteBufferInputStream(cachedChunk);
    }

    @Override
    public RefCountedByteBuffer cacheChunk(final ChunkKey chunkKey, final InputStream chunk) throws IOException {
        try (chunk) {
            final byte[] bytes = chunk.readAllBytes();
            final int requestedSize = Math.max(bytes.length, bufferPool.getBufferSize());
            final DirectByteBufferPool.PoolGetResult result = bufferPool.get(requestedSize);
            final ByteBuffer directBuffer = result.buffer();
            directBuffer.put(bytes);
            directBuffer.flip();
            poolMetrics.bufferAcquired(directBuffer.capacity(), result.poolHit());
            return new RefCountedByteBuffer(directBuffer, bufferPool);
        }
    }

    @Override
    public RemovalListener<ChunkKey, RefCountedByteBuffer> removalListener() {
        return (key, content, cause) -> {
            if (content != null) {
                content.release();
            }
            log.debug("Evicted key {} from cache, cause: {}", key, cause);
        };
    }

    @Override
    public Weigher<ChunkKey, RefCountedByteBuffer> weigher() {
        return (key, value) -> value.capacity();
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        final ChunkCacheConfig config = new ChunkCacheConfig(configs);

        int bufferSize = DEFAULT_BUFFER_SIZE;
        if (configs.containsKey("chunk.size")) {
            bufferSize = Integer.parseInt(configs.get("chunk.size").toString());
        }

        long maxPoolSize = DEFAULT_MAX_POOL_SIZE;
        if (config.cacheSize().isPresent()) {
            maxPoolSize = config.cacheSize().get() / bufferSize;
        }

        log.info("DirectMemoryChunkCache: bufferSize={}, maxPoolSize={}", bufferSize, maxPoolSize);

        this.bufferPool = new DirectByteBufferPool(bufferSize, maxPoolSize);
        this.cache = buildCache(config);
        this.poolMetrics = new DirectMemoryChunkCacheMetrics(bufferPool);
    }

    public DirectByteBufferPool getBufferPool() {
        return bufferPool;
    }
}
