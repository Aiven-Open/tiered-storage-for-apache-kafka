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
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import io.aiven.kafka.tieredstorage.chunkmanager.ChunkKey;
import io.aiven.kafka.tieredstorage.chunkmanager.ChunkManager;

import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Weigher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;

public class DiskBasedChunkCache extends ChunkCache<Path> {

    private static final Logger log = LoggerFactory.getLogger(DiskBasedChunkCache.class);

    private DiskBasedChunkCacheConfig config;

    public DiskBasedChunkCache(final ChunkManager chunkManager) {
        super(chunkManager);
    }

    @Override
    public InputStream cachedChunkToInputStream(final Path cachedChunk) {
        try {
            return Files.newInputStream(cachedChunk);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Writes the chunk to specified directory on disk.
     * To be able to understand if the files are fully written to disk during a potential recovery process,
     * this implementation initially writes them to temporary directory and then atomically moves them
     * to the actual caching directory.
     */
    @Override
    public Path cacheChunk(final ChunkKey chunkKey, final InputStream chunk) throws IOException {
        final Path tempChunkPath = config.tempCachePath()
                .resolve(chunkKey.uuid + "-" + chunkKey.chunkId);
        final Path tempCached = writeToDisk(chunk, tempChunkPath);
        log.debug("Chunk file has been stored to temporary caching directory {}", tempCached);
        final Path cachedChunkPath = config.cachePath()
                .resolve(chunkKey.uuid + "-" + chunkKey.chunkId);
        try {
            final Path newPath = Files.move(tempCached, cachedChunkPath, ATOMIC_MOVE);
            log.debug("Chunk file has been moved to cache directory {}", newPath);
            return newPath;
        } finally {
            // In case of exception during the move, the chunk file should be cleaned from temporary cache directory.
            if (Files.exists(tempCached)) {
                log.error("Failed to move chunk file {} to cache directory from temporary one.", tempCached);
                Files.delete(tempCached);
            }
        }
    }

    private static Path writeToDisk(final InputStream chunk, final Path tempChunkPath) throws IOException {
        try (chunk) {
            return Files.write(tempChunkPath, chunk.readAllBytes());
        }
    }

    @Override
    public RemovalListener<ChunkKey, Path> removalListener() {
        return (key, path, cause) -> {
            try {
                Files.delete(path);
                log.debug("Deleted cached file for key {} with path {} from cache directory."
                        + " The reason of the deletion is {}", key, path, cause);
            } catch (final IOException e) {
                log.error("Failed to delete cached file for key {} with path {} from cache directory."
                              + " The reason of the deletion is {}", key, path, cause, e);
            }
        };
    }

    @Override
    public Weigher<ChunkKey, Path> weigher() {
        return (key, value) -> {
            try {
                final var fileSize = Files.size(value);
                if (fileSize <= Integer.MAX_VALUE) {
                    return (int) fileSize;
                } else {
                    log.warn(
                            "Cache size calculation have been inaccurate "
                                    + "because size of a cached file was bigger than Integer.MAX_VALUE. "
                                    + "This should never happen.");
                    return Integer.MAX_VALUE;
                }
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        this.config = new DiskBasedChunkCacheConfig(configs);
        this.cache = buildCache(config);
    }
}
