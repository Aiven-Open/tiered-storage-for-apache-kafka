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

package io.aiven.kafka.tieredstorage.benchs.cache;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import io.aiven.kafka.tieredstorage.fetch.ChunkManager;
import io.aiven.kafka.tieredstorage.fetch.cache.ChunkCache;
import io.aiven.kafka.tieredstorage.fetch.cache.DirectMemoryChunkCache;
import io.aiven.kafka.tieredstorage.fetch.cache.DiskChunkCache;
import io.aiven.kafka.tieredstorage.fetch.cache.MemoryChunkCache;

public final class CacheBenchUtils {

    private CacheBenchUtils() {
    }

    public static ChunkCache<?> createCache(final String type, final ChunkManager cm,
                                            final int chunkSize, final long cacheSize,
                                            final Path diskPath) throws Exception {
        final ChunkCache<?> c = switch (type) {
            case "memory" -> new MemoryChunkCache(cm);
            case "refcount-direct" -> new DirectMemoryChunkCache(cm);
            case "disk" -> new DiskChunkCache(cm);
            default -> throw new IllegalArgumentException("Unknown cache type: " + type);
        };

        final Map<String, String> config = new HashMap<>();
        config.put("size", String.valueOf(cacheSize));
        config.put("retention.ms", "-1");
        // Used only by DirectMemoryChunkCache to size its DirectByteBufferPool (bufferSize and
        // maxPoolSize = cacheSize / bufferSize). Other cache types ignore this key.
        config.put("chunk.size", String.valueOf(chunkSize));
        config.put("thread.pool.size", String.valueOf(Runtime.getRuntime().availableProcessors() * 2));

        if ("disk".equals(type)) {
            final Path dir = diskPath != null ? diskPath : Files.createTempDirectory("bench-cache");
            config.put("path", dir.toString());
            if (diskPath == null) {
                Runtime.getRuntime().addShutdownHook(new Thread(() -> deleteRecursively(dir)));
            }
        }

        c.configure(config);
        return c;
    }

    /** Best-effort recursive delete of a directory tree; ignores failures. */
    public static void deleteRecursively(final Path dir) {
        try {
            Files.walk(dir)
                .sorted(Comparator.reverseOrder())
                .forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (final Exception ignored) {
                        // best-effort cleanup
                    }
                });
        } catch (final Exception ignored) {
            // best-effort cleanup
        }
    }

    public static long totalGcCount() {
        return ManagementFactory.getGarbageCollectorMXBeans().stream()
            .mapToLong(GarbageCollectorMXBean::getCollectionCount).sum();
    }

    public static long totalGcTimeMs() {
        return ManagementFactory.getGarbageCollectorMXBeans().stream()
            .mapToLong(GarbageCollectorMXBean::getCollectionTime).sum();
    }
}
