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

package io.aiven.kafka.tieredstorage.cache;

import java.io.InputStream;
import java.util.Optional;

import org.apache.kafka.common.Configurable;

import io.aiven.kafka.tieredstorage.ChunkKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

/**
 * A chunk cache.
 *
 * <p>A pretty abstract interface for the chunk cache.
 *
 * <p>Getting is straightforward.
 *
 * <p>Storing is a bit more complex.
 * To cater for use cases where the cache is backed by relatively slow and non-atomic storage
 * (primarily, a file system), the storing is split into two steps.
 * <ol>
 *     <li>In the first step represented by {@link #storeTemporarily(byte[])}, a chunk is stored in
 *     some temporary storage (e.g. a temp directory for a file system-backed cache). This operation doesn't
 *     need synchronization, many of them could be performed concurrently, and it's OK if it takes relatively long.</li>
 *     <li>In the second step represented by {@link #store(String, ChunkKey)}, a temporarily stored chunk is added
 *     to the cache properly, which needs synchronization. For example, for a file system-backed cache this could be
 *     file renaming, which is faster than writing and is atomic.
 *     </li>
 * </ol>
 *
 * <p>By this separation, the atomicity of adding chunks to cache is achieved and potential lock contention is lowered.
 */
public interface ChunkCache extends Configurable {
    /**
     * Gets a cached chunk if present.
     * @return the requested chunk if present; empty otherwise.
     */
    Optional<InputStream> get(ChunkKey chunkKey) throws StorageBackendException;

    /**
     * Stores a chunk temporarily.
     *
     * <p>This operation doesn't require synchronization, many can be run concurrently.
     *
     * <p>To avoid conflicts, the temporary ID must be unique.
     *
     * <p>See the class' Javadoc for the details.
     *
     * @return temporary ID (e.g. a file name).
     */
    String storeTemporarily(byte[] chunk) throws StorageBackendException;

    /**
     * Stores permanently a chunk that was stored temporarily previously.
     *
     * <p>This operation requires synchronization.
     *
     * <p>See the class' Javadoc for the details.
     */
    void store(String tempId, ChunkKey chunkKey) throws StorageBackendException;
}
