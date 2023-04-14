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

package io.aiven.kafka.tieredstorage.commons.cache;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import io.aiven.kafka.tieredstorage.commons.ChunkKey;

/**
 * An unbound memory-backed chunk cache.
 *
 * <p>This is not suitable for production use, but is OK for testing.
 */
public class UnboundInMemoryChunkCache implements ChunkCache {
    private final ConcurrentHashMap<ChunkKey, byte[]> permanentStorage = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, byte[]> tempStorage = new ConcurrentHashMap<>();

    @Override
    public Optional<InputStream> get(final ChunkKey chunkKey) throws IOException {
        Objects.requireNonNull(chunkKey, "chunkKey cannot be null");

        return Optional.ofNullable(permanentStorage.get(chunkKey))
            .map(ByteArrayInputStream::new);
    }

    @Override
    public String storeTemporarily(final byte[] chunk) throws IOException {
        Objects.requireNonNull(chunk, "chunk cannot be null");

        final byte[] randomBytes = new byte[20];
        ThreadLocalRandom.current().nextBytes(randomBytes);
        final String tempId = Base64.getEncoder().encodeToString(randomBytes);
        // This should never happen practically if the source of randomness is fair.
        if (tempStorage.putIfAbsent(tempId, chunk) != null) {
            throw new IOException("Temp ID conflict");
        }
        return tempId;
    }

    @Override
    public void store(final String tempId, final ChunkKey chunkKey) throws IOException {
        Objects.requireNonNull(tempId, "tempId cannot be null");
        Objects.requireNonNull(chunkKey, "chunkKey cannot be null");

        final byte[] bytes = tempStorage.get(tempId);
        if (bytes == null) {
            throw new IOException("Temp ID " + tempId + " not found");
        }
        permanentStorage.putIfAbsent(chunkKey, bytes);
        tempStorage.remove(tempId);
    }
}
