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

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import org.apache.kafka.common.Uuid;

import io.aiven.kafka.tieredstorage.commons.ChunkKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UnboundInMemoryChunkCacheTest {
    static final ChunkKey CHUNK_KEY = new ChunkKey(Uuid.randomUuid(), 0);

    @Test
    void getByNull() {
        final var cache = new UnboundInMemoryChunkCache();
        assertThatThrownBy(() -> cache.get(null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("chunkKey cannot be null");
    }

    @Test
    void getFromEmptyCache() throws IOException {
        final var cache = new UnboundInMemoryChunkCache();
        assertThat(cache.get(CHUNK_KEY)).isEmpty();
    }

    @Test
    void storePermanentlyWithoutStoringTemporarily() throws StorageBackendException {
        final var cache = new UnboundInMemoryChunkCache();
        // Store something, but the name is different.
        cache.storeTemporarily(new byte[1]);

        assertThatThrownBy(() -> cache.store("aaa", CHUNK_KEY))
            .isInstanceOf(StorageBackendException.class)
            .hasMessage("Temporary ID aaa not found in chunk cache");
    }

    @Test
    void storeTemporarilyWithNulls() {
        final var cache = new UnboundInMemoryChunkCache();
        assertThatThrownBy(() -> cache.storeTemporarily(null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("chunk cannot be null");

        assertThatThrownBy(() -> cache.store(null, CHUNK_KEY))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("tempId cannot be null");
        assertThatThrownBy(() -> cache.store("", null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("chunkKey cannot be null");
    }

    @Test
    void storeAndGet() throws Exception {
        final var cache = new UnboundInMemoryChunkCache();
        final var chunk = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
        final String tempFilename = cache.storeTemporarily(chunk);
        cache.store(tempFilename, CHUNK_KEY);
        final Optional<InputStream> result = cache.get(CHUNK_KEY);
        assertThat(result).isPresent();
        assertThat(result.get().readAllBytes()).isEqualTo(chunk);
    }
}
