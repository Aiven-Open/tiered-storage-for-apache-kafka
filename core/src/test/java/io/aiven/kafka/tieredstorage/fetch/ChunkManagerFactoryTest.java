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

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.stream.Stream;

import io.aiven.kafka.tieredstorage.fetch.cache.ChunkCache;
import io.aiven.kafka.tieredstorage.fetch.cache.DiskChunkCache;
import io.aiven.kafka.tieredstorage.fetch.cache.MemoryChunkCache;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class ChunkManagerFactoryTest {

    ChunkManagerFactory chunkManagerFactory = new ChunkManagerFactory();

    public static Stream<Arguments> cachingChunkManagers() {
        return Stream.of(
            arguments(MemoryChunkCache.class),
            arguments(DiskChunkCache.class)
        );
    }

    @Test
    void defaultChunkManager() {
        chunkManagerFactory.configure(Map.of());
        final ChunkManager chunkManager = chunkManagerFactory.initChunkManager(null, null);
        assertThat(chunkManager).isInstanceOf(DefaultChunkManager.class);
    }

    @ParameterizedTest
    @MethodSource("cachingChunkManagers")
    void cachingChunkManagers(final Class<ChunkCache<?>> cls) {
        chunkManagerFactory.configure(Map.of(
                "fetch.chunk.cache.class", cls,
                "fetch.chunk.cache.size", 10,
                "fetch.chunk.cache.retention.ms", 10,
                "other.config.x", 10
            )
        );
        try (final MockedConstruction<?> ignored = mockConstruction(cls)) {
            final ChunkManager chunkManager = chunkManagerFactory.initChunkManager(null, null);
            assertThat(chunkManager).isInstanceOf(cls);
            verify((ChunkCache<?>) chunkManager).configure(Map.of(
                "class", cls,
                "size", 10,
                "retention.ms", 10
            ));
        }
    }

    @Test
    void failedInitialization() {
        chunkManagerFactory.configure(Map.of("fetch.chunk.cache.class", MemoryChunkCache.class));
        try (final MockedConstruction<?> ignored = mockConstruction(MemoryChunkCache.class,
            (cachingChunkManager, context) -> {
                throw new InvocationTargetException(null);
            })) {
            assertThatThrownBy(() -> chunkManagerFactory.initChunkManager(null, null))
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(ReflectiveOperationException.class);
        }
    }
}
