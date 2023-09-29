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

import io.aiven.kafka.tieredstorage.fetch.cache.DiskBasedFetchCache;
import io.aiven.kafka.tieredstorage.fetch.cache.FetchCache;
import io.aiven.kafka.tieredstorage.fetch.cache.InMemoryFetchCache;

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
class FetchManagerFactoryTest {

    FetchManagerFactory fetchManagerFactory = new FetchManagerFactory();

    public static Stream<Arguments> cachingChunkManagers() {
        return Stream.of(
            arguments(InMemoryFetchCache.class),
            arguments(DiskBasedFetchCache.class)
        );
    }

    @Test
    void defaultChunkManager() {
        fetchManagerFactory.configure(Map.of());
        final FetchManager fetchManager = fetchManagerFactory.initChunkManager(null, null);
        assertThat(fetchManager).isInstanceOf(DefaultFetchManager.class);
    }

    @ParameterizedTest
    @MethodSource("cachingChunkManagers")
    void cachingChunkManagers(final Class<FetchCache<?>> cls) {
        fetchManagerFactory.configure(Map.of(
                "fetch.cache.class", cls,
                "fetch.cache.size", 10,
                "fetch.cache.retention.ms", 10,
                "other.config.x", 10
            )
        );
        try (final MockedConstruction<?> ignored = mockConstruction(cls)) {
            final FetchManager fetchManager = fetchManagerFactory.initChunkManager(null, null);
            assertThat(fetchManager).isInstanceOf(cls);
            verify((FetchCache<?>) fetchManager).configure(Map.of(
                "class", cls,
                "size", 10,
                "retention.ms", 10
            ));
        }
    }

    @Test
    void failedInitialization() {
        fetchManagerFactory.configure(Map.of("fetch.cache.class", InMemoryFetchCache.class));
        try (final MockedConstruction<?> ignored = mockConstruction(InMemoryFetchCache.class,
            (cachingChunkManager, context) -> {
                throw new InvocationTargetException(null);
            })) {
            assertThatThrownBy(() -> fetchManagerFactory.initChunkManager(null, null))
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(ReflectiveOperationException.class);
        }
    }
}
