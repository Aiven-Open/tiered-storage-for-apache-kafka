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

package io.aiven.kafka.tieredstorage.manifest;

import javax.management.ObjectName;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ForkJoinPool;

import io.aiven.kafka.tieredstorage.manifest.index.FixedSizeChunkIndex;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SegmentManifestProviderTest {
    static final ObjectMapper MAPPER = new ObjectMapper();
    public static final String MANIFEST_KEY = "topic/manifest";

    static {
        MAPPER.registerModule(new Jdk8Module());
    }

    static final String MANIFEST =
        "{\"version\":\"1\","
            + "\"chunkIndex\":{\"type\":\"fixed\",\"originalChunkSize\":100,"
            + "\"originalFileSize\":1000,\"transformedChunkSize\":110,\"finalTransformedChunkSize\":110},"
            + "\"compression\":false}";

    @Mock
    StorageBackend storage;

    SegmentManifestProvider provider;

    @BeforeEach
    void setup() {
        provider = new SegmentManifestProvider(
            Optional.of(1000L), Optional.empty(), storage, MAPPER,
            ForkJoinPool.commonPool(), false);
    }

    @Test
    void unboundedShouldBeCreated() {
        assertThatNoException()
            .isThrownBy(() -> new SegmentManifestProvider(
                Optional.empty(), Optional.of(Duration.ofMillis(1)), storage, MAPPER,
                ForkJoinPool.commonPool(), false));
    }

    @Test
    void withoutRetentionLimitsShouldBeCreated() {
        assertThatNoException()
            .isThrownBy(() -> new SegmentManifestProvider(
                Optional.of(1L), Optional.empty(), storage, MAPPER,
                ForkJoinPool.commonPool(), false));
    }

    @Test
    void shouldReturnAndCache() throws StorageBackendException, IOException {
        final String key = "topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000000023-AAAAAAAAAAAAAAAAAAAAAA.rsm-manifest";
        when(storage.fetch(key))
            .thenReturn(new ByteArrayInputStream(MANIFEST.getBytes()));
        final SegmentManifestV1 expectedManifest = new SegmentManifestV1(
            new FixedSizeChunkIndex(100, 1000, 110, 110),
            false, null
        );
        assertThat(provider.get(key)).isEqualTo(expectedManifest);
        verify(storage).fetch(key);
        assertThat(provider.get(key)).isEqualTo(expectedManifest);
        verifyNoMoreInteractions(storage);
    }

    @Test
    void invalidateCache_jmx() throws Exception {
        provider = new SegmentManifestProvider(
            Optional.of(1000L), Optional.empty(), storage, MAPPER,
            ForkJoinPool.commonPool(), true);

        final String key = "topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000000023-AAAAAAAAAAAAAAAAAAAAAA.rsm-manifest";
        final SegmentManifestV1 expectedManifest = new SegmentManifestV1(
            new FixedSizeChunkIndex(100, 1000, 110, 110),
            false, null
        );
        when(storage.fetch(key))
            .thenReturn(new ByteArrayInputStream(MANIFEST.getBytes()));
        assertThat(provider.get(key)).isEqualTo(expectedManifest);
        verify(storage).fetch(key);

        final var mbeanName = new ObjectName(SegmentManifestCacheManager.MBEAN_NAME);
        final var mbeanServer = ManagementFactory.getPlatformMBeanServer();
        assertThat(mbeanServer.isRegistered(mbeanName)).isTrue();

        final var sizeBefore = provider.cache().estimatedSize();
        assertThat(sizeBefore).isEqualTo(1L);

        mbeanServer.invoke(mbeanName, "clean", new Object[]{}, new String[]{});

        final var sizeAfter = provider.cache().estimatedSize();
        assertThat(sizeAfter).isEqualTo(0L);

        when(storage.fetch(key))
            .thenReturn(new ByteArrayInputStream(MANIFEST.getBytes()));
        assertThat(provider.get(key)).isEqualTo(expectedManifest);
        verify(storage, times(2)).fetch(key);
    }

    @Test
    void shouldPropagateStorageBackendException() throws StorageBackendException {
        when(storage.fetch(anyString()))
            .thenThrow(new StorageBackendException("test"));
        assertThatThrownBy(() -> provider.get(MANIFEST_KEY))
            .isInstanceOf(StorageBackendException.class)
            .hasMessage("test");
    }

    @Test
    void shouldPropagateIOException(@Mock final InputStream isMock) throws StorageBackendException, IOException {
        doAnswer(invocation -> {
            throw new IOException("test");
        }).when(isMock).close();
        when(storage.fetch(anyString()))
            .thenReturn(isMock);
        assertThatThrownBy(() -> provider.get(MANIFEST_KEY))
            .isInstanceOf(IOException.class)
            .hasMessage("test");
    }
}
