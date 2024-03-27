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

package io.aiven.kafka.tieredstorage.fetch.manifest;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;

import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType;

import io.aiven.kafka.tieredstorage.manifest.SegmentIndexesV1;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifestV1;
import io.aiven.kafka.tieredstorage.manifest.index.FixedSizeChunkIndex;
import io.aiven.kafka.tieredstorage.manifest.serde.KafkaTypeSerdeModule;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
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
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MemorySegmentManifestCacheTest {
    static final ObjectMapper MAPPER = new ObjectMapper();
    public static final ObjectKey MANIFEST_KEY = () -> "topic/manifest";

    static {
        MAPPER.registerModule(new Jdk8Module());
        MAPPER.registerModule(KafkaTypeSerdeModule.create());
    }

    static final String MANIFEST =
        "{\"version\":\"1\","
            + "\"chunkIndex\":{\"type\":\"fixed\",\"originalChunkSize\":100,"
            + "\"originalFileSize\":1000,\"transformedChunkSize\":110,\"finalTransformedChunkSize\":110},"
            + "\"segmentIndexes\":{"
            + "\"offset\":{\"position\":0,\"size\":1},"
            + "\"timestamp\":{\"position\":1,\"size\":1},"
            + "\"producerSnapshot\":{\"position\":2,\"size\":1},"
            + "\"leaderEpoch\":{\"position\":3,\"size\":1},"
            + "\"transaction\":{\"position\":4,\"size\":1}"
            + "},"
            + "\"compression\":false}";

    static final SegmentIndexesV1 SEGMENT_INDEXES = SegmentIndexesV1.builder()
        .add(IndexType.OFFSET, 1)
        .add(IndexType.TIMESTAMP, 1)
        .add(IndexType.PRODUCER_SNAPSHOT, 1)
        .add(IndexType.LEADER_EPOCH, 1)
        .add(IndexType.TRANSACTION, 1)
        .build();

    @Mock
    StorageBackend storage;

    MemorySegmentManifestCache provider;

    @BeforeEach
    void setup() {
        final var config = MemorySegmentManifestCache.CONFIG_BUILDER.build(Map.of(
            "size", 1000L
        ));
        provider = new MemorySegmentManifestCache(
            config, storage, MAPPER,
            ForkJoinPool.commonPool());
    }

    @Test
    void unboundedShouldBeCreated() {
        final var config = MemorySegmentManifestCache.CONFIG_BUILDER.build(Map.of(
            "retention", 1L
        ));
        assertThatNoException()
            .isThrownBy(() -> new MemorySegmentManifestCache(
                config, storage, MAPPER,
                ForkJoinPool.commonPool()));
    }

    @Test
    void withoutRetentionLimitsShouldBeCreated() {
        final var config = MemorySegmentManifestCache.CONFIG_BUILDER.build(Map.of(
            "size", 1L
        ));
        assertThatNoException()
            .isThrownBy(() -> new MemorySegmentManifestCache(
                config, storage, MAPPER,
                ForkJoinPool.commonPool()));
    }

    @Test
    void shouldReturnAndCache() throws StorageBackendException, IOException {
        final ObjectKey key =
            () -> "topic-AAAAAAAAAAAAAAAAAAAAAQ/7/00000000000000000023-AAAAAAAAAAAAAAAAAAAAAA.rsm-manifest";
        when(storage.fetch(key))
            .thenReturn(new ByteArrayInputStream(MANIFEST.getBytes()));
        final var chunkIndex = new FixedSizeChunkIndex(100, 1000, 110, 110);
        final var expectedManifest = new SegmentManifestV1(chunkIndex, SEGMENT_INDEXES, false, null, null);
        assertThat(provider.get(key)).isEqualTo(expectedManifest);
        verify(storage).fetch(key);
        assertThat(provider.get(key)).isEqualTo(expectedManifest);
        verifyNoMoreInteractions(storage);
    }

    @Test
    void shouldPropagateStorageBackendException() throws StorageBackendException {
        when(storage.fetch(any()))
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
        when(storage.fetch(any()))
            .thenReturn(isMock);
        assertThatThrownBy(() -> provider.get(MANIFEST_KEY))
            .isInstanceOf(IOException.class)
            .hasMessage("test");
    }

    @Test
    void shouldNotPoisonCacheWithFailedFutures()
        throws StorageBackendException {
        when(storage.fetch(MANIFEST_KEY))
            .thenThrow(new StorageBackendException("test"))
            .thenReturn(new ByteArrayInputStream(MANIFEST.getBytes()));

        assertThatThrownBy(() -> provider.get(MANIFEST_KEY))
            .isInstanceOf(StorageBackendException.class)
            .hasMessage("test");

        final var chunkIndex = new FixedSizeChunkIndex(100, 1000, 110, 110);
        final var expectedManifest = new SegmentManifestV1(chunkIndex, SEGMENT_INDEXES, false, null, null);

        await().atMost(Duration.ofMillis(50))
            .pollInterval(Duration.ofMillis(5))
            .ignoreExceptions()
            .until(() -> provider.get(MANIFEST_KEY).equals(expectedManifest));
    }
}
