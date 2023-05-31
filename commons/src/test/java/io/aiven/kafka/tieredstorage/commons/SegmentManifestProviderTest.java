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

package io.aiven.kafka.tieredstorage.commons;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ForkJoinPool;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import io.aiven.kafka.tieredstorage.commons.manifest.SegmentManifestV1;
import io.aiven.kafka.tieredstorage.commons.manifest.index.FixedSizeChunkIndex;
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SegmentManifestProviderTest {
    static final ObjectKey OBJECT_KEY = new ObjectKey("");

    static final ObjectMapper MAPPER = new ObjectMapper();

    static {
        MAPPER.registerModule(new Jdk8Module());
    }

    static final String MANIFEST =
        "{\"version\":\"1\","
            + "\"chunkIndex\":{\"type\":\"fixed\",\"originalChunkSize\":100,"
            + "\"originalFileSize\":1000,\"transformedChunkSize\":110,\"finalTransformedChunkSize\":110},"
            + "\"compression\":false}";

    static final Uuid TOPIC_ID = Uuid.METADATA_TOPIC_ID;  // string representation: AAAAAAAAAAAAAAAAAAAAAQ
    static final Uuid SEGMENT_ID = Uuid.ZERO_UUID;  // string representation: AAAAAAAAAAAAAAAAAAAAAA
    static final RemoteLogSegmentMetadata REMOTE_LOG_METADATA = new RemoteLogSegmentMetadata(
        new RemoteLogSegmentId(
            new TopicIdPartition(TOPIC_ID, new TopicPartition("topic", 7)),
            SEGMENT_ID
        ),
        23L, 2000L, 0, 0, 0, 1, Map.of(0, 0L));

    @Mock
    StorageBackend storage;

    SegmentManifestProvider provider;

    @BeforeEach
    void setup() {
        provider = new SegmentManifestProvider(
            OBJECT_KEY, Optional.of(1000L), Optional.empty(), storage, MAPPER,
            ForkJoinPool.commonPool());
    }

    @Test
    void unboundedShouldBeCreated() {
        assertThatNoException()
            .isThrownBy(() -> new SegmentManifestProvider(
                OBJECT_KEY, Optional.empty(), Optional.of(Duration.ofMillis(1)), storage, MAPPER,
                ForkJoinPool.commonPool()));
    }

    @Test
    void withoutRetentionLimitsShouldBeCreated() {
        assertThatNoException()
            .isThrownBy(() -> new SegmentManifestProvider(
                OBJECT_KEY, Optional.of(1L), Optional.empty(), storage, MAPPER,
                ForkJoinPool.commonPool()));
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
        assertThat(provider.get(REMOTE_LOG_METADATA)).isEqualTo(expectedManifest);
        verify(storage).fetch(key);
        assertThat(provider.get(REMOTE_LOG_METADATA)).isEqualTo(expectedManifest);
        verifyNoMoreInteractions(storage);
    }

    @Test
    void shouldPropagateStorageBackendException() throws StorageBackendException {
        when(storage.fetch(anyString()))
            .thenThrow(new StorageBackendException("test"));
        assertThatThrownBy(() -> provider.get(REMOTE_LOG_METADATA))
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
        assertThatThrownBy(() -> provider.get(REMOTE_LOG_METADATA))
            .isInstanceOf(IOException.class)
            .hasMessage("test");
    }
}
