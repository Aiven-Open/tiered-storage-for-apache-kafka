/*
 * Copyright 2025 Aiven Oy
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

package io.aiven.kafka.tieredstorage;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig;
import io.aiven.kafka.tieredstorage.fetch.ChunkManager;
import io.aiven.kafka.tieredstorage.fetch.manifest.MemorySegmentManifestCache;
import io.aiven.kafka.tieredstorage.manifest.SegmentIndexV1;
import io.aiven.kafka.tieredstorage.manifest.SegmentIndexesV1;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifestV1;
import io.aiven.kafka.tieredstorage.manifest.index.FixedSizeChunkIndex;
import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class KafkaRemoteStorageManagerTest extends BaseRemoteStorageManagerTest {
    static final SegmentManifestV1 SEGMENT_MANIFEST = new SegmentManifestV1(
        new FixedSizeChunkIndex(100, 1000, 110, 110),
        new SegmentIndexesV1(
            new SegmentIndexV1(0, 1),
            new SegmentIndexV1(1, 1),
            new SegmentIndexV1(2, 1),
            new SegmentIndexV1(3, 1),
            new SegmentIndexV1(4, 1)
        ),
        false,
        null
    );

    @Mock
    Logger log;
    @Mock
    Time time;
    @Mock
    UploadMetricReporter uploadMetricReporter;

    @Mock
    MemorySegmentManifestCache segmentManifestCache;

    @ParameterizedTest
    @MethodSource("provideInterruptionExceptions")
    void fetchSegmentInterruptionWhenGettingSegment(final Class<Exception> outerExceptionClass,
                                                    final Class<Exception> exceptionClass) throws Exception {
        when(segmentManifestCache.get(any())).thenReturn(SEGMENT_MANIFEST);

        final ChunkManager chunkManager = mock(ChunkManager.class);
        when(chunkManager.getChunk(any(), any(), anyInt())).thenAnswer(invocation -> {
            final Exception innerException = exceptionClass.getDeclaredConstructor().newInstance();
            if (outerExceptionClass != null) {
                throw outerExceptionClass.getDeclaredConstructor(String.class, Throwable.class)
                    .newInstance("", innerException);
            } else {
                throw innerException;
            }
        });

        final var config = new RemoteStorageManagerConfig(Map.of(
            "chunk.size", "1",
            "storage.backend.class", "io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage",
            "storage.root", targetDir.toString()
        ));
        final KafkaRemoteStorageManager rsm = new KafkaRemoteStorageManager(log, time, config);
        rsm.setChunkManager(chunkManager);
        rsm.setSegmentManifestCache(segmentManifestCache);

        final InputStream inputStream = rsm.fetchLogSegment(
            REMOTE_LOG_METADATA, BytesRange.of(0, REMOTE_LOG_METADATA.segmentSizeInBytes() - 1));
        assertThat(inputStream).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("provideNonInterruptionExceptions")
    void fetchSegmentNonInterruptionExceptionWhenGettingSegment(
        final Class<Exception> outerExceptionClass,
        final Class<Exception> exceptionClass
    ) throws Exception {
        when(segmentManifestCache.get(any())).thenReturn(SEGMENT_MANIFEST);

        final ChunkManager chunkManager = mock(ChunkManager.class);
        when(chunkManager.getChunk(any(), any(), anyInt())).thenAnswer(invocation -> {
            Exception innerException;
            try {
                innerException = exceptionClass.getDeclaredConstructor().newInstance();
            } catch (final NoSuchMethodException e) {
                innerException = exceptionClass.getDeclaredConstructor(String.class).newInstance("");
            }

            if (outerExceptionClass != null) {
                throw outerExceptionClass.getDeclaredConstructor(String.class, Throwable.class)
                    .newInstance("", innerException);
            } else {
                throw innerException;
            }
        });

        final var config = new RemoteStorageManagerConfig(Map.of(
            "chunk.size", "1",
            "storage.backend.class", "io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage",
            "storage.root", targetDir.toString()
        ));
        final KafkaRemoteStorageManager rsm = new KafkaRemoteStorageManager(log, time, config);
        rsm.setChunkManager(chunkManager);
        rsm.setSegmentManifestCache(segmentManifestCache);

        assertThatThrownBy(() -> rsm.fetchLogSegment(
            REMOTE_LOG_METADATA, BytesRange.of(0, REMOTE_LOG_METADATA.segmentSizeInBytes() - 1)))
            .isInstanceOf(RemoteStorageException.class)
            .hasRootCauseInstanceOf(exceptionClass);
    }

    @Test
    void deleteObjectsWhenUploadFails(
        @TempDir final Path partitionDir
    ) throws IOException, StorageBackendException, RemoteStorageException {
        // given a sample local segment to be uploaded
        final var segmentPath = Files.createFile(partitionDir.resolve("0000.log"));
        final var segmentContent = "test";
        Files.writeString(segmentPath, segmentContent);
        final var indexPath = Files.createFile(partitionDir.resolve("0000.index"));
        final var timeIndexPath = Files.createFile(partitionDir.resolve("0000.timeindex"));
        final var producerSnapshotPath = Files.createFile(partitionDir.resolve("0000.snapshot"));
        final var logSegmentData = new LogSegmentData(
            segmentPath,
            indexPath,
            timeIndexPath,
            Optional.empty(),
            producerSnapshotPath,
            ByteBuffer.wrap("test".getBytes(StandardCharsets.UTF_8))
        );

        final var remoteLogSegmentMetadata = new RemoteLogSegmentMetadata(
            REMOTE_SEGMENT_ID, 0, 1L,
            0, 0, 0, segmentContent.length(), Map.of(0, 0L));

        final var remotePartitionPath = targetDir.resolve(TOPIC_ID_PARTITION.topic() + "-" + TOPIC_ID)
            .resolve(String.valueOf(TOPIC_ID_PARTITION.partition()));

        final var config = new RemoteStorageManagerConfig(Map.of(
            "chunk.size", "1",
            "storage.backend.class", "io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage",
            "storage.root", targetDir.toString()
        ));
        final KafkaRemoteStorageManager rsm = spy(new KafkaRemoteStorageManager(log, time, config));

        // when first upload fails
        doThrow(IOException.class).when(rsm).uploadSegmentLog(any(), any(), anyBoolean(), any(), any(), any());

        assertThatThrownBy(() -> rsm.copyLogSegmentData(remoteLogSegmentMetadata, logSegmentData, uploadMetricReporter))
            .isInstanceOf(RemoteStorageException.class)
            .hasRootCauseInstanceOf(IOException.class);

        // then no files stored in remote
        assertThat(remotePartitionPath).doesNotExist();

        // fallback to real method
        doCallRealMethod().when(rsm).uploadSegmentLog(any(), any(), anyBoolean(), any(), any(), any());

        // when second upload fails
        doThrow(IOException.class).when(rsm).uploadIndexes(any(), any(), any(), any(), any());

        assertThatThrownBy(() -> rsm.copyLogSegmentData(remoteLogSegmentMetadata, logSegmentData, uploadMetricReporter))
            .isInstanceOf(RemoteStorageException.class)
            .hasRootCauseInstanceOf(IOException.class);

        // then no files stored in remote
        assertThat(remotePartitionPath).doesNotExist();

        // fallback to real method
        doCallRealMethod().when(rsm).uploadIndexes(any(), any(), any(), any(), any());

        // when third upload fails
        doThrow(IOException.class).when(rsm).uploadManifest(any(), any(), any(), anyBoolean(), any(), any(), any());

        assertThatThrownBy(() -> rsm.copyLogSegmentData(remoteLogSegmentMetadata, logSegmentData, uploadMetricReporter))
            .isInstanceOf(RemoteStorageException.class)
            .hasRootCauseInstanceOf(IOException.class);

        // then no files stored in remote
        assertThat(remotePartitionPath).doesNotExist();

        // fallback to real method
        doCallRealMethod().when(rsm).uploadManifest(any(), any(), any(), anyBoolean(), any(), any(), any());

        // when all good
        rsm.copyLogSegmentData(remoteLogSegmentMetadata, logSegmentData, uploadMetricReporter);
        assertThat(Files.list(remotePartitionPath)).hasSize(3);
    }

    @ParameterizedTest
    @MethodSource("provideInterruptionExceptions")
    void fetchSegmentInterruptionWhenGettingManifest(final Class<Exception> outerExceptionClass,
                                                     final Class<Exception> exceptionClass) throws Exception {
        when(segmentManifestCache.get(any())).thenAnswer(invocation -> {
            final Exception innerException = exceptionClass.getDeclaredConstructor().newInstance();
            if (outerExceptionClass != null) {
                throw outerExceptionClass.getDeclaredConstructor(String.class, Throwable.class)
                    .newInstance("", innerException);
            } else {
                throw innerException;
            }
        });

        final var config = new RemoteStorageManagerConfig(Map.of(
            "chunk.size", "1",
            "storage.backend.class", "io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage",
            "storage.root", targetDir.toString()
        ));
        final KafkaRemoteStorageManager rsm = spy(new KafkaRemoteStorageManager(log, time, config));
        rsm.setSegmentManifestCache(segmentManifestCache);

        final InputStream inputStream = rsm.fetchLogSegment(REMOTE_LOG_METADATA,
            BytesRange.of(0, REMOTE_LOG_METADATA.segmentSizeInBytes() - 1));
        assertThat(inputStream).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("provideNonInterruptionExceptions")
    void fetchSegmentNonInterruptionExceptionWhenGettingManifest(
        final Class<Exception> outerExceptionClass,
        final Class<Exception> exceptionClass
    ) throws Exception {
        when(segmentManifestCache.get(any())).thenAnswer(invocation -> {
            Exception innerException;
            try {
                innerException = exceptionClass.getDeclaredConstructor().newInstance();
            } catch (final NoSuchMethodException e) {
                innerException = exceptionClass.getDeclaredConstructor(String.class).newInstance("");
            }

            if (outerExceptionClass != null) {
                throw outerExceptionClass.getDeclaredConstructor(String.class, Throwable.class)
                    .newInstance("", innerException);
            } else {
                throw innerException;
            }
        });

        final var config = new RemoteStorageManagerConfig(Map.of(
            "chunk.size", "1",
            "storage.backend.class", "io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage",
            "storage.root", targetDir.toString()
        ));
        final KafkaRemoteStorageManager rsm = spy(new KafkaRemoteStorageManager(log, time, config));
        rsm.setSegmentManifestCache(segmentManifestCache);

        assertThatThrownBy(() -> rsm.fetchLogSegment(REMOTE_LOG_METADATA,
            BytesRange.of(0, REMOTE_LOG_METADATA.segmentSizeInBytes() - 1)))
            .isInstanceOf(RemoteStorageException.class)
            .hasRootCauseInstanceOf(exceptionClass);
    }
}
