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

package io.aiven.kafka.tieredstorage;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import io.aiven.kafka.tieredstorage.fetch.ChunkManager;
import io.aiven.kafka.tieredstorage.fetch.manifest.MemorySegmentManifestCache;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

class RemoteStorageManagerTest {
    static final int IV_SIZE = 12;
    static final int SEGMENT_SIZE = 10 * 1024 * 1024;
    static final Uuid TOPIC_ID = Uuid.METADATA_TOPIC_ID;  // string representation: AAAAAAAAAAAAAAAAAAAAAQ
    static final Uuid SEGMENT_ID = Uuid.ZERO_UUID;  // string representation: AAAAAAAAAAAAAAAAAAAAAA
    static final TopicIdPartition TOPIC_ID_PARTITION = new TopicIdPartition(TOPIC_ID, new TopicPartition("topic", 7));
    static final RemoteLogSegmentId REMOTE_SEGMENT_ID = new RemoteLogSegmentId(TOPIC_ID_PARTITION, SEGMENT_ID);
    static final long START_OFFSET = 23L;
    static final RemoteLogSegmentMetadata REMOTE_LOG_METADATA = new RemoteLogSegmentMetadata(
        REMOTE_SEGMENT_ID, START_OFFSET, 2000L,
        0, 0, 0, SEGMENT_SIZE, Map.of(0, 0L));

    RemoteStorageManager rsm;

    @TempDir
    Path tmpDir;
    Path targetDir;

    @BeforeEach
    void init() throws IOException {
        rsm = new RemoteStorageManager();

        targetDir = Path.of(tmpDir.toString(), "target/");
        Files.createDirectories(targetDir);
    }

    @ParameterizedTest
    @MethodSource("provideInterruptionExceptions")
    void fetchSegmentInterruptionWhenGettingManifest(final Class<Exception> outerExceptionClass,
                                                     final Class<Exception> exceptionClass) throws Exception {
        final MemorySegmentManifestCache segmentManifestCache = mock(MemorySegmentManifestCache.class);
        when(segmentManifestCache.get(any())).thenAnswer(invocation -> {
            final Exception innerException = exceptionClass.getDeclaredConstructor().newInstance();
            if (outerExceptionClass != null) {
                throw outerExceptionClass.getDeclaredConstructor(String.class, Throwable.class)
                    .newInstance("", innerException);
            } else {
                throw innerException;
            }
        });

        final var config = Map.of(
            "chunk.size", "1",
            "storage.backend.class", "io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage",
            "storage.root", targetDir.toString()
        );
        rsm.configure(config);
        rsm.setSegmentManifestCache(segmentManifestCache);

        final InputStream inputStream = rsm.fetchLogSegment(REMOTE_LOG_METADATA, 0);
        assertThat(inputStream).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("provideInterruptionExceptions")
    void fetchSegmentInterruptionWhenGettingSegment(final Class<Exception> outerExceptionClass,
                                                    final Class<Exception> exceptionClass) throws Exception {
        // Ensure the manifest exists.
        final ObjectKeyFactory objectKeyFactory = new ObjectKeyFactory("", false);
        writeManifest(objectKeyFactory);

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

        final var config = Map.of(
            "chunk.size", "1",
            "storage.backend.class", "io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage",
            "storage.root", targetDir.toString()
        );
        rsm.configure(config);
        rsm.setChunkManager(chunkManager);

        final InputStream inputStream = rsm.fetchLogSegment(REMOTE_LOG_METADATA, 0);
        assertThat(inputStream).isEmpty();
    }

    static Stream<Arguments> provideInterruptionExceptions() {
        return Stream.of(
            // This is deliberately not tested as this cannot happen (due to the exception checking):
            //arguments(null, InterruptedException.class),
            arguments(null, ClosedByInterruptException.class),
            arguments(RuntimeException.class, InterruptedException.class),
            arguments(RuntimeException.class, ClosedByInterruptException.class),
            arguments(StorageBackendException.class, InterruptedException.class),
            arguments(StorageBackendException.class, ClosedByInterruptException.class)
        );
    }

    @ParameterizedTest
    @MethodSource("provideNonInterruptionExceptions")
    void fetchSegmentNonInterruptionExceptionWhenGettingManifest(
        final Class<Exception> outerExceptionClass,
        final Class<Exception> exceptionClass
    ) throws Exception {
        final MemorySegmentManifestCache segmentManifestCache = mock(MemorySegmentManifestCache.class);
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

        final var config = Map.of(
            "chunk.size", "1",
            "storage.backend.class", "io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage",
            "storage.root", targetDir.toString()
        );
        rsm.configure(config);
        rsm.setSegmentManifestCache(segmentManifestCache);

        assertThatThrownBy(() -> rsm.fetchLogSegment(REMOTE_LOG_METADATA, 0))
            .isInstanceOf(RemoteStorageException.class)
            .hasRootCauseInstanceOf(exceptionClass);
    }

    @ParameterizedTest
    @MethodSource("provideNonInterruptionExceptions")
    void fetchSegmentNonInterruptionExceptionWhenGettingSegment(
        final Class<Exception> outerExceptionClass,
        final Class<Exception> exceptionClass
    ) throws Exception {
        // Ensure the manifest exists.
        final ObjectKeyFactory objectKeyFactory = new ObjectKeyFactory("", false);
        writeManifest(objectKeyFactory);

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

        final var config = Map.of(
            "chunk.size", "1",
            "storage.backend.class", "io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage",
            "storage.root", targetDir.toString()
        );
        rsm.configure(config);
        rsm.setChunkManager(chunkManager);

        assertThatThrownBy(() -> rsm.fetchLogSegment(REMOTE_LOG_METADATA, 0))
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

        final var config = Map.of(
            "chunk.size", "1",
            "storage.backend.class", "io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage",
            "storage.root", targetDir.toString()
        );
        rsm.configure(config);
        rsm = spy(rsm);

        // when first upload fails
        doThrow(IOException.class).when(rsm).uploadSegmentLog(any(), any(), anyBoolean(), any(), any());

        assertThatThrownBy(() -> rsm.copyLogSegmentData(remoteLogSegmentMetadata, logSegmentData))
            .isInstanceOf(RemoteStorageException.class)
            .hasRootCauseInstanceOf(IOException.class);

        // then no files stored in remote
        assertThat(remotePartitionPath).doesNotExist();

        // fallback to real method
        doCallRealMethod().when(rsm).uploadSegmentLog(any(), any(), anyBoolean(), any(), any());

        // when second upload fails
        doThrow(IOException.class).when(rsm).uploadIndexes(any(), any(), any(), any());

        assertThatThrownBy(() -> rsm.copyLogSegmentData(remoteLogSegmentMetadata, logSegmentData))
            .isInstanceOf(RemoteStorageException.class)
            .hasRootCauseInstanceOf(IOException.class);

        // then no files stored in remote
        assertThat(remotePartitionPath).doesNotExist();

        // fallback to real method
        doCallRealMethod().when(rsm).uploadIndexes(any(), any(), any(), any());

        // when third upload fails
        doThrow(IOException.class).when(rsm).uploadManifest(any(), any(), any(), anyBoolean(), any(), any());

        assertThatThrownBy(() -> rsm.copyLogSegmentData(remoteLogSegmentMetadata, logSegmentData))
            .isInstanceOf(RemoteStorageException.class)
            .hasRootCauseInstanceOf(IOException.class);

        // then no files stored in remote
        assertThat(remotePartitionPath).doesNotExist();

        // fallback to real method
        doCallRealMethod().when(rsm).uploadManifest(any(), any(), any(), anyBoolean(), any(), any());

        // when all good
        rsm.copyLogSegmentData(remoteLogSegmentMetadata, logSegmentData);
        assertThat(Files.list(remotePartitionPath)).hasSize(3);
    }

    static Stream<Arguments> provideNonInterruptionExceptions() {
        return Stream.of(
            arguments(null, Exception.class),
            arguments(null, RuntimeException.class),
            arguments(null, StorageBackendException.class),
            arguments(RuntimeException.class, Exception.class),
            arguments(RuntimeException.class, RuntimeException.class),
            arguments(RuntimeException.class, StorageBackendException.class),
            arguments(StorageBackendException.class, Exception.class),
            arguments(StorageBackendException.class, RuntimeException.class),
            arguments(StorageBackendException.class, StorageBackendException.class)
        );
    }

    private void writeManifest(final ObjectKeyFactory objectKeyFactory) throws IOException {
        // Ensure the manifest exists.
        final String manifest =
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
        final Path manifestPath = targetDir.resolve(
            objectKeyFactory.key(REMOTE_LOG_METADATA, ObjectKeyFactory.Suffix.MANIFEST).value());
        Files.createDirectories(manifestPath.getParent());
        Files.writeString(manifestPath, manifest);
    }
}
