/*
 * Copyright 2024 Aiven Oy
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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import io.aiven.kafka.tieredstorage.fetch.cache.MemoryChunkCache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

/**
 * Regression tests for the {@code RemoteStorageManager.copyLogSegmentData} SPI contract:
 * <em>every</em> failure must surface as {@link RemoteStorageException}, never an unchecked
 * {@code Error} or other {@code Throwable}. See #820.
 *
 * <p>Background: an {@code Error} (such as {@code OutOfMemoryError} or {@code NoClassDefFoundError})
 * escaping {@code copyLogSegmentData} bypasses Kafka's {@code RLMTask} {@code catch (Exception)}
 * clause and trips the JDK contract of
 * {@code ScheduledThreadPoolExecutor#scheduleWithFixedDelay}, which silently suppresses all
 * further executions of the task. The partition's copy loop then stops permanently until the
 * broker is restarted or leadership transfers, with local on-disk segments accumulating.
 *
 * <p>The fix wraps the upload body in a {@code try/catch (Throwable)} that converts any
 * escaping {@code Throwable} into {@code RemoteStorageException}. Since
 * {@code RemoteStorageException extends Exception}, the broker's existing catch handles it
 * and the schedule keeps ticking.
 */
@ExtendWith(MockitoExtension.class)
class RemoteStorageManagerErrorHandlingTest {

    static final int LOG_SEGMENT_BYTES = 10;
    static final RemoteLogSegmentMetadata REMOTE_LOG_SEGMENT_METADATA =
        new RemoteLogSegmentMetadata(
            new RemoteLogSegmentId(
                new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic", 0)),
                Uuid.randomUuid()),
            1, 100, -1, -1, 1L,
            LOG_SEGMENT_BYTES, Collections.singletonMap(1, 100L));

    LogSegmentData logSegmentData;
    Map<String, Object> configs;

    @BeforeEach
    void setup(@TempDir final Path tmpDir) throws IOException {
        final Path target = tmpDir.resolve("target");
        Files.createDirectories(target);

        configs = Map.of(
            "chunk.size", "123",
            "storage.backend.class",
            "io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorage",
            "storage.root", target.toString(),
            "fetch.chunk.cache.path", tmpDir.resolve("cache").toString(),
            "fetch.chunk.cache.class", MemoryChunkCache.class.getCanonicalName(),
            "fetch.chunk.cache.size", 100 * 1024 * 1024,
            "metrics.recording.level", "DEBUG"
        );

        final Path source = tmpDir.resolve("source");
        Files.createDirectories(source);
        final Path sourceFile = source.resolve("file");
        Files.write(sourceFile, new byte[LOG_SEGMENT_BYTES]);

        final var leaderEpoch = ByteBuffer.wrap(new byte[LOG_SEGMENT_BYTES]);
        logSegmentData = new LogSegmentData(
            sourceFile, sourceFile, sourceFile, Optional.empty(), sourceFile,
            leaderEpoch
        );
    }

    @Test
    void shouldWrapEscapingErrorAsRemoteStorageException() throws Exception {
        // Inject the Throwable class most commonly observed in production: OutOfMemoryError.
        // Before the fix this Error escapes unchanged and wedges the scheduler.
        assertWrappedAsRemoteStorageException(new OutOfMemoryError("injected for test"));
    }

    @Test
    void shouldWrapEscapingNoClassDefFoundErrorAsRemoteStorageException() throws Exception {
        // NoClassDefFoundError is another Error subtype that can escape from inside the
        // multipart-upload state machine if a transitively-loaded class is missing.
        assertWrappedAsRemoteStorageException(new NoClassDefFoundError("injected for test"));
    }

    @Test
    void shouldWrapEscapingRuntimeExceptionAsRemoteStorageException() throws Exception {
        // An unchecked Exception that is NOT a RemoteStorageException must also be wrapped,
        // so callers see a consistent SPI exception type regardless of the failure mode.
        assertWrappedAsRemoteStorageException(new IllegalStateException("injected for test"));
    }

    @Test
    void shouldPassThroughExistingRemoteStorageException() throws Exception {
        // The wrapper must NOT double-wrap when the inner call already raises the contract type.
        final RemoteStorageException original = new RemoteStorageException("already correct");
        try (MockedConstruction<KafkaRemoteStorageManager> ignored = mockConstruction(
                KafkaRemoteStorageManager.class,
                (mock, ctx) -> when(mock.copyLogSegmentData(any(), any(), any())).thenThrow(original))) {
            final RemoteStorageManager rsm = new RemoteStorageManager(Time.SYSTEM);
            rsm.configure(configs);
            assertThatThrownBy(() -> rsm.copyLogSegmentData(REMOTE_LOG_SEGMENT_METADATA, logSegmentData))
                .isSameAs(original);
        }
    }

    private void assertWrappedAsRemoteStorageException(final Throwable injected) {
        try (MockedConstruction<KafkaRemoteStorageManager> ignored = mockConstruction(
                KafkaRemoteStorageManager.class,
                (mock, ctx) -> when(mock.copyLogSegmentData(any(), any(), any())).thenThrow(injected))) {
            final RemoteStorageManager rsm = new RemoteStorageManager(Time.SYSTEM);
            rsm.configure(configs);
            assertThatThrownBy(() -> rsm.copyLogSegmentData(REMOTE_LOG_SEGMENT_METADATA, logSegmentData))
                .isInstanceOf(RemoteStorageException.class)
                .hasCauseInstanceOf(injected.getClass());
        }
    }
}
