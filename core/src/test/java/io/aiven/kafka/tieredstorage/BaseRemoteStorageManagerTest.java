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
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import io.aiven.kafka.tieredstorage.storage.StorageBackendException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.params.provider.Arguments.arguments;

abstract class BaseRemoteStorageManagerTest {
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

    @TempDir
    Path tmpDir;
    Path targetDir;

    @BeforeEach
    void initTmpDirs() throws IOException {
        targetDir = Path.of(tmpDir.toString(), "target/");
        Files.createDirectories(targetDir);
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
}
