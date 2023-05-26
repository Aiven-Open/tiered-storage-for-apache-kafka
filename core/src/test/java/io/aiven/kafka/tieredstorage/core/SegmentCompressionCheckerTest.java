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

package io.aiven.kafka.tieredstorage.core;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.TimestampType;

import org.assertj.core.util.Files;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SegmentCompressionCheckerTest {

    @TempDir
    Path dir;

    @Test
    void shouldFailWhenReadingEmptyFile() {
        assertThatThrownBy(() -> SegmentCompressionChecker.check(Files.newTemporaryFile()))
            .isInstanceOf(InvalidRecordBatchException.class)
            .hasMessage("Record batch is null");
    }

    @Test
    void shouldFailWhenReadingInvalidFile() throws IOException {
        // Given a valid record batch
        final Path path = dir.resolve("invalid.log");
        final File file = Files.newFile(path.toString());
        try (final FileRecords records = FileRecords.open(file, false, 100000, true);
             final MemoryRecordsBuilder builder = MemoryRecords.builder(
                 ByteBuffer.allocate(1024),
                 CompressionType.NONE,
                 TimestampType.CREATE_TIME,
                 0)) {
            builder.append(0L, "key-0".getBytes(), "value-0".getBytes());
            records.append(builder.build());
        }
        // When messing with the record content
        final byte[] bytes = java.nio.file.Files.readAllBytes(path);
        final byte[] output = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == "0".getBytes()[0]) {
                output[i] = "1".getBytes()[0];
            } else {
                output[i] = bytes[i];
            }
        }
        java.nio.file.Files.write(path, output);
        // Then
        assertThatThrownBy(() -> SegmentCompressionChecker.check(file))
            .isInstanceOf(InvalidRecordBatchException.class)
            .hasMessage("Failed to read and validate first batch");
    }

    @ParameterizedTest
    @CsvSource({"NONE,false", "ZSTD,true"})
    void shouldReturnCompressedWhenEnabled(final CompressionType compressionType, final boolean result)
        throws InvalidRecordBatchException, IOException {
        final File file = dir.resolve("segment.log").toFile();
        try (final FileRecords records = FileRecords.open(file, false, 100000, true);
             final MemoryRecordsBuilder builder = MemoryRecords.builder(
                 ByteBuffer.allocate(1024),
                 compressionType,
                 TimestampType.CREATE_TIME,
                 0)) {
            builder.append(0L, "key-0".getBytes(), "value-0".getBytes());
            records.append(builder.build());
        }

        final boolean requires = SegmentCompressionChecker.check(file);
        assertThat(requires).isEqualTo(result);
    }

    @ParameterizedTest
    @CsvSource({"NONE,ZSTD,false", "ZSTD,NONE,true"})
    void shouldReturnCompressedWhenCompressionChanges(final CompressionType firstCompressionType,
                                                      final CompressionType nextCompressionType,
                                                      final boolean result)
        throws InvalidRecordBatchException, IOException {
        final File file = dir.resolve("segment.log").toFile();
        try (final FileRecords records = FileRecords.open(file, false, 100000, true);
             final MemoryRecordsBuilder b1 = MemoryRecords.builder(
                 ByteBuffer.allocate(1024),
                 firstCompressionType,
                 TimestampType.CREATE_TIME,
                 0);
             final MemoryRecordsBuilder b2 = MemoryRecords.builder(
                 ByteBuffer.allocate(1024),
                 nextCompressionType,
                 TimestampType.CREATE_TIME,
                 0)) {
            b1.append(0L, "key-0".getBytes(), "value-0".getBytes());
            records.append(b1.build());
            b2.append(0L, "key-0".getBytes(), "value-0".getBytes());
            records.append(b2.build());
        }

        final boolean requires = SegmentCompressionChecker.check(file);
        assertThat(requires).isEqualTo(result);
    }
}
