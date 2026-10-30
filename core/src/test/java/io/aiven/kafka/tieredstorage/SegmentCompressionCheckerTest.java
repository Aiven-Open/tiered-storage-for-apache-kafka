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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SegmentCompressionCheckerTest {

    private static final int MAGIC_OFFSET = 16;

    @TempDir
    Path dir;

    @Test
    void shouldFailWhenReadingEmptyFile() throws IOException {
        final File file = Files.createFile(dir.resolve("empty.log")).toFile();
        assertThatThrownBy(() -> SegmentCompressionChecker.check(file))
            .isInstanceOf(InvalidRecordBatchException.class)
            .hasMessage("Record batch is null");
    }

    @Test
    void shouldFailWhenReadingInvalidFile() throws IOException {
        final byte[] bytes = fixture("segment-v2-none.log");
        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] == '0') {
                bytes[i] = '1';
            }
        }
        final File file = write("invalid.log", bytes);

        assertThatThrownBy(() -> SegmentCompressionChecker.check(file))
            .isInstanceOf(InvalidRecordBatchException.class)
            .hasMessage("Failed to read and validate first batch");
    }

    @Test
    void shouldFailWhenFirstBatchIsTruncated() throws IOException {
        final byte[] bytes = fixture("segment-v2-none.log");
        final File file = write("truncated.log", Arrays.copyOf(bytes, bytes.length - 1));

        assertThatThrownBy(() -> SegmentCompressionChecker.check(file))
            .isInstanceOf(InvalidRecordBatchException.class)
            .hasMessage("Record batch is null");
    }

    @Test
    void shouldFailWhenMagicIsUnknown() throws IOException {
        final byte[] bytes = fixture("segment-v2-none.log");
        bytes[MAGIC_OFFSET] = 3;
        final File file = write("unknown-magic.log", bytes);

        assertThatThrownBy(() -> SegmentCompressionChecker.check(file))
            .isInstanceOf(InvalidRecordBatchException.class)
            .hasMessage("Failed to read and validate first batch");
    }

    @ParameterizedTest
    @CsvSource({
        "segment-v2-none.log,false",
        "segment-v2-gzip.log,true",
        "segment-v2-snappy.log,true",
        "segment-v2-lz4.log,true",
        "segment-v2-zstd.log,true",
        "segment-v1-none.log,false",
        "segment-v1-gzip.log,true",
        "segment-v0-none.log,false",
        "segment-v0-gzip.log,true"
    })
    void shouldReturnCompressedWhenFirstBatchIsCompressed(final String fixture, final boolean result)
        throws InvalidRecordBatchException, IOException {
        final File file = write("segment.log", fixture(fixture));

        assertThat(SegmentCompressionChecker.check(file)).isEqualTo(result);
    }

    @ParameterizedTest
    @CsvSource({"none,zstd,false", "zstd,none,true"})
    void shouldReturnCompressedWhenCompressionChanges(final String firstCompressionType,
                                                      final String nextCompressionType,
                                                      final boolean result)
        throws InvalidRecordBatchException, IOException {
        final byte[] first = fixture("segment-v2-" + firstCompressionType + ".log");
        final byte[] next = fixture("segment-v2-" + nextCompressionType + ".log");
        final byte[] bytes = Arrays.copyOf(first, first.length + next.length);
        System.arraycopy(next, 0, bytes, first.length, next.length);
        final File file = write("segment.log", bytes);

        assertThat(SegmentCompressionChecker.check(file)).isEqualTo(result);
    }

    private File write(final String name, final byte[] bytes) throws IOException {
        return Files.write(dir.resolve(name), bytes).toFile();
    }

    private static byte[] fixture(final String name) throws IOException {
        try (final InputStream in = SegmentCompressionCheckerTest.class.getResourceAsStream("/segments/" + name)) {
            assertThat(in).as("fixture " + name).isNotNull();
            return in.readAllBytes();
        }
    }
}
