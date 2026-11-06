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

package io.aiven.kafka.tieredstorage.iceberg.kafka;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Checks the plugin's log reader on a segment made of batches that Kafka itself built, one after another.
 */
class KafkaLogReaderTest {
    @TempDir
    Path dir;

    private static byte[] segment(final List<KafkaTestData.KafkaBuiltBatch> batches) throws IOException {
        final ByteArrayOutputStream segment = new ByteArrayOutputStream();
        for (final KafkaTestData.KafkaBuiltBatch batch : batches) {
            segment.write(batch.bytes());
        }
        return segment.toByteArray();
    }

    private List<KafkaBatch> read(final Path file) throws IOException {
        final List<KafkaBatch> batches = new ArrayList<>();
        try (final KafkaLogReader reader = KafkaLogReader.open(file)) {
            reader.forEachRemaining(batches::add);
        }
        return batches;
    }

    @Test
    void readsBatchesOfEveryCodec() throws IOException {
        final List<KafkaTestData.KafkaBuiltBatch> built = KafkaTestData.kafkaBuiltBatches();
        final Path file = Files.write(dir.resolve("00000000000000000000.log"), segment(built));

        assertThat(read(file))
            .usingRecursiveComparison()
            .withEqualsForType(ByteBuffer::equals, ByteBuffer.class)
            .isEqualTo(built.stream().map(batch -> batch.testCase().expectedBatch()).toList());
    }

    @Test
    void ignoresAPartialBatchAtTheEnd() throws IOException {
        final List<KafkaTestData.KafkaBuiltBatch> built = KafkaTestData.kafkaBuiltBatches();
        final List<KafkaBatch> expected = built.stream().map(batch -> batch.testCase().expectedBatch()).toList();
        final byte[] segment = segment(built);

        // The file can end anywhere: cut it in the middle of the first batches, where the headers are,
        // and at the batch boundaries and one byte around them.
        final List<Integer> ends = new ArrayList<>();
        final Set<Integer> lengths = new TreeSet<>();
        for (int length = 0; length <= 200; length++) {
            lengths.add(length);
        }
        int start = 0;
        for (final KafkaTestData.KafkaBuiltBatch batch : built) {
            final int end = start + batch.bytes().length;
            ends.add(end);
            lengths.addAll(List.of(start + 11, start + 12, start + 13, end - 1, end));
            start = end;
        }

        for (final int length : lengths) {
            if (length > segment.length) {
                continue;
            }
            final Path file = Files.write(dir.resolve("cut-" + length + ".log"), Arrays.copyOf(segment, length));
            final int completeBatches = (int) ends.stream().filter(end -> end <= length).count();

            assertThat(read(file))
                .usingRecursiveComparison()
                .withEqualsForType(ByteBuffer::equals, ByteBuffer.class)
                .isEqualTo(expected.subList(0, completeBatches));
        }
    }

    @Test
    void emptyFileHasNoBatches() throws IOException {
        final Path file = Files.createFile(dir.resolve("empty.log"));

        assertThat(read(file)).isEmpty();
    }

    @Test
    void failsOnAnImpossibleBatchSize() throws IOException {
        final byte[] segment = segment(KafkaTestData.kafkaBuiltBatches());
        segment[11] = 3;  // the low byte of the size of the first batch
        segment[10] = 0;
        segment[9] = 0;
        segment[8] = 0;
        final Path file = Files.write(dir.resolve("corrupt.log"), segment);

        assertThatThrownBy(() -> read(file)).isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Invalid batch size 3");
    }
}
