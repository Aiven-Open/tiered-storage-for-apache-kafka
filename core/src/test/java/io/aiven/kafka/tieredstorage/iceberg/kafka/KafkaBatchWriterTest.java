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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.CRC32C;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Checks the plugin's batch reader and writer against batches that Kafka itself built.
 */
class KafkaBatchWriterTest {
    private static final int CRC_OFFSET = 17;
    private static final int ATTRIBUTES_OFFSET = 21;

    static List<KafkaTestData.Case> cases() {
        return KafkaTestData.allCases();
    }

    static List<KafkaTestData.KafkaBuiltBatch> kafkaBuiltBatches() throws IOException {
        return KafkaTestData.kafkaBuiltBatches();
    }

    static List<KafkaTestData.KafkaBuiltBatch> uncompressedKafkaBuiltBatches() throws IOException {
        return KafkaTestData.kafkaBuiltBatches().stream()
            .filter(built -> built.testCase().header().compressionTypeId() == KafkaCodec.NONE.id())
            .toList();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("kafkaBuiltBatches")
    void parsesBatchBuiltByKafka(final KafkaTestData.KafkaBuiltBatch built) {
        assertThat(KafkaBatchParser.parse(ByteBuffer.wrap(built.bytes())))
            .usingRecursiveComparison()
            .withEqualsForType(ByteBuffer::equals, ByteBuffer.class)
            .isEqualTo(built.testCase().expectedBatch());
    }

    // Compressed bytes depend on the library version and level, so only the uncompressed ones can be compared.
    @ParameterizedTest(name = "{0}")
    @MethodSource("uncompressedKafkaBuiltBatches")
    void writesTheSameBytesAsKafka(final KafkaTestData.KafkaBuiltBatch built) {
        assertThat(KafkaBatchWriter.write(built.testCase().header(), built.testCase().records()))
            .isEqualTo(built.bytes());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("cases")
    void readsBackWhatItWrites(final KafkaTestData.Case testCase) {
        final byte[] written = KafkaBatchWriter.write(testCase.header(), testCase.records());

        assertThat(KafkaBatchParser.parse(ByteBuffer.wrap(written)))
            .usingRecursiveComparison()
            .withEqualsForType(ByteBuffer::equals, ByteBuffer.class)
            .isEqualTo(testCase.expectedBatch());
    }

    @Test
    void rejectsUnknownCompressionType() {
        final var testCase = KafkaTestData.cases(0, 0).get(0);
        final var header = new KafkaBatchWriter.BatchHeader(testCase.header().magic(), 9, 0, 0L, 0L, -1L,
            (short) -1, -1, -1);

        assertThatThrownBy(() -> KafkaBatchWriter.write(header, testCase.records()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unknown compression type 9");
    }

    @Test
    void rejectsNoTimestampType() {
        final var testCase = KafkaTestData.cases(0, 0).get(0);
        final var header = new KafkaBatchWriter.BatchHeader(testCase.header().magic(), 0, -1, 0L, 0L, -1L,
            (short) -1, -1, -1);

        assertThatThrownBy(() -> KafkaBatchWriter.write(header, testCase.records()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unknown timestamp type -1");
    }

    @ParameterizedTest
    @ValueSource(ints = {-2, 2})
    void rejectsRecordSizeThatDoesNotMatchItsBody(final int encodedSizeDelta) throws IOException {
        final byte[] batch = KafkaTestData.kafkaBuiltBatches().stream()
            .filter(built -> built.testCase().name().equals("single record codec=0 timestampType=0"))
            .findFirst()
            .orElseThrow()
            .bytes()
            .clone();
        batch[KafkaBatchWriter.HEADER_SIZE] += (byte) encodedSizeDelta;
        updateCrc(batch);

        assertThatThrownBy(() -> KafkaBatchParser.parse(ByteBuffer.wrap(batch)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageStartingWith("Invalid record size:");
    }

    private static void updateCrc(final byte[] batch) {
        final CRC32C crc = new CRC32C();
        crc.update(batch, ATTRIBUTES_OFFSET, batch.length - ATTRIBUTES_OFFSET);
        ByteBuffer.wrap(batch).putInt(CRC_OFFSET, (int) crc.getValue());
    }
}
