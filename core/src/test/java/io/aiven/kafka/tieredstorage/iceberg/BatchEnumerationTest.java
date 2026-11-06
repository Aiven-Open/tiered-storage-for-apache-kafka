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

package io.aiven.kafka.tieredstorage.iceberg;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;

import io.aiven.kafka.tieredstorage.iceberg.data.RowSchema;
import io.aiven.kafka.tieredstorage.iceberg.kafka.KafkaBatch;
import io.aiven.kafka.tieredstorage.iceberg.kafka.KafkaHeader;
import io.aiven.kafka.tieredstorage.iceberg.kafka.KafkaLogReader;
import io.aiven.kafka.tieredstorage.iceberg.kafka.KafkaRecord;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BatchEnumerationTest {
    static final String HEADER1_KEY = "header1";
    static final String HEADER2_KEY = "header2";

    // Kafka's values, not taken from its record classes (not public API, they moved between versions)
    static final byte MAGIC_V2 = 2;
    static final int NO_COMPRESSION_ID = 0;
    static final int GZIP_ID = 1;
    static final int SNAPPY_ID = 2;
    static final int LZ4_ID = 3;
    static final int ZSTD_ID = 4;

    @TempDir
    Path dir;

    @Test
    void test() throws IOException {
        // TODO test nulls

        // Records 0-2 (batch 0) are not "read".

        // Batch 1.
        final byte batch1Magic = MAGIC_V2;
        final int batch1ByteOffset = 1;
        final int batch1CompressionType = GZIP_ID;
        final TimestampType batch1TimestampType = TimestampType.CREATE_TIME;
        final long batch1BaseOffset = 3L;
        final long batch1LogAppendTime = 1000L;
        final long batch1ProducerId = 123L;
        final short batch1ProducerEpoch = 1;
        final int batch1BaseSequence = 0;
        final int batch1PartitionLeaderEpoch = 0;

        final GenericData.Record record3 = createRecord(
            3L, 1000000003L, batch1ByteOffset, batch1BaseOffset, batch1PartitionLeaderEpoch, batch1Magic,
            batch1TimestampType, batch1CompressionType,
            batch1LogAppendTime,
            batch1ProducerId,
            batch1ProducerEpoch,
            batch1BaseSequence
        );
        final GenericData.Record record4 = createRecord(
            4L, 1000000004L, batch1ByteOffset, batch1BaseOffset, batch1PartitionLeaderEpoch, batch1Magic,
            batch1TimestampType, batch1CompressionType,
            batch1LogAppendTime,
            batch1ProducerId,
            batch1ProducerEpoch,
            batch1BaseSequence
        );
        final GenericData.Record record5 = createRecord(
            5L, 1000000005L, batch1ByteOffset, batch1BaseOffset, batch1PartitionLeaderEpoch, batch1Magic,
            batch1TimestampType, batch1CompressionType,
            batch1LogAppendTime,
            batch1ProducerId,
            batch1ProducerEpoch,
            0
        );

        // Batch 2.
        final int batch2Magic = MAGIC_V2;
        final int batch2ByteOffset = 2;
        final int batch2CompressionType = SNAPPY_ID;
        final TimestampType batch2TimestampType = TimestampType.LOG_APPEND_TIME;
        final long batch2BaseOffset = 6L;
        final long batch2LogAppendTime = 2000L;
        final long batch2ProducerId = 456L;
        final short batch2ProducerEpoch = 2;
        final int batch2BaseSequence = 4;
        final int batch2PartitionLeaderEpoch = 2;

        final GenericData.Record record6 = createRecord(
            6L, 0L, batch2ByteOffset, batch2BaseOffset, batch2PartitionLeaderEpoch, batch2Magic,
            batch2TimestampType, batch2CompressionType,
            batch2LogAppendTime,
            batch2ProducerId,
            batch2ProducerEpoch,
            batch2BaseSequence
            // doesn't matter, log append time is used
        );
        final GenericData.Record record7 = createRecord(
            7L, 0L, batch2ByteOffset, batch2BaseOffset, batch2PartitionLeaderEpoch, batch2Magic,
            batch2TimestampType, batch2CompressionType,
            batch2LogAppendTime,
            batch2ProducerId,
            batch2ProducerEpoch,
            batch2BaseSequence
            // doesn't matter, log append time is used
        );
        final GenericData.Record record8 = createRecord(
            8L, 0L, batch2ByteOffset, batch2BaseOffset, batch2PartitionLeaderEpoch, batch2Magic,
            batch2TimestampType, batch2CompressionType,
            batch2LogAppendTime,
            batch2ProducerId,
            batch2ProducerEpoch,
            batch2BaseSequence
            // doesn't matter, log append time is used
        );

        final var multiFileReader = mock(MultiFileReader.class);
        when(multiFileReader.read()).thenReturn(
            record3, record4, record5,
            record6, record7, record8,
            null);

        final RecordBatchGrouper recordBatchGrouper = new RecordBatchGrouper(multiFileReader);
        final StructureProvider structureProvider = mock(StructureProvider.class);
        try (final var batchEnumeration = new BatchEnumeration(recordBatchGrouper, structureProvider, "")) {
            // Batch 1.
            assertThat(batchEnumeration.hasMoreElements()).isTrue();
            final var batches1 = batches(batchEnumeration.nextElement().readAllBytes());
            assertThat(batches1).hasSize(1);

            final KafkaBatch batch1 = batches1.get(0);
            assertThat(batch1.magic()).isEqualTo(batch1Magic);
            assertThat((int) batch1.compressionTypeId()).isEqualTo(batch1CompressionType);
            assertThat(batch1.timestampTypeId()).isEqualTo(batch1TimestampType.id);
            assertThat(batch1.baseOffset()).isEqualTo(batch1BaseOffset);
            assertThat(batch1.producerId()).isEqualTo(batch1ProducerId);
            assertThat(batch1.producerEpoch()).isEqualTo(batch1ProducerEpoch);
            assertThat(batch1.baseSequence()).isEqualTo(batch1BaseSequence);
            assertThat(batch1.partitionLeaderEpoch()).isEqualTo(batch1PartitionLeaderEpoch);

            assertThat(batch1.records())
                .usingRecursiveComparison()
                .withEqualsForType(ByteBuffer::equals, ByteBuffer.class)
                .isEqualTo(List.of(
                    expectedRecord(3L, 1000000003L),
                    expectedRecord(4L, 1000000004L),
                    expectedRecord(5L, 1000000005L)));

            // Batch 2.
            assertThat(batchEnumeration.hasMoreElements()).isTrue();
            final var batches2 = batches(batchEnumeration.nextElement().readAllBytes());
            assertThat(batches2).hasSize(1);

            final KafkaBatch batch2 = batches2.get(0);
            assertThat(batch2.magic()).isEqualTo((byte) batch2Magic);
            assertThat((int) batch2.compressionTypeId()).isEqualTo(batch2CompressionType);
            assertThat(batch2.timestampTypeId()).isEqualTo(batch2TimestampType.id);
            assertThat(batch2.baseOffset()).isEqualTo(batch2BaseOffset);
            assertThat(batch2.producerId()).isEqualTo(batch2ProducerId);
            assertThat(batch2.producerEpoch()).isEqualTo(batch2ProducerEpoch);
            assertThat(batch2.baseSequence()).isEqualTo(batch2BaseSequence);
            assertThat(batch2.partitionLeaderEpoch()).isEqualTo(batch2PartitionLeaderEpoch);

            assertThat(batch2.records())
                .usingRecursiveComparison()
                .withEqualsForType(ByteBuffer::equals, ByteBuffer.class)
                .isEqualTo(List.of(
                    expectedRecord(6L, batch2LogAppendTime),
                    expectedRecord(7L, batch2LogAppendTime),
                    expectedRecord(8L, batch2LogAppendTime)));

            assertThat(batchEnumeration.hasMoreElements()).isFalse();
        }
    }

    @Test
    void batchLargerThanInitialBuffer() throws IOException {
        // Bigger than the 512 KiB buffer that the Kafka-based builder used to start with.
        final byte[] bigValue = new byte[700 * 1024];
        new Random(1).nextBytes(bigValue);

        final GenericData.Record record = singleRecord(NO_COMPRESSION_ID);
        record.put(RowSchema.Fields.VALUE_RAW, bigValue);

        final KafkaBatch batch = readSingleBatch(record);
        assertThat((int) batch.compressionTypeId()).isEqualTo(NO_COMPRESSION_ID);
        assertThat(batch.records()).hasSize(1);
        assertThat(batch.records().get(0).offset()).isEqualTo(10L);
        assertThat(batch.records().get(0).value()).isEqualByComparingTo(ByteBuffer.wrap(bigValue));
    }

    @ParameterizedTest
    @ValueSource(ints = {NO_COMPRESSION_ID, GZIP_ID, SNAPPY_ID, LZ4_ID, ZSTD_ID})
    void compressionTypeIsPreserved(final int compressionTypeId) throws IOException {
        final KafkaBatch batch = readSingleBatch(singleRecord(compressionTypeId));
        assertThat((int) batch.compressionTypeId()).isEqualTo(compressionTypeId);
        assertThat(batch.records()).hasSize(1);
        assertThat(batch.records().get(0).value()).isEqualByComparingTo(ByteBuffer.wrap(value(10L)));
    }

    @Test
    void unknownCompressionTypeFails() throws IOException {
        final var multiFileReader = mock(MultiFileReader.class);
        when(multiFileReader.read()).thenReturn(singleRecord(9), (GenericData.Record) null);

        try (final var batchEnumeration = new BatchEnumeration(
            new RecordBatchGrouper(multiFileReader), mock(StructureProvider.class), "")) {
            assertThat(batchEnumeration.hasMoreElements()).isTrue();
            assertThatThrownBy(batchEnumeration::nextElement)
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Unknown compression type 9");
        }
    }

    private static GenericData.Record singleRecord(final int compressionTypeId) {
        return createRecord(
            10L, 1000L, 0, 10L, 0, MAGIC_V2,
            TimestampType.CREATE_TIME, compressionTypeId,
            1000L, -1L, -1, -1);
    }

    private KafkaBatch readSingleBatch(final GenericData.Record record) throws IOException {
        final var multiFileReader = mock(MultiFileReader.class);
        when(multiFileReader.read()).thenReturn(record, (GenericData.Record) null);

        try (final var batchEnumeration = new BatchEnumeration(
            new RecordBatchGrouper(multiFileReader), mock(StructureProvider.class), "")) {
            assertThat(batchEnumeration.hasMoreElements()).isTrue();
            final var batches = batches(batchEnumeration.nextElement().readAllBytes());
            assertThat(batches).hasSize(1);
            assertThat(batchEnumeration.hasMoreElements()).isFalse();
            return batches.get(0);
        }
    }

    private static GenericData.Record createRecord(
        final long offset,
        final long timestamp,
        final int batchByteOffset,
        final long batchBaseOffset,
        final int batchPartitionLeaderEpoch,
        final int batchMagic,
        final TimestampType batchTimestampType,
        final int batchCompressionType,
        final long batchMaxTimestamp,
        final long batchProducerId,
        final int batchProducerEpoch,
        final int batchBaseSequence
    ) {
        final GenericData.Record record = new GenericData.Record(RowSchema.createRowSchema(
            Schema.createRecord("k", "", "", false),
            Schema.createRecord("v", "", "", false)
        ));

        final GenericData.Record kafkaPart = new GenericData.Record(RowSchema.KAFKA);
        kafkaPart.put(RowSchema.Fields.OFFSET, offset);
        kafkaPart.put(RowSchema.Fields.TIMESTAMP, timestamp);
        kafkaPart.put(RowSchema.Fields.BATCH_BYTE_OFFSET, batchByteOffset);
        kafkaPart.put(RowSchema.Fields.BATCH_BASE_OFFSET, batchBaseOffset);
        kafkaPart.put(RowSchema.Fields.BATCH_PARTITION_LEADER_EPOCH, batchPartitionLeaderEpoch);
        kafkaPart.put(RowSchema.Fields.BATCH_MAGIC, batchMagic);
        kafkaPart.put(RowSchema.Fields.BATCH_TIMESTAMP_TYPE, batchTimestampType.id);
        kafkaPart.put(RowSchema.Fields.BATCH_COMPRESSION_TYPE, batchCompressionType);
        kafkaPart.put(RowSchema.Fields.BATCH_MAX_TIMESTAMP, batchMaxTimestamp);
        kafkaPart.put(RowSchema.Fields.BATCH_PRODUCER_ID, batchProducerId);
        kafkaPart.put(RowSchema.Fields.BATCH_PRODUCER_EPOCH, batchProducerEpoch);
        kafkaPart.put(RowSchema.Fields.BATCH_BASE_SEQUENCE, batchBaseSequence);
        record.put(RowSchema.Fields.KAFKA, kafkaPart);

        final GenericData.Record header1 = new GenericData.Record(RowSchema.HEADER);
        header1.put(RowSchema.Fields.HEADER_KEY, HEADER1_KEY);
        header1.put(RowSchema.Fields.HEADER_VALUE, header1Value(offset));
        final GenericData.Record header2 = new GenericData.Record(RowSchema.HEADER);
        header2.put(RowSchema.Fields.HEADER_KEY, HEADER2_KEY);
        header2.put(RowSchema.Fields.HEADER_VALUE, header2Value(offset));
        record.put(RowSchema.Fields.HEADERS, List.of(header1, header2));

        record.put(RowSchema.Fields.KEY_RAW, key(offset));
        record.put(RowSchema.Fields.VALUE_RAW, value(offset));

        return record;
    }

    private static KafkaRecord expectedRecord(final long offset, final long timestamp) {
        final byte[] key = key(offset);
        final byte[] value = value(offset);
        return new KafkaRecord(offset, timestamp,
            key.length, ByteBuffer.wrap(key),
            value.length, ByteBuffer.wrap(value),
            new Header[] {
                new KafkaHeader(HEADER1_KEY, header1Value(offset)),
                new KafkaHeader(HEADER2_KEY, header2Value(offset))});
    }

    private List<KafkaBatch> batches(final byte[] bytes) throws IOException {
        final Path file = Files.write(Files.createTempFile(dir, "batches", ".log"), bytes);
        final List<KafkaBatch> batches = new ArrayList<>();
        try (final KafkaLogReader reader = KafkaLogReader.open(file)) {
            reader.forEachRemaining(batches::add);
        }
        return batches;
    }

    private static byte[] header1Value(final long offset) {
        return String.format("header1 value: %d", offset).getBytes();
    }

    private static byte[] header2Value(final long offset) {
        return String.format("header2 value: %d", offset).getBytes();
    }

    private static byte[] key(final long offset) {
        return String.format("key %d", offset).getBytes();
    }

    private static byte[] value(final long offset) {
        return String.format("value %d", offset).getBytes();
    }
}
