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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.kafka.common.header.Header;

/**
 * Batches to test the reader and writer with, and the batch that reading each of them must give back.
 */
final class KafkaTestData {
    static final long NO_TIMESTAMP = -1L;

    private KafkaTestData() {
    }

    record Case(String name, KafkaBatchWriter.BatchHeader header, List<KafkaBatchWriter.RecordData> records) {
        @Override
        public String toString() {
            return name;
        }

        /** The batch that reading the written case must give back. */
        KafkaBatch expectedBatch() {
            final boolean logAppendTime = header.timestampTypeId() == KafkaBatchWriter.LOG_APPEND_TIME;
            final List<KafkaRecord> expected = new ArrayList<>();
            long maxTimestamp = NO_TIMESTAMP;
            for (int i = 0; i < records.size(); i++) {
                final KafkaBatchWriter.RecordData record = records.get(i);
                final long timestamp = logAppendTime ? header.maxTimestamp() : record.timestamp();
                maxTimestamp = Math.max(maxTimestamp, timestamp);
                expected.add(new KafkaRecord(header.baseOffset() + i, timestamp,
                    record.key() == null ? -1 : record.key().remaining(), record.key(),
                    record.value() == null ? -1 : record.value().remaining(), record.value(),
                    record.headers()));
            }
            return new KafkaBatch(header.baseOffset(), header.partitionLeaderEpoch(), header.magic(),
                header.timestampTypeId(), (byte) header.compressionTypeId(), maxTimestamp,
                header.producerId(), header.producerEpoch(), header.baseSequence(), expected);
        }
    }

    /** A case and the bytes of its batch, as built by Kafka itself. */
    record KafkaBuiltBatch(Case testCase, byte[] bytes) {
        @Override
        public String toString() {
            return testCase.name();
        }
    }

    /**
     * Batches built by Kafka's own classes (several versions agree on their content), one per line of
     * {@code kafka-batches.txt} as {@code case name<TAB>hex}. Only a few cases of each codec are there, since
     * compressed bytes depend on the library, so they cannot be rebuilt and compared byte by byte.
     *
     * <p>Each line was made from the {@link Case} of the same name, by building its batch with Kafka's
     * {@code MemoryRecordsBuilder} (the case's header and records as the arguments) and hex-encoding the bytes.
     * If a case changes, its line has to be made again the same way, otherwise the tests fail on that case.
     */
    static List<KafkaBuiltBatch> kafkaBuiltBatches() throws IOException {
        final Map<String, Case> byName = new HashMap<>();
        for (final Case testCase : allCases()) {
            byName.put(testCase.name(), testCase);
        }
        try (final InputStream in = KafkaTestData.class.getResourceAsStream("/kafka-batches.txt")) {
            final List<KafkaBuiltBatch> batches = new ArrayList<>();
            for (final String line : new String(in.readAllBytes(), StandardCharsets.UTF_8).lines().toList()) {
                final String[] parts = line.split("\t");
                batches.add(new KafkaBuiltBatch(byName.get(parts[0]), HexFormat.of().parseHex(parts[1])));
            }
            return batches;
        }
    }

    static List<Case> allCases() {
        final List<Case> all = new ArrayList<>();
        for (int codec = 0; codec <= 4; codec++) {
            for (int timestampType = 0; timestampType <= 1; timestampType++) {
                all.addAll(cases(codec, timestampType));
            }
        }
        return all;
    }

    static List<Case> cases(final int compressionTypeId, final int timestampTypeId) {
        final String suffix = " codec=" + compressionTypeId + " timestampType=" + timestampTypeId;
        final long logAppendTime = 1_700_000_000_000L;
        return List.of(
            new Case("typical" + suffix,
                header(compressionTypeId, timestampTypeId, 0L, logAppendTime, -1L, (short) -1, -1, -1),
                typical()),
            new Case("with producer and offset" + suffix,
                header(compressionTypeId, timestampTypeId, 1_000_000_000_000L, logAppendTime, 123L, (short) 1, 5, 2),
                typical()),
            new Case("nulls and empties" + suffix,
                header(compressionTypeId, timestampTypeId, 7L, logAppendTime, -1L, (short) -1, -1, 0),
                nullsAndEmpties()),
            new Case("single record" + suffix,
                header(compressionTypeId, timestampTypeId, 42L, logAppendTime, 9L, (short) 0, 0, 0),
                List.of(record(1000L, "k", "v"))),
            new Case("out of order timestamps" + suffix,
                header(compressionTypeId, timestampTypeId, 0L, logAppendTime, -1L, (short) -1, -1, -1),
                List.of(record(5000L, "a", "1"), record(1000L, "b", "2"), record(5000L, "c", "3"),
                    record(NO_TIMESTAMP, "d", "4"))),
            new Case("many small records" + suffix,
                header(compressionTypeId, timestampTypeId, 100L, logAppendTime, -1L, (short) -1, -1, 3),
                many(1000)),
            new Case("large value over 512 KiB" + suffix,
                header(compressionTypeId, timestampTypeId, 0L, logAppendTime, -1L, (short) -1, -1, -1),
                List.of(new KafkaBatchWriter.RecordData(1000L, ByteBuffer.wrap("key".getBytes()),
                    ByteBuffer.wrap(randomBytes(700 * 1024)), new Header[0]))));
    }

    private static KafkaBatchWriter.BatchHeader header(final int compressionTypeId,
                                                       final int timestampTypeId,
                                                       final long baseOffset,
                                                       final long logAppendTime,
                                                       final long producerId,
                                                       final short producerEpoch,
                                                       final int baseSequence,
                                                       final int partitionLeaderEpoch) {
        return new KafkaBatchWriter.BatchHeader(KafkaBatchParser.MAGIC_V2, compressionTypeId, timestampTypeId,
            baseOffset, logAppendTime, producerId, producerEpoch, baseSequence, partitionLeaderEpoch);
    }

    private static List<KafkaBatchWriter.RecordData> typical() {
        final List<KafkaBatchWriter.RecordData> records = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            records.add(new KafkaBatchWriter.RecordData(
                1_600_000_000_000L + i * 10,
                ByteBuffer.wrap(("key " + i).getBytes()),
                ByteBuffer.wrap(("value " + i + " ".repeat(i * 100)).getBytes()),
                new Header[] {
                    new KafkaHeader("h1", ("one " + i).getBytes()),
                    new KafkaHeader("h2", ("two " + i).getBytes())}));
        }
        return records;
    }

    private static List<KafkaBatchWriter.RecordData> nullsAndEmpties() {
        return List.of(
            new KafkaBatchWriter.RecordData(10L, null, ByteBuffer.wrap("v".getBytes()), new Header[0]),
            new KafkaBatchWriter.RecordData(11L, ByteBuffer.wrap("k".getBytes()), null, new Header[0]),
            new KafkaBatchWriter.RecordData(12L, ByteBuffer.allocate(0), ByteBuffer.allocate(0), new Header[0]),
            new KafkaBatchWriter.RecordData(13L, null, null, new Header[] {
                new KafkaHeader("null value", null), new KafkaHeader("empty", new byte[0]),
                new KafkaHeader("ünïcödé", "x".getBytes())}));
    }

    private static List<KafkaBatchWriter.RecordData> many(final int count) {
        final List<KafkaBatchWriter.RecordData> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(record(2_000L + i, "key" + i, "value" + i));
        }
        return records;
    }

    private static KafkaBatchWriter.RecordData record(final long timestamp, final String key, final String value) {
        return new KafkaBatchWriter.RecordData(timestamp, ByteBuffer.wrap(key.getBytes()),
            ByteBuffer.wrap(value.getBytes()), new Header[0]);
    }

    private static byte[] randomBytes(final int size) {
        final byte[] bytes = new byte[size];
        new Random(1).nextBytes(bytes);
        return bytes;
    }

}
