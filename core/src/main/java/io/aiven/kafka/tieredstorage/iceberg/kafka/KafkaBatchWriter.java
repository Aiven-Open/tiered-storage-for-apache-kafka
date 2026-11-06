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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.CRC32C;

import org.apache.kafka.common.header.Header;

/**
 * Writes a Kafka record batch (message format v2) as Kafka writes it to a log segment.
 * Transactional and control batches are not supported.
 */
public final class KafkaBatchWriter {

    /**
     * The batch fields that do not come from the records.
     *
     * @param maxTimestamp the log append time. It is used only for batches with log append time, as the batch
     *                     max timestamp: readers take the timestamp of every record from it. For other batches
     *                     the max timestamp is the largest record timestamp.
     */
    public record BatchHeader(
        byte magic,
        int compressionTypeId,
        int timestampTypeId,
        long baseOffset,
        long maxTimestamp,
        long producerId,
        short producerEpoch,
        int baseSequence,
        int partitionLeaderEpoch
    ) {
    }

    /** A record to write. Its offset is {@code baseOffset} plus its position in the batch. */
    public record RecordData(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
    }

    static final int HEADER_SIZE = 61;
    static final int LOG_OVERHEAD = 12;
    private static final int CRC_OFFSET = 17;
    private static final int ATTRIBUTES_OFFSET = 21;
    private static final int LOG_APPEND_TIME_MASK = 0x08;
    static final int LOG_APPEND_TIME = 1;
    static final int CREATE_TIME = 0;
    private static final long NO_TIMESTAMP = -1L;

    private KafkaBatchWriter() {
    }

    public static byte[] write(final BatchHeader header, final List<RecordData> records) {
        if (header.magic() != KafkaBatchParser.MAGIC_V2) {
            throw new UnsupportedOperationException("Unsupported magic " + header.magic());
        }
        if (header.timestampTypeId() != CREATE_TIME && header.timestampTypeId() != LOG_APPEND_TIME) {
            throw new IllegalArgumentException("Unknown timestamp type " + header.timestampTypeId());
        }
        if (records.isEmpty()) {
            throw new IllegalArgumentException("A batch needs at least one record");
        }
        final KafkaCodec codec = KafkaCodec.forId(header.compressionTypeId());
        final boolean logAppendTime = header.timestampTypeId() == LOG_APPEND_TIME;

        try {
            final ByteArrayOutputStream body = new ByteArrayOutputStream();
            long baseTimestamp = 0;
            long maxTimestamp = NO_TIMESTAMP;
            for (int i = 0; i < records.size(); i++) {
                final RecordData record = records.get(i);
                if (i == 0) {
                    baseTimestamp = record.timestamp();
                }
                maxTimestamp = Math.max(maxTimestamp, record.timestamp());
                writeRecord(record, record.timestamp() - baseTimestamp, i, body);
            }

            final byte[] compressedRecords = compress(codec, body.toByteArray());
            final ByteBuffer batch = ByteBuffer.allocate(HEADER_SIZE + compressedRecords.length);
            batch.putLong(header.baseOffset());
            batch.putInt(batch.capacity() - LOG_OVERHEAD);
            batch.putInt(header.partitionLeaderEpoch());
            batch.put(header.magic());
            batch.putInt(0);  // the CRC, set below
            batch.putShort((short) (codec.id() | (logAppendTime ? LOG_APPEND_TIME_MASK : 0)));
            batch.putInt(records.size() - 1);
            batch.putLong(baseTimestamp);
            batch.putLong(logAppendTime ? header.maxTimestamp() : maxTimestamp);
            batch.putLong(header.producerId());
            batch.putShort(header.producerEpoch());
            batch.putInt(header.baseSequence());
            batch.putInt(records.size());
            batch.put(compressedRecords);

            final CRC32C crc = new CRC32C();
            crc.update(batch.array(), ATTRIBUTES_OFFSET, batch.capacity() - ATTRIBUTES_OFFSET);
            batch.putInt(CRC_OFFSET, (int) crc.getValue());
            return batch.array();
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to write a Kafka record batch", e);
        }
    }

    private static byte[] compress(final KafkaCodec codec, final byte[] data) throws IOException {
        if (codec == KafkaCodec.NONE) {
            return data;
        }
        final ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (final OutputStream out = codec.compress(compressed)) {
            out.write(data);
        }
        return compressed.toByteArray();
    }

    private static void writeRecord(final RecordData record,
                                    final long timestampDelta,
                                    final int offsetDelta,
                                    final ByteArrayOutputStream out) {
        final ByteArrayOutputStream body = new ByteArrayOutputStream();
        body.write(0);  // attributes, unused
        Varints.writeVarlong(timestampDelta, body);
        Varints.writeVarint(offsetDelta, body);
        writeBytes(record.key(), body);
        writeBytes(record.value(), body);
        final Header[] headers = record.headers();
        Varints.writeVarint(headers.length, body);
        for (final Header header : headers) {
            if (header.key() == null) {
                throw new IllegalArgumentException("Invalid null header key found in headers");
            }
            final byte[] key = header.key().getBytes(StandardCharsets.UTF_8);
            Varints.writeVarint(key.length, body);
            body.writeBytes(key);
            final byte[] value = header.value();
            Varints.writeVarint(value == null ? -1 : value.length, body);
            if (value != null) {
                body.writeBytes(value);
            }
        }
        Varints.writeVarint(body.size(), out);
        out.writeBytes(body.toByteArray());
    }

    private static void writeBytes(final ByteBuffer bytes, final ByteArrayOutputStream out) {
        if (bytes == null) {
            Varints.writeVarint(-1, out);
            return;
        }
        final ByteBuffer copy = bytes.duplicate();
        Varints.writeVarint(copy.remaining(), out);
        final byte[] array = new byte[copy.remaining()];
        copy.get(array);
        out.writeBytes(array);
    }
}
