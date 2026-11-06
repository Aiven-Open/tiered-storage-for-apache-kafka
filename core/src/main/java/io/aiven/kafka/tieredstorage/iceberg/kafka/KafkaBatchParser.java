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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.header.Header;

/**
 * Parses one Kafka record batch (message format v2).
 */
final class KafkaBatchParser {
    static final byte MAGIC_V2 = 2;

    private static final int MAGIC_OFFSET = 16;
    private static final int LOG_APPEND_TIME_MASK = 0x08;
    private static final int COMPRESSION_MASK = 0x07;

    private KafkaBatchParser() {
    }

    /**
     * @param bytes one complete batch, starting at its position
     */
    static KafkaBatch parse(final ByteBuffer bytes) {
        final ByteBuffer batch = bytes.slice();
        final byte magic = batch.get(MAGIC_OFFSET);
        if (magic != MAGIC_V2) {
            throw new UnsupportedOperationException("Unsupported magic " + magic);
        }
        final long baseOffset = batch.getLong(0);
        final int partitionLeaderEpoch = batch.getInt(12);
        final short attributes = batch.getShort(21);
        final long baseTimestamp = batch.getLong(27);
        final long maxTimestamp = batch.getLong(35);
        final long producerId = batch.getLong(43);
        final short producerEpoch = batch.getShort(51);
        final int baseSequence = batch.getInt(53);
        final int recordCount = batch.getInt(57);

        final KafkaCodec codec = KafkaCodec.forId(attributes & COMPRESSION_MASK);
        final boolean logAppendTime = (attributes & LOG_APPEND_TIME_MASK) != 0;
        final ByteBuffer records = records(batch.slice(KafkaBatchWriter.HEADER_SIZE,
            batch.limit() - KafkaBatchWriter.HEADER_SIZE), codec);

        final List<KafkaRecord> result = new ArrayList<>(recordCount);
        for (int i = 0; i < recordCount; i++) {
            result.add(parseRecord(records, baseOffset, baseTimestamp, logAppendTime ? maxTimestamp : null));
        }
        return new KafkaBatch(
            baseOffset,
            partitionLeaderEpoch,
            magic,
            logAppendTime ? KafkaBatchWriter.LOG_APPEND_TIME : KafkaBatchWriter.CREATE_TIME,
            (byte) codec.id(),
            maxTimestamp,
            producerId,
            producerEpoch,
            baseSequence,
            result);
    }

    private static ByteBuffer records(final ByteBuffer compressed, final KafkaCodec codec) {
        if (codec == KafkaCodec.NONE) {
            return compressed;
        }
        final byte[] bytes = new byte[compressed.remaining()];
        compressed.get(bytes);
        try (final InputStream in = codec.decompress(new ByteArrayInputStream(bytes))) {
            return ByteBuffer.wrap(in.readAllBytes());
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static KafkaRecord parseRecord(final ByteBuffer in,
                                           final long baseOffset,
                                           final long baseTimestamp,
                                           final Long logAppendTime) {
        final int recordSize = Varints.readVarint(in);
        if (recordSize < 0 || in.remaining() < recordSize) {
            throw new IllegalStateException("Invalid record size: expected " + recordSize
                + " bytes in record payload, but the buffer has only " + in.remaining() + " remaining bytes");
        }
        final int recordStart = in.position();
        in.get();  // attributes, unused
        final long timestampDelta = Varints.readVarlong(in);
        final int offsetDelta = Varints.readVarint(in);
        final int keySize = Varints.readVarint(in);
        final ByteBuffer key = readSlice(in, keySize);
        final int valueSize = Varints.readVarint(in);
        final ByteBuffer value = readSlice(in, valueSize);
        final int headerCount = Varints.readVarint(in);
        if (headerCount < 0 || headerCount > in.remaining()) {
            throw new IllegalStateException("Invalid header count " + headerCount);
        }
        final Header[] headers = new Header[headerCount];
        for (int i = 0; i < headerCount; i++) {
            final int headerKeySize = Varints.readVarint(in);
            if (headerKeySize < 0) {
                throw new IllegalStateException("Invalid negative header key size " + headerKeySize);
            }
            final byte[] headerKey = new byte[headerKeySize];
            in.get(headerKey);
            final int headerValueSize = Varints.readVarint(in);
            byte[] headerValue = null;
            if (headerValueSize >= 0) {
                headerValue = new byte[headerValueSize];
                in.get(headerValue);
            }
            headers[i] = new KafkaHeader(new String(headerKey, StandardCharsets.UTF_8), headerValue);
        }
        final int actualSize = in.position() - recordStart;
        if (actualSize != recordSize) {
            throw new IllegalStateException("Invalid record size: expected to read " + recordSize
                + " bytes in record payload, but instead read " + actualSize);
        }
        return new KafkaRecord(
            baseOffset + offsetDelta,
            logAppendTime != null ? logAppendTime : baseTimestamp + timestampDelta,
            keySize, key, valueSize, value, headers);
    }

    private static ByteBuffer readSlice(final ByteBuffer in, final int size) {
        if (size < 0) {
            return null;
        }
        if (size > in.remaining()) {
            throw new IllegalStateException("Invalid byte array size " + size);
        }
        final ByteBuffer slice = in.slice(in.position(), size);
        in.position(in.position() + size);
        return slice;
    }
}
