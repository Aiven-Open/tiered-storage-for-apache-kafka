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

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

/**
 * Checks if segment are compressed or not.
 * To be used when segment files are received on archival.
 *
 * <p>It reads the first batch header from the file. It does not use the Kafka record classes,
 * which are internal API and move between Kafka versions. The offsets mirror the Kafka log format
 * (see the record batch format in the Kafka documentation): v2 in DefaultRecordBatch, v0 and v1
 * in LegacyRecord, and the 12-byte log overhead in Records. LegacyRecord offsets are relative to
 * the record, so the constants here add the log overhead. Compression ids are in CompressionType.
 */
public class SegmentCompressionChecker {

    private static final int SIZE_OFFSET = 8;
    private static final int LOG_OVERHEAD = 12;
    private static final int MAGIC_OFFSET = 16;
    private static final int HEADER_SIZE_UP_TO_MAGIC = 17;

    private static final byte MAGIC_V0 = 0;
    private static final byte MAGIC_V1 = 1;
    private static final byte MAGIC_V2 = 2;

    private static final int LEGACY_CRC_OFFSET = 12;
    private static final int LEGACY_ATTRIBUTES_OFFSET = 17;
    private static final int LEGACY_MIN_SIZE = 14;

    private static final int V2_CRC_OFFSET = 17;
    private static final int V2_ATTRIBUTES_OFFSET = 21;
    private static final int V2_MIN_SIZE = 49;

    private static final int PREFIX_SIZE = V2_ATTRIBUTES_OFFSET + Short.BYTES;
    private static final int COMPRESSION_MASK = 0x07;
    private static final int MAX_COMPRESSION_ID = 4;
    private static final int CHUNK_SIZE = 64 * 1024;

    /**
     * @param file Kafka log segment file
     * @return true if log segment is compressed, otherwise returns false
     */
    public static boolean check(final File file) throws InvalidRecordBatchException {
        try (final FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            final long end = channel.size();
            if (end <= HEADER_SIZE_UP_TO_MAGIC) {
                throw new InvalidRecordBatchException("Record batch is null");
            }
            final ByteBuffer prefix = ByteBuffer.allocate((int) Math.min(PREFIX_SIZE, end));
            readFully(channel, prefix);

            final int size = prefix.getInt(SIZE_OFFSET);
            if (size < LEGACY_MIN_SIZE) {
                throw invalid();
            }
            final long batchEnd = (long) LOG_OVERHEAD + size;
            if (batchEnd > end) {
                throw new InvalidRecordBatchException("Record batch is null");
            }

            final byte magic = prefix.get(MAGIC_OFFSET);
            final int attributes;
            if (magic == MAGIC_V0 || magic == MAGIC_V1) {
                verifyChecksum(channel, new CRC32(), prefix.getInt(LEGACY_CRC_OFFSET), MAGIC_OFFSET, batchEnd);
                attributes = prefix.get(LEGACY_ATTRIBUTES_OFFSET);
            } else if (magic == MAGIC_V2 && size >= V2_MIN_SIZE) {
                verifyChecksum(channel, new CRC32C(), prefix.getInt(V2_CRC_OFFSET), V2_ATTRIBUTES_OFFSET, batchEnd);
                attributes = prefix.getShort(V2_ATTRIBUTES_OFFSET);
            } else {
                throw invalid();
            }

            final int compressionId = attributes & COMPRESSION_MASK;
            if (compressionId > MAX_COMPRESSION_ID) {
                throw invalid();
            }
            return compressionId != 0;
        } catch (final IOException e) {
            throw new InvalidRecordBatchException("Failed to read and validate first batch", e);
        }
    }

    private static void verifyChecksum(final FileChannel channel,
                                       final Checksum checksum,
                                       final int stored,
                                       final long from,
                                       final long to) throws IOException, InvalidRecordBatchException {
        final ByteBuffer chunk = ByteBuffer.allocate(CHUNK_SIZE);
        for (long position = from; position < to; position += chunk.limit()) {
            chunk.clear().limit((int) Math.min(CHUNK_SIZE, to - position));
            readFully(channel, chunk, position);
            checksum.update(chunk.array(), 0, chunk.limit());
        }
        if (checksum.getValue() != (stored & 0xFFFFFFFFL)) {
            throw invalid();
        }
    }

    private static void readFully(final FileChannel channel, final ByteBuffer buffer) throws IOException {
        readFully(channel, buffer, 0);
    }

    private static void readFully(final FileChannel channel,
                                  final ByteBuffer buffer,
                                  final long start) throws IOException {
        long position = start;
        while (buffer.hasRemaining()) {
            final int read = channel.read(buffer, position);
            if (read < 0) {
                throw new EOFException("Unexpected end of file");
            }
            position += read;
        }
    }

    private static InvalidRecordBatchException invalid() {
        return new InvalidRecordBatchException("Failed to read and validate first batch");
    }
}
