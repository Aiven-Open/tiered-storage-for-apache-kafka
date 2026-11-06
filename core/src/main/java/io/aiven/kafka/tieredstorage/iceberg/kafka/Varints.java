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
import java.nio.ByteBuffer;

/**
 * Zigzag variable-length integers, as used by Kafka's record format.
 */
final class Varints {
    private Varints() {
    }

    static int readVarint(final ByteBuffer buffer) {
        final int value = (int) readUnsigned(buffer, 35);
        return (value >>> 1) ^ -(value & 1);
    }

    static long readVarlong(final ByteBuffer buffer) {
        final long value = readUnsigned(buffer, 70);
        return (value >>> 1) ^ -(value & 1);
    }

    private static long readUnsigned(final ByteBuffer buffer, final int maxBits) {
        long value = 0;
        int shift = 0;
        long b;
        while (((b = buffer.get()) & 0x80) != 0) {
            value |= (b & 0x7f) << shift;
            shift += 7;
            if (shift >= maxBits) {
                throw new IllegalArgumentException("Invalid variable-length integer");
            }
        }
        return value | (b << shift);
    }

    static void writeVarint(final int value, final ByteArrayOutputStream out) {
        writeUnsigned(((value << 1) ^ (value >> 31)) & 0xffffffffL, out);
    }

    static void writeVarlong(final long value, final ByteArrayOutputStream out) {
        writeUnsigned((value << 1) ^ (value >> 63), out);
    }

    private static void writeUnsigned(final long zigzag, final ByteArrayOutputStream out) {
        long v = zigzag;
        while ((v & ~0x7fL) != 0) {
            out.write((int) ((v & 0x7f) | 0x80));
            v >>>= 7;
        }
        out.write((int) v);
    }
}
