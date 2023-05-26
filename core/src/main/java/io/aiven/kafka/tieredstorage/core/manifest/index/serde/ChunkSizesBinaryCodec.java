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

package io.aiven.kafka.tieredstorage.core.manifest.index.serde;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

// TODO property-based testing
/**
 * A binary encoder/decoder for lists of chunk sizes.
 *
 * <p>It tries to encode values using few bytes.
 * The approach is based on the assumption, that values are pretty similar to each other.
 * This is a fair assumption because values are supposed to be transformed chunk sizes.
 * The original chunk size is supposed to be the same for each input, so the transformed chunks should be also
 * "reasonably" close (not orders of magnitude different).
 * The last value is an exception, it may be significantly smaller (even 1) compared to other values. Because of this,
 * the last value doesn't participate in the compression and is encoded independently in full 4 bytes.
 *
 * <p>The first technique used to reduce the byte count per value is finding the max number of bytes
 * needed to encode values and using it instead of full 4 bytes.
 *
 * <p>Based on the similarity assumption and the first technique, the second technique
 * is to find the "base" (i.e. the minimum) and subtract it from all values (again, except for the last),
 * thus reducing the number of bytes needed to encode values.
 *
 * <p>For example, consider values <code>1000000, 1000010, 1000020</code> (each needs min 3 bytes to encode).
 * The base is 1000000 and "de-based" values are <code>0, 10, 20</code>, which need 1 byte per value.
 *
 * <p>The average byte count per value depends mostly on the variability of values. For example
 *
 * <pre>
 *     int size = Integer.MAX_VALUE;  // 2 GB
 *     int originalChunkSize = 1024 * 1024;  // 1 MB
 *     int variability = 300;
 *     List&lt;Integer&gt; chunks = new ArrayList<>();
 *     var random = new Random();
 *     for (int pos = originalChunkSize; pos < size - originalChunkSize; pos += originalChunkSize) {
 *         chunks.add(originalChunkSize + random.nextInt(variability));
 *     }
 *     System.out.println("Bytes per value: "
 *         + (float) ChunkSizesBinaryCodec.encode(chunks).length / chunks.size());
 * </pre>
 * prints "<code>Bytes per value: 2.0053763</code>".
 * If variability is <code>200</code>, it prints "<code>Bytes per value: 1.0058651</code>".
 *
 * <p>The byte layout is the following:
 * <table>
 *     <tr>
 *         <th>Field</th>
 *         <th>Size, B</th>
 *     </tr>
 *     <tr>
 *         <td>Count</td>
 *         <td>4</td>
 *     </tr>
 *     <tr>
 *         <td>Base</td>
 *         <td>4</td>
 *     </tr>
 *     <tr>
 *         <td>Bytes per value</td>
 *         <td>1</td>
 *     </tr>
 *     <tr>
 *         <td>Values (without last)</td>
 *         <td>(Count - 1) * Bytes per value</td>
 *     </tr>
 *     <tr>
 *         <td>Last value</td>
 *         <td>4</td>
 *     </tr>
 * </table>
 *
 * <p>When there are zero values, only the count is output.
 *
 * <p>When there is one value, only the count and the (last) value is output,
 * i.e. no base, bytes per value, non-last values.
 *
 * <p>The codec accepts only non-negative (>= 0) values.
 */
class ChunkSizesBinaryCodec {
    private static final int COUNT_SIZE = 4;
    private static final int BASE_SIZE = 4;
    private static final int BYTES_PER_VALUE_SIZE = 1;
    private static final int FULL_VALUE_SIZE = 4;

    static byte[] encode(final List<Integer> values) {
        final int count = values.size();
        if (count == 0) {
            final ByteBuffer intBuf = ByteBuffer.allocate(COUNT_SIZE);
            intBuf.putInt(count);
            return intBuf.array();
        }

        final int lastValue = values.get(values.size() - 1);

        if (count == 1) {
            if (lastValue < 0) {
                throw new IllegalArgumentException("Values cannot be negative");
            }

            final ByteBuffer intBuf = ByteBuffer.allocate(COUNT_SIZE + FULL_VALUE_SIZE);
            intBuf.putInt(count);
            intBuf.putInt(lastValue);
            return intBuf.array();
        }

        final int min = values.stream()
            // Don't include the last value, it's encoded independently.
            .limit(values.size() - 1)
            .mapToInt(Integer::intValue)
            .min().getAsInt();
        if (min < 0 || lastValue < 0) {
            throw new IllegalArgumentException("Values cannot be negative");
        }
        final int base = min;

        final byte bytesPerValue = (byte) values.stream()
            .limit(values.size() - 1)
            .mapToInt(v -> bytesNeeded(v - base))
            .max().getAsInt();

        final int bufSize = COUNT_SIZE + BASE_SIZE + BYTES_PER_VALUE_SIZE + ((count - 1) * bytesPerValue)
            + FULL_VALUE_SIZE;  // last value in full
        final ByteBuffer buf = ByteBuffer.allocate(bufSize);

        buf.putInt(count);
        buf.putInt(base);
        buf.put(bytesPerValue);

        final ByteBuffer intBuf = ByteBuffer.allocate(FULL_VALUE_SIZE);
        // The buffer is big-endian.
        final int offset = FULL_VALUE_SIZE - bytesPerValue;
        for (int i = 0; i < values.size() - 1; i++) {
            final int value = values.get(i);
            final int onBase = value - base;
            intBuf.rewind();
            intBuf.putInt(onBase);
            buf.put(intBuf.array(), offset, bytesPerValue);
        }

        buf.putInt(lastValue);

        return buf.array();
    }

    private static byte bytesNeeded(final int v) {
        if (v <= 0xFF) {
            return 1;
        } else if (v <= 0xFFFF) {
            return 2;
        } else if (v <= 0xFFFFFF) {
            return 3;
        } else {
            return 4;
        }
    }

    static List<Integer> decode(final byte[] array) {
        final ByteBuffer buf = ByteBuffer.wrap(array);
        final int count = buf.getInt();
        if (count == 0) {
            return List.of();
        }

        if (count == 1) {
            return List.of(buf.getInt());
        }

        final List<Integer> result = new ArrayList<>();
        final int base = buf.getInt();
        final byte bytesPerValue = buf.get();
        final ByteBuffer valBuf =  ByteBuffer.allocate(FULL_VALUE_SIZE);
        // The buffer is big-endian.
        final int offset = FULL_VALUE_SIZE - bytesPerValue;
        for (int i = 0; i < count - 1; i++) {
            buf.get(valBuf.array(), offset, bytesPerValue);
            valBuf.rewind();
            result.add(valBuf.getInt() + base);
        }

        result.add(buf.getInt());

        return result;
    }
}
