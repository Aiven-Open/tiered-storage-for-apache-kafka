/*
 * Copyright 2024 Aiven Oy
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

package io.aiven.kafka.tieredstorage.fetch.cache;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ByteBufferInputStreamTest {
    private static final int CHUNK_SIZE = 10;
    private static final byte[] CHUNK_0 = "0123456789".getBytes();

    @Test
    void readsAllBytes() throws IOException {
        try (InputStream is = new ByteBufferInputStream(newRefCountedBuffer(CHUNK_0))) {
            assertThat(is).hasBinaryContent(CHUNK_0);
        }
    }

    @Test
    void singleByteReadReturnsUnsignedByte() throws IOException {
        final RefCountedByteBuffer ref = newRefCountedBuffer(new byte[] {(byte) 0xFF, 0x00});
        try (InputStream is = new ByteBufferInputStream(ref)) {
            assertThat(is.read()).isEqualTo(0xFF);
            assertThat(is.read()).isZero();
            assertThat(is.read()).isEqualTo(-1);
        }
    }

    @Test
    void partialReadReturnsRequestedLength() throws IOException {
        try (InputStream is = new ByteBufferInputStream(newRefCountedBuffer(CHUNK_0))) {
            final byte[] dst = new byte[4];
            assertThat(is.read(dst, 0, 4)).isEqualTo(4);
            assertThat(dst).containsExactly('0', '1', '2', '3');
        }
    }

    @Test
    void zeroLengthReadReturnsZeroWhileDataRemains() throws IOException {
        // InputStream contract: a len==0 read must return 0 (not -1) even when data is available.
        // Pinning this guards the readByte-loop semantics used by Utils.readFully.
        try (InputStream is = new ByteBufferInputStream(newRefCountedBuffer(CHUNK_0))) {
            assertThat(is.read(new byte[4], 0, 0)).isZero();
            assertThat(is.available()).isEqualTo(CHUNK_0.length);
        }
    }

    @Test
    void skipAndAvailable() throws IOException {
        try (InputStream is = new ByteBufferInputStream(newRefCountedBuffer(CHUNK_0))) {
            assertThat(is.available()).isEqualTo(10);
            assertThat(is.skip(3)).isEqualTo(3);
            assertThat(is.available()).isEqualTo(7);
            assertThat(is.read()).isEqualTo('3');
            assertThat(is.skip(100)).isEqualTo(6); // clamped to remaining
            assertThat(is.read()).isEqualTo(-1);
        }
    }

    @Test
    void closeReleasesBufferToPool() throws IOException {
        final DirectByteBufferPool pool = new DirectByteBufferPool(CHUNK_SIZE, 16);
        final InputStream is = new ByteBufferInputStream(newRefCountedBuffer(pool, CHUNK_0));
        assertThat(pool.currentPoolSize()).isZero();
        is.close();
        assertThat(pool.currentPoolSize()).isEqualTo(1);
        // Reading after close yields EOF and close is idempotent.
        assertThat(is.read()).isEqualTo(-1);
        is.close();
        assertThat(pool.currentPoolSize()).isEqualTo(1);
    }

    private static RefCountedByteBuffer newRefCountedBuffer(final byte[] data) {
        return newRefCountedBuffer(new DirectByteBufferPool(data.length, 16), data);
    }

    private static RefCountedByteBuffer newRefCountedBuffer(final DirectByteBufferPool pool, final byte[] data) {
        final ByteBuffer buffer = pool.get(data.length).buffer();
        buffer.put(data);
        buffer.flip();
        return new RefCountedByteBuffer(buffer, pool);
    }
}
