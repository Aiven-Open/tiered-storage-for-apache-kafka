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

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RefCountedByteBufferTest {
    private static final int CHUNK_SIZE = 10;
    private static final byte[] CHUNK_0 = "0123456789".getBytes();

    @Test
    void releaseReturnsBufferToPoolExactlyOnce() {
        final DirectByteBufferPool pool = new DirectByteBufferPool(CHUNK_SIZE, 16);
        final RefCountedByteBuffer ref = newRefCountedBuffer(pool, CHUNK_0);

        assertThat(pool.currentPoolSize()).isZero();
        ref.release(); // initial refcount of 1 -> 0, returns to pool
        assertThat(pool.currentPoolSize()).isEqualTo(1);
    }

    @Test
    void retainPreventsReturnUntilAllReleased() {
        final DirectByteBufferPool pool = new DirectByteBufferPool(CHUNK_SIZE, 16);
        final RefCountedByteBuffer ref = newRefCountedBuffer(pool, CHUNK_0);

        ref.retain(); // 1 -> 2
        ref.release(); // 2 -> 1, not yet returned
        assertThat(pool.currentPoolSize()).isZero();
        ref.release(); // 1 -> 0, returned
        assertThat(pool.currentPoolSize()).isEqualTo(1);
    }

    @Test
    void retainAfterFullReleaseThrows() {
        final RefCountedByteBuffer ref = newRefCountedBuffer(CHUNK_0);
        ref.release(); // -> 0
        assertThatThrownBy(ref::retain)
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Cannot retain a released buffer");
    }

    @Test
    void overReleaseThrows() {
        final RefCountedByteBuffer ref = newRefCountedBuffer(CHUNK_0);
        ref.release(); // -> 0
        assertThatThrownBy(ref::release)
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Buffer already fully released");
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
