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
import java.util.concurrent.atomic.AtomicInteger;

class RefCountedByteBuffer {
    private final ByteBuffer buffer;
    private final DirectByteBufferPool bufferPool;
    private final AtomicInteger refCount = new AtomicInteger(1);

    RefCountedByteBuffer(final ByteBuffer buffer, final DirectByteBufferPool bufferPool) {
        this.buffer = buffer;
        this.bufferPool = bufferPool;
    }

    ByteBuffer duplicate() {
        return buffer.asReadOnlyBuffer();
    }

    int capacity() {
        return buffer.capacity();
    }

    void retain() {
        final int count = refCount.incrementAndGet();
        if (count <= 1) {
            refCount.decrementAndGet();
            throw new IllegalStateException("Cannot retain a released buffer");
        }
    }

    void release() {
        final int count = refCount.decrementAndGet();
        if (count == 0) {
            bufferPool.returnBuffer(buffer);
        } else if (count < 0) {
            throw new IllegalStateException("Buffer already fully released");
        }
    }
}
