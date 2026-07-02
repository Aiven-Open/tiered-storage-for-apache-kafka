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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public class DirectByteBufferPool {
    private final int bufferSize;  // must equal with chunk size
    private final long maxPoolSize;  // should be chunk cache size / chunk size

    private final ConcurrentLinkedQueue<ByteBuffer> pool = new ConcurrentLinkedQueue<>();
    private final AtomicLong poolCount = new AtomicLong(0);
    private final AtomicLong activeCount = new AtomicLong(0);
    private final AtomicLong totalAllocatedBytes = new AtomicLong(0);

    public DirectByteBufferPool(final int bufferSize, final long maxPoolSize) {
        this.bufferSize = bufferSize;
        this.maxPoolSize = maxPoolSize;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    /**
     * Acquire a direct buffer of at least the requested size.
     * Returns a result indicating whether the buffer was reused from the pool or freshly allocated.
     */
    public PoolGetResult get(final int requestedSize) {
        activeCount.incrementAndGet();
        if (requestedSize != this.bufferSize) {
            totalAllocatedBytes.addAndGet(requestedSize);
            return new PoolGetResult(ByteBuffer.allocateDirect(requestedSize), false);
        }

        final ByteBuffer buf = pool.poll();
        if (buf != null) {
            poolCount.decrementAndGet();
            buf.clear();
            return new PoolGetResult(buf, true);
        }

        totalAllocatedBytes.addAndGet(this.bufferSize);
        return new PoolGetResult(ByteBuffer.allocateDirect(this.bufferSize), false);
    }

    public void returnBuffer(final ByteBuffer buf) {
        if (buf == null || !buf.isDirect()) {
            return;
        }

        activeCount.decrementAndGet();

        if (buf.capacity() != this.bufferSize) {
            return;
        }

        long current;
        do {
            current = poolCount.get();
            if (current >= maxPoolSize) {
                return;
            }
        } while (!poolCount.compareAndSet(current, current + 1));

        buf.clear();
        pool.offer(buf);
    }

    public long currentPoolSize() {
        return poolCount.get();
    }

    public long maxPoolSize() {
        return maxPoolSize;
    }

    public long activeCount() {
        return activeCount.get();
    }

    public long totalAllocatedBytes() {
        return totalAllocatedBytes.get();
    }

    public record PoolGetResult(ByteBuffer buffer, boolean poolHit) {
    }
}
