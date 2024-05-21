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

package io.aiven.kafka.tieredstorage.transform;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;

import io.github.bucket4j.Bucket;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class RateLimitedInputStreamTest {
    @Test
    void testDoesNotBlockRead() {
        // Given a bucket with min size of default buffer
        final Bucket bucket = RateLimitedInputStream.rateLimitBucket(1);
        // When a stream of size less than capacity is read
        final byte[] bytes = new byte[RateLimitedInputStream.MIN_RATE - 1];
        final ByteArrayInputStream source = new ByteArrayInputStream(bytes);
        final InputStream test = new RateLimitedInputStream(source, bucket);
        // Then read should happen without blocking, i.e. less than 1 sec
        await().atMost(Duration.ofSeconds(1))
            .until(() -> {
                test.readAllBytes();
                return true;
            });
    }

    @Test
    void testBlocksRead() {
        // Given a bucket with min size of default buffer
        final Bucket bucket = RateLimitedInputStream.rateLimitBucket(1);
        // When a stream of size larger than capacity is read
        final byte[] bytes = new byte[RateLimitedInputStream.MIN_RATE + 1];
        Arrays.fill(bytes, (byte) 0);
        final InputStream test = new RateLimitedInputStream(new ByteArrayInputStream(bytes), bucket);
        // Then read should block while bucket is refill; taking at least 1 sec but not more than 2
        await().atLeast(Duration.ofSeconds(1))
            .until(() -> {
                test.readAllBytes();
                return true;
            });
    }

    @Test
    void testBlocksOnSeparateStreams() {
        // Given a bucket with min size of default buffer
        final Bucket bucket = RateLimitedInputStream.rateLimitBucket(1);
        // When 2 streams with less than buffer size
        final byte[] bytes0 = new byte[RateLimitedInputStream.MIN_RATE - 1];
        Arrays.fill(bytes0, (byte) 0);
        final InputStream test0 = new RateLimitedInputStream(new ByteArrayInputStream(bytes0), bucket);
        final byte[] bytes1 = new byte[RateLimitedInputStream.MIN_RATE - 1];
        Arrays.fill(bytes1, (byte) 0);
        final InputStream test1 = new RateLimitedInputStream(new ByteArrayInputStream(bytes1), bucket);
        // Then read should not block on first stream
        await().atMost(Duration.ofSeconds(1))
            .until(() -> {
                test0.readAllBytes();
                return true;
            });
        // but should block on the second one for a second to refill bucket consumed by first stream
        // minus 100 to account for some timing skew in between runs
        await().atLeast(Duration.ofSeconds(1).minusMillis(100))
            .until(() -> {
                test1.readAllBytes();
                return true;
            });
    }

    @Test
    void testAllNecessaryCallsAreRateLimited() throws IOException {
        // bucket never used on single byte reads
        Bucket bucket = spy(RateLimitedInputStream.rateLimitBucket(1));
        var rateLimitedInputStream = new RateLimitedInputStream(new ByteArrayInputStream(new byte[100]), bucket);
        rateLimitedInputStream.read();
        verify(bucket, Mockito.never()).asBlocking();

        // bucket only used on reads by range
        final var buf = new byte[10];
        bucket = spy(RateLimitedInputStream.rateLimitBucket(1));
        rateLimitedInputStream = new RateLimitedInputStream(new ByteArrayInputStream(new byte[100]), bucket);
        rateLimitedInputStream.read(buf);
        verify(bucket).asBlocking();

        bucket = spy(RateLimitedInputStream.rateLimitBucket(1));
        rateLimitedInputStream = new RateLimitedInputStream(new ByteArrayInputStream(new byte[100]), bucket);
        rateLimitedInputStream.read(buf, 0, 10);
        verify(bucket).asBlocking();

        bucket = spy(RateLimitedInputStream.rateLimitBucket(1));
        rateLimitedInputStream = new RateLimitedInputStream(new ByteArrayInputStream(new byte[100]), bucket);
        rateLimitedInputStream.readNBytes(10);
        verify(bucket).asBlocking();

        bucket = spy(RateLimitedInputStream.rateLimitBucket(1));
        rateLimitedInputStream = new RateLimitedInputStream(new ByteArrayInputStream(new byte[100]), bucket);
        rateLimitedInputStream.readNBytes(buf, 0, 10);
        verify(bucket).asBlocking();

        bucket = spy(RateLimitedInputStream.rateLimitBucket(1));
        rateLimitedInputStream = new RateLimitedInputStream(new ByteArrayInputStream(new byte[100]), bucket);
        rateLimitedInputStream.readAllBytes();
        verify(bucket, atLeastOnce()).asBlocking();
    }
}
