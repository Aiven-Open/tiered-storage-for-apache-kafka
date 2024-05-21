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
import java.io.InputStream;
import java.time.Duration;

import io.github.bucket4j.Bucket;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class RateLimitedInputStreamTest {
    @Test
    void testDoesNotBlockRead() {
        final Bucket bucket = Bucket.builder()
            .addLimit(limit -> limit.capacity(3).refillGreedy(1, Duration.ofSeconds(1)))
            .build();
        final byte[] bytes = "ABC".getBytes();
        final ByteArrayInputStream source = new ByteArrayInputStream(bytes);
        final InputStream test = new RateLimitedInputStream(source, bucket);
        final int[] ints = new int[3];
        await().atMost(Duration.ofSeconds(2))
            .until(() -> {
                ints[0] = test.read();
                ints[1] = test.read();
                ints[2] = test.read();
                return true;
            });
        assertThat(bytes).contains(ints);
    }

    @Test
    void testBlocksRead() {
        final Bucket bucket = Bucket.builder()
            .addLimit(limit -> limit.capacity(1).refillGreedy(1, Duration.ofSeconds(1)))
            .build();
        final byte[] bytes = "ABC".getBytes();
        final InputStream test = new RateLimitedInputStream(new ByteArrayInputStream(bytes), bucket);
        final int[] ints = new int[3];
        await().between(Duration.ofSeconds(2), Duration.ofSeconds(3))
            .until(() -> {
                ints[0] = test.read();
                ints[1] = test.read();
                ints[2] = test.read();
                return true;
            });
        assertThat(bytes).contains(ints);
    }

    @Test
    void testBlocksReadAll() {
        final Bucket bucket = Bucket.builder()
            .addLimit(limit -> limit.capacity(3).refillGreedy(3, Duration.ofSeconds(1)))
            .build();
        final byte[] bytes = "ABC".getBytes();
        final InputStream test0 = new RateLimitedInputStream(new ByteArrayInputStream(bytes), bucket);
        final InputStream test1 = new RateLimitedInputStream(new ByteArrayInputStream(bytes), bucket);
        await().atMost(Duration.ofSeconds(1))
            .until(() -> {
                test0.readAllBytes();
                return true;
            });

        await().between(Duration.ofSeconds(1), Duration.ofSeconds(2))
            .until(() -> {
                test1.readAllBytes();
                return true;
            });
    }

}
