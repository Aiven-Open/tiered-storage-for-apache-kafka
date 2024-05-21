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

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;

import io.github.bucket4j.Bucket;

/**
 * Input Stream with a limited rate of reading from source.
 * Aims to provide a stable upload speed that does not affect other streams of work in a broker.
 */
public class RateLimitedInputStream extends InputStream {
    private final InputStream delegated;
    final Bucket bucket;

    public RateLimitedInputStream(final InputStream delegated, final Bucket bucket) {
        this.delegated = delegated;
        this.bucket = bucket;
    }

    public static Bucket rateLimitBucket(final int uploadRate) {
        return Bucket.builder()
            .addLimit(limit ->
                limit.capacity(uploadRate)
                    // every 100ms a 10th of the chunk size is added back to bucket
                    .refillGreedy(uploadRate, Duration.ofSeconds(1)))
            .build();
    }

    private void consumeRate(final int rate) {
        try {
            bucket.asBlocking().consume(rate);
        } catch (final InterruptedException e) {
            throw new RuntimeException("Rate limited consumption of input stream interrupted", e);
        }
    }

    @Override
    public int read() throws IOException {
        consumeRate(1);
        return delegated.read();
    }

    @Override
    public int read(final byte[] b) throws IOException {
        consumeRate(b.length);
        return delegated.read(b);
    }

}
