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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;

import io.github.bucket4j.Bucket;

/**
 * Input Stream with a limited rate of reading from source.
 * Aims to provide a stable upload speed that does not affect other streams of work in a broker.
 * Rate limiting is only implemented for {@code InputStream#read(bytes, offset, len)} as rate limiting individual reads
 * is too expensive.
 */
public class RateLimitedInputStream extends FilterInputStream {
    static final int DEFAULT_BUFFER_SIZE = 8192;

    final Bucket bucket;

    public RateLimitedInputStream(final InputStream delegated, final Bucket bucket) {
        super(delegated);
        this.bucket = bucket;
    }

    public static Bucket rateLimitBucket(final int uploadRate) {
        final int rate = Math.max(uploadRate, DEFAULT_BUFFER_SIZE);
        return Bucket.builder()
            .addLimit(limit ->
                limit.capacity(rate)
                    // every 100ms a 10th of the chunk size is added back to bucket
                    .refillGreedy(rate, Duration.ofSeconds(1)))
            .build();
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        // only block when some bytes are requested
        if (len > 0) {
            try {
                bucket.asBlocking().consume(len);
            } catch (final InterruptedException e) {
                throw new RuntimeException("Rate limited consumption of input stream interrupted", e);
            }
        }

        // forward request
        final int read = super.read(b, off, len);

        // compensate for tokens buffered but not read
        if (read > -1) {
            // if number of bytes read is less than buffer, return tokens
            if (len > read) {
                bucket.forceAddTokens(len - read);
            }
        } else {
            // if stream is empty, return tokens
            if (len > 0) {
                bucket.forceAddTokens(len);
            }
        }

        return read;
    }

}
