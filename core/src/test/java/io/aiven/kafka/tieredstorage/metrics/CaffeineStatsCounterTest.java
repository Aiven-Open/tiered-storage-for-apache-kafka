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

package io.aiven.kafka.tieredstorage.metrics;

import com.github.benmanes.caffeine.cache.RemovalCause;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CaffeineStatsCounterTest {

    @Test
    void validEmpty() {
        final var counter = new CaffeineStatsCounter("test");
        assertThat(counter.toString())
            .isEqualTo("CacheStats{"
                + "hitCount=0, missCount=0, "
                + "loadSuccessCount=0, loadFailureCount=0, totalLoadTime=0, "
                + "evictionCount=0, evictionWeight=0}");
    }

    @Test
    void validFull() {
        final var counter = new CaffeineStatsCounter("test");
        counter.recordHits(1);
        counter.recordMisses(2);
        counter.recordLoadSuccess(100);
        counter.recordLoadFailure(10);
        counter.recordEviction(10, RemovalCause.EXPIRED);
        assertThat(counter.toString())
            .isEqualTo("CacheStats{"
                + "hitCount=1, missCount=2, "
                + "loadSuccessCount=1, loadFailureCount=1, totalLoadTime=110, "
                + "evictionCount=1, evictionWeight=10}");
    }
}
