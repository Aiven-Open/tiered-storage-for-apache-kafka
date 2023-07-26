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