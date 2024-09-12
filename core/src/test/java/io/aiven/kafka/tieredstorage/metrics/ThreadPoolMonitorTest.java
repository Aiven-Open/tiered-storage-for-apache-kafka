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

package io.aiven.kafka.tieredstorage.metrics;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.awaitility.Awaitility.await;

class ThreadPoolMonitorTest {

    @Test
    void failOnNotForkJoin() {
        assertThatThrownBy(() -> new ThreadPoolMonitor("test", new ScheduledThreadPoolExecutor(1)));
    }

    static final MBeanServer MBEAN_SERVER = ManagementFactory.getPlatformMBeanServer();

    @Test
    void collectMetrics() throws Exception {
        final var pool = new ForkJoinPool(2);
        new ThreadPoolMonitor("test", pool);

        final var threadPoolObjectName =
            new ObjectName("aiven.kafka.server.tieredstorage.thread-pool:type=test");

        // initial values
        assertThat(MBEAN_SERVER.getAttribute(threadPoolObjectName, "active-thread-count-total"))
            .isEqualTo(0.0);
        assertThat(MBEAN_SERVER.getAttribute(threadPoolObjectName, "running-thread-count-total"))
            .isEqualTo(0.0);
        assertThat(MBEAN_SERVER.getAttribute(threadPoolObjectName, "parallelism-total"))
            .isEqualTo(2.0);
        assertThat(MBEAN_SERVER.getAttribute(threadPoolObjectName, "pool-size-total"))
            .isEqualTo(0.0);
        assertThat(MBEAN_SERVER.getAttribute(threadPoolObjectName, "steal-task-count-total"))
            .isEqualTo(0.0);
        assertThat(MBEAN_SERVER.getAttribute(threadPoolObjectName, "queued-task-count-total"))
            .isEqualTo(0.0);

        IntStream.range(0, 10).forEach(value -> pool.execute(() -> System.out.println("test:"+value)));

        // The following assertion are relaxed, just showing that values have changed;
        // but it's hard to account for specific numbers
        assertThat(MBEAN_SERVER.getAttribute(threadPoolObjectName, "active-thread-count-total"))
            .asInstanceOf(DOUBLE)
            .isGreaterThanOrEqualTo(1.0);
        assertThat(MBEAN_SERVER.getAttribute(threadPoolObjectName, "running-thread-count-total"))
            .asInstanceOf(DOUBLE)
            .isGreaterThanOrEqualTo(0.0);
        assertThat(MBEAN_SERVER.getAttribute(threadPoolObjectName, "pool-size-total"))
            .asInstanceOf(DOUBLE)
            .isGreaterThanOrEqualTo(0.0);
        await()
            .atMost(Duration.ofSeconds(5))
            .untilAsserted(() ->
                assertThat(MBEAN_SERVER.getAttribute(threadPoolObjectName, "queued-task-count-total"))
                    .isEqualTo(0.0));
        assertThat(MBEAN_SERVER.getAttribute(threadPoolObjectName, "steal-task-count-total"))
            .asInstanceOf(DOUBLE)
            .isGreaterThan(0.0);
    }
}
