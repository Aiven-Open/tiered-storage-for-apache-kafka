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

import java.util.function.Supplier;

import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Value;

/**
 * Implementation of {@link Value} that allows fetching a value from provided {@code Long} {@link Supplier}
 * to avoid unnecessary calls to {@link Sensor#record()} that under the hood has a synchronized block and affects
 * performance because of that.
 */
class MeasurableValue extends Value {
    private final Supplier<Long> value;

    MeasurableValue(final Supplier<Long> value) {
        this.value = value;
    }

    @Override
    public double measure(final MetricConfig config, final long now) {
        return value.get();
    }
}
