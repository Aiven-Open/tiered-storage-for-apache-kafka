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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Time;

/**
 * The input stream that measures the time to first byte.
 *
 * <p>The time is measured on {@link #read()}, {@link #read(byte[], int, int)}, and {@link #skip(long)}.
 * Otherwise, the class delegates everything to the inner {@link InputStream}.
 */
public class TimeToFirstByteMeasuringInputStream extends FilterInputStream {
    private final Time time;
    private final Sensor sensor;

    private boolean firstByteArrived = false;

    public TimeToFirstByteMeasuringInputStream(final InputStream in,
                                               final Time time,
                                               final Sensor sensor) {
        super(Objects.requireNonNull(in, "in cannot be null"));
        this.time = Objects.requireNonNull(time, "time cannot be null");
        this.sensor = Objects.requireNonNull(sensor, "sensor cannot be null");
    }

    @Override
    public int read() throws IOException {
        if (firstByteArrived) {
            return super.read();
        }

        final long startMs = time.milliseconds();
        firstByteArrived = true;
        final var r = super.read();
        sensor.record(time.milliseconds() - startMs);
        return r;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        if (firstByteArrived) {
            return super.read(b, off, len);
        }

        final long startMs = time.milliseconds();
        firstByteArrived = true;
        final var r = super.read(b, off, len);
        sensor.record(time.milliseconds() - startMs);
        return r;
    }

    @Override
    public long skip(final long n) throws IOException {
        if (firstByteArrived) {
            return super.skip(n);
        }

        final long startMs = time.milliseconds();
        firstByteArrived = true;
        final var r = super.skip(n);
        sensor.record(time.milliseconds() - startMs);
        return r;
    }
}
