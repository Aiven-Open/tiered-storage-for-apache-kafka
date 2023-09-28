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

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;

import org.apache.kafka.common.utils.Time;

public class MeteredInputStream extends InputStream {
    final InputStream inner;
    final Time time;
    long readingTime = 0L;
    final Consumer<Long> recorder;

    public MeteredInputStream(final InputStream inner,
                              final Time time,
                              final Consumer<Long> recorder) {
        this.inner = inner;
        this.time = time;
        this.recorder = recorder;
    }

    @Override
    public int read() throws IOException {
        final var start = time.nanoseconds();
        try {
            return inner.read();
        } finally {
            readingTime += time.nanoseconds() - start;
        }
    }

    @Override
    public void close() throws IOException {
        recorder.accept(readingTime / 1_000_000); // to millis
        inner.close();
    }
}
