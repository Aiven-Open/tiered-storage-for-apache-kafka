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

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TimeToFirstByteMeasuringInputStreamTest {
    @Mock
    Time time;
    @Mock
    Sensor sensor;
    TimeToFirstByteMeasuringInputStream testInputStream;

    @BeforeEach
    void setup() {
        when(time.milliseconds())
            .thenReturn(0L)
            .thenReturn(3L);

        testInputStream = new TimeToFirstByteMeasuringInputStream(
            new ByteArrayInputStream(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}),
            time, sensor);
    }

    @Test
    void testReadFirst() throws IOException {
        assertThat(testInputStream.available()).isEqualTo(10);

        assertThat(testInputStream.read()).isEqualTo(0);
        verify(sensor).record(3.0);

        assertThat(testInputStream.read()).isEqualTo(1);
        assertThat(testInputStream.readAllBytes()).contains(2, 3, 4, 5, 6, 7, 8, 9);
        assertThat(testInputStream.available()).isZero();
        assertThat(testInputStream.read()).isEqualTo(-1);
        verifyNoMoreInteractions(sensor);
    }

    @Test
    void testReadArrayFirst() throws IOException {
        assertThat(testInputStream.available()).isEqualTo(10);

        final byte[] b = new byte[3];
        assertThat(testInputStream.read(b)).isEqualTo(3);
        assertThat(b).contains(0, 1, 2);
        verify(sensor).record(3.0);

        assertThat(testInputStream.read()).isEqualTo(3);
        assertThat(testInputStream.readAllBytes()).contains(4, 5, 6, 7, 8, 9);
        assertThat(testInputStream.available()).isZero();
        assertThat(testInputStream.read()).isEqualTo(-1);
        verifyNoMoreInteractions(sensor);
    }

    @Test
    void testReadArrayWithOffsetAndLengthFirst() throws IOException {
        assertThat(testInputStream.available()).isEqualTo(10);

        final byte[] b = new byte[3];
        assertThat(testInputStream.read(b, 0, 3)).isEqualTo(3);
        assertThat(b).contains(0, 1, 2);
        verify(sensor).record(3.0);

        assertThat(testInputStream.read()).isEqualTo(3);
        assertThat(testInputStream.readAllBytes()).contains(4, 5, 6, 7, 8, 9);
        assertThat(testInputStream.available()).isZero();
        assertThat(testInputStream.read()).isEqualTo(-1);
        verifyNoMoreInteractions(sensor);
    }

    @Test
    void testReadAllBytesFirst() throws IOException {
        assertThat(testInputStream.available()).isEqualTo(10);

        assertThat(testInputStream.readAllBytes()).contains(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        verify(sensor).record(3.0);

        assertThat(testInputStream.read()).isEqualTo(-1);
        assertThat(testInputStream.readAllBytes()).isEmpty();
        assertThat(testInputStream.available()).isZero();
        assertThat(testInputStream.read()).isEqualTo(-1);
        verifyNoMoreInteractions(sensor);
    }

    @Test
    void testReadNBytesFirst() throws IOException {
        assertThat(testInputStream.available()).isEqualTo(10);

        assertThat(testInputStream.readNBytes(3)).contains(0, 1, 2);
        verify(sensor).record(3.0);

        assertThat(testInputStream.read()).isEqualTo(3);
        assertThat(testInputStream.readAllBytes()).contains(4, 5, 6, 7, 8, 9);
        assertThat(testInputStream.available()).isZero();
        assertThat(testInputStream.read()).isEqualTo(-1);
        verifyNoMoreInteractions(sensor);
    }

    @Test
    void testReadNBytesWithOffsetAndLengthFirst() throws IOException {
        assertThat(testInputStream.available()).isEqualTo(10);

        final byte[] b = new byte[3];
        assertThat(testInputStream.readNBytes(b, 0, 3)).isEqualTo(3);
        assertThat(b).contains(0, 1, 2);
        verify(sensor).record(3.0);

        assertThat(testInputStream.read()).isEqualTo(3);
        assertThat(testInputStream.readAllBytes()).contains(4, 5, 6, 7, 8, 9);
        assertThat(testInputStream.available()).isZero();
        assertThat(testInputStream.read()).isEqualTo(-1);
        verifyNoMoreInteractions(sensor);
    }
}
