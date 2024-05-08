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

package io.aiven.kafka.tieredstorage;

import java.io.IOException;
import java.io.InputStream;

import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ClosableInputStreamHolderTest {
    @Test
    void closeAll() throws IOException {
        final InputStream is1 = mock(InputStream.class);
        final InputStream is2 = mock(InputStream.class);
        final InputStream is3 = mock(InputStream.class);
        try (final var holder = new ClosableInputStreamHolder()) {
            holder.add(is1);
            holder.add(is2);
            holder.add(is3);
        }
        verify(is1).close();
        verify(is2).close();
        verify(is3).close();
    }

    @Test
    void closeAllEvenWithErrors() throws IOException {
        final InputStream is1 = mock(InputStream.class);
        final InputStream is2 = mock(InputStream.class);
        doThrow(new IOException("test")).when(is2).close();
        final InputStream is3 = mock(InputStream.class);

        try (final var holder = new ClosableInputStreamHolder()) {
            holder.add(is1);
            holder.add(is2);
            holder.add(is3);
        }
        verify(is1).close();
        verify(is2).close();
        verify(is3).close();
    }
}
