/*
 * Copyright 2025 Aiven Oy
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

package io.aiven.kafka.tieredstorage.iceberg;

import java.io.IOException;
import java.util.List;

import io.aiven.kafka.tieredstorage.iceberg.manifest.DataFileMetadata;

import org.apache.avro.generic.GenericData;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MultiFileReaderTest {
    @Mock
    GenericData.Record record1;
    @Mock
    GenericData.Record record2;
    @Mock
    GenericData.Record record3;
    @Mock
    GenericData.Record record4;
    @Mock
    GenericData.Record record5;
    @Mock
    GenericData.Record record6;
    @Mock
    GenericData.Record record7;
    @Mock
    GenericData.Record record8;
    @Mock
    GenericData.Record record9;

    @Test
    void normalSequentialRead() throws IOException {
        final var iterable1 = mock(CloseableIterable.class);
        final var iterator1 = mock(CloseableIterator.class);
        when(iterable1.iterator()).thenReturn(iterator1);
        when(iterator1.hasNext()).thenReturn(true, true, true, false);
        when(iterator1.next()).thenReturn(record1, record2, record3, null);
        final var iterable2 = mock(CloseableIterable.class);
        final var iterator2 = mock(CloseableIterator.class);
        when(iterable2.iterator()).thenReturn(iterator2);
        when(iterator2.hasNext()).thenReturn(true, true, true, false);
        when(iterator2.next()).thenReturn(record4, record5, record6, null);
        final var iterable3 = mock(CloseableIterable.class);
        final var iterator3 = mock(CloseableIterator.class);
        when(iterable3.iterator()).thenReturn(iterator3);
        when(iterator3.hasNext()).thenReturn(true, true, true, false);
        when(iterator3.next()).thenReturn(record7, record8, record9, null);

        final MultiFileReader.ReaderFactory readerFactory = mock(MultiFileReader.ReaderFactory.class);
        final DataFileMetadata f1 = new DataFileMetadata("f1", 0, 0, 0, 0);
        final DataFileMetadata f2 = new DataFileMetadata("f2", 0, 0, 0, 0);
        final DataFileMetadata f3 = new DataFileMetadata("f3", 0, 0, 0, 0);
        when(readerFactory.create(eq(f1))).thenReturn(iterable1);
        when(readerFactory.create(eq(f2))).thenReturn(iterable2);
        when(readerFactory.create(eq(f3))).thenReturn(iterable3);

        final MultiFileReader reader = new MultiFileReader(List.of(f1, f2, f3), readerFactory);
        assertThat(reader.read()).isSameAs(record1);
        assertThat(reader.read()).isSameAs(record2);
        assertThat(reader.read()).isSameAs(record3);
        assertThat(reader.read()).isSameAs(record4);
        assertThat(reader.read()).isSameAs(record5);
        assertThat(reader.read()).isSameAs(record6);
        assertThat(reader.read()).isSameAs(record7);
        assertThat(reader.read()).isSameAs(record8);
        assertThat(reader.read()).isSameAs(record9);
        assertThat(reader.read()).isNull();
        assertThat(reader.read()).isNull();

        verify(iterable1).close();
        verify(iterable2).close();
        verify(iterable3).close();

        reader.close();

        // All the readers are already closed.
        verifyNoMoreInteractions(iterator1);
        verifyNoMoreInteractions(iterator2);
        verifyNoMoreInteractions(iterator3);
    }

    @Test
    void singleLocation() throws IOException {
        final var iterable1 = mock(CloseableIterable.class);
        final var iterator1 = mock(CloseableIterator.class);
        when(iterable1.iterator()).thenReturn(iterator1);
        when(iterator1.hasNext()).thenReturn(true, true, true, false);
        when(iterator1.next()).thenReturn(record1, record2, record3, null);

        final MultiFileReader.ReaderFactory readerFactory = mock(MultiFileReader.ReaderFactory.class);
        final DataFileMetadata singleFile = new DataFileMetadata("single_file", 0, 0, 0, 0);
        when(readerFactory.create(eq(singleFile))).thenReturn(iterable1);

        final MultiFileReader reader = new MultiFileReader(List.of(singleFile), readerFactory);
        assertThat(reader.read()).isSameAs(record1);
        assertThat(reader.read()).isSameAs(record2);
        assertThat(reader.read()).isSameAs(record3);
        assertThat(reader.read()).isNull();
        assertThat(reader.read()).isNull();

        verify(iterable1).close();

        reader.close();

        // All the readers are already closed.
        verifyNoMoreInteractions(iterator1);
    }

    @Test
    void emptyListOfLocations() throws IOException {
        final MultiFileReader.ReaderFactory readerFactory = mock(MultiFileReader.ReaderFactory.class);
        final MultiFileReader reader = new MultiFileReader(List.of(), readerFactory);
        assertThat(reader.read()).isNull();
        assertThat(reader.read()).isNull();

        verify(readerFactory, never()).create(any(DataFileMetadata.class));

        reader.close();
    }

    @Test
    void allEmptyReaders() throws IOException {
        final var iterable1 = mock(CloseableIterable.class);
        final var iterator1 = mock(CloseableIterator.class);
        when(iterable1.iterator()).thenReturn(iterator1);
        when(iterator1.hasNext()).thenReturn(false);
        final var iterable2 = mock(CloseableIterable.class);
        final var iterator2 = mock(CloseableIterator.class);
        when(iterable2.iterator()).thenReturn(iterator2);
        when(iterator2.hasNext()).thenReturn(false);
        final var iterable3 = mock(CloseableIterable.class);
        final var iterator3 = mock(CloseableIterator.class);
        when(iterable3.iterator()).thenReturn(iterator3);
        when(iterator3.hasNext()).thenReturn(false);

        final MultiFileReader.ReaderFactory readerFactory = mock(MultiFileReader.ReaderFactory.class);
        final DataFileMetadata f1 = new DataFileMetadata("f1", 0, 0, 0, 0);
        final DataFileMetadata f2 = new DataFileMetadata("f2", 0, 0, 0, 0);
        final DataFileMetadata f3 = new DataFileMetadata("f3", 0, 0, 0, 0);

        when(readerFactory.create(eq(f1))).thenReturn(iterable1);
        when(readerFactory.create(eq(f2))).thenReturn(iterable2);
        when(readerFactory.create(eq(f3))).thenReturn(iterable3);

        final MultiFileReader reader = new MultiFileReader(List.of(f1, f2, f3), readerFactory);
        assertThat(reader.read()).isNull();
        assertThat(reader.read()).isNull();

        verify(iterable1).close();
        verify(iterable2).close();
        verify(iterable3).close();

        reader.close();

        // All the readers are already closed.
        verifyNoMoreInteractions(iterator1);
        verifyNoMoreInteractions(iterator2);
        verifyNoMoreInteractions(iterator3);
    }

    @Test
    void someEmptyReaders() throws IOException {
        final var iterable1 = mock(CloseableIterable.class);
        final var iterator1 = mock(CloseableIterator.class);
        when(iterable1.iterator()).thenReturn(iterator1);
        when(iterator1.hasNext()).thenReturn(false);

        final var iterable2 = mock(CloseableIterable.class);
        final var iterator2 = mock(CloseableIterator.class);
        when(iterable2.iterator()).thenReturn(iterator2);
        when(iterator2.hasNext()).thenReturn(true, true, false);
        when(iterator2.next()).thenReturn(record1, record2, null);

        final var iterable3 = mock(CloseableIterable.class);
        final var iterator3 = mock(CloseableIterator.class);
        when(iterable3.iterator()).thenReturn(iterator3);
        when(iterator3.hasNext()).thenReturn(false);

        final var iterable4 = mock(CloseableIterable.class);
        final var iterator4 = mock(CloseableIterator.class);
        when(iterable4.iterator()).thenReturn(iterator4);
        when(iterator4.hasNext()).thenReturn(true, true, false);
        when(iterator4.next()).thenReturn(record3, record4, null);

        final var iterable5 = mock(CloseableIterable.class);
        final var iterator5 = mock(CloseableIterator.class);
        when(iterable5.iterator()).thenReturn(iterator5);
        when(iterator5.hasNext()).thenReturn(false);

        final MultiFileReader.ReaderFactory readerFactory = mock(MultiFileReader.ReaderFactory.class);
        final DataFileMetadata f1 = new DataFileMetadata("f1", 0, 0, 0, 0);
        final DataFileMetadata f2 = new DataFileMetadata("f2", 0, 0, 0, 0);
        final DataFileMetadata f3 = new DataFileMetadata("f3", 0, 0, 0, 0);
        final DataFileMetadata f4 = new DataFileMetadata("f4", 0, 0, 0, 0);
        final DataFileMetadata f5 = new DataFileMetadata("f5", 0, 0, 0, 0);
        when(readerFactory.create(eq(f1))).thenReturn(iterable1);
        when(readerFactory.create(eq(f2))).thenReturn(iterable2);
        when(readerFactory.create(eq(f3))).thenReturn(iterable3);
        when(readerFactory.create(eq(f4))).thenReturn(iterable4);
        when(readerFactory.create(eq(f5))).thenReturn(iterable5);

        final MultiFileReader reader = new MultiFileReader(List.of(f1, f2, f3, f4, f5), readerFactory);
        assertThat(reader.read()).isSameAs(record1);
        assertThat(reader.read()).isSameAs(record2);
        assertThat(reader.read()).isSameAs(record3);
        assertThat(reader.read()).isSameAs(record4);
        assertThat(reader.read()).isNull();
        assertThat(reader.read()).isNull();

        verify(iterable1).close();
        verify(iterable2).close();
        verify(iterable3).close();
        verify(iterable4).close();
        verify(iterable5).close();

        reader.close();

        // All the readers are already closed.
        verifyNoMoreInteractions(iterable1);
        verifyNoMoreInteractions(iterable2);
        verifyNoMoreInteractions(iterable3);
        verifyNoMoreInteractions(iterable4);
        verifyNoMoreInteractions(iterable5);
    }

    @Test
    void closeInTheMiddle() throws IOException {
        final var iterable1 = mock(CloseableIterable.class);
        final var iterator1 = mock(CloseableIterator.class);
        when(iterable1.iterator()).thenReturn(iterator1);
        when(iterator1.hasNext()).thenReturn(true, true, false);
        when(iterator1.next()).thenReturn(record1, record2, null);

        final var iterable2 = mock(CloseableIterable.class);

        final MultiFileReader.ReaderFactory readerFactory = mock(MultiFileReader.ReaderFactory.class);
        final DataFileMetadata singleFile = new DataFileMetadata("single_file", 0, 0, 0, 0);
        when(readerFactory.create(eq(singleFile))).thenReturn(iterable1, iterable2);

        final MultiFileReader reader = new MultiFileReader(List.of(singleFile), readerFactory);
        assertThat(reader.read()).isSameAs(record1);

        reader.close();

        verify(iterable1).close();
        verifyNoMoreInteractions(iterable2);
    }

    @Test
    void readerInitFailure() throws IOException {
        final MultiFileReader.ReaderFactory readerFactory = mock(MultiFileReader.ReaderFactory.class);
        final IOException exception = new IOException("test");
        final DataFileMetadata singleFile = new DataFileMetadata("single_file", 0, 0, 0, 0);
        when(readerFactory.create(eq(singleFile))).thenThrow(exception);

        final MultiFileReader reader = new MultiFileReader(List.of(singleFile), readerFactory);
        assertThatThrownBy(reader::read).isSameAs(exception);
    }

    @Test
    void readFailure() throws IOException {
        final RuntimeException exception = new RuntimeException("test");

        final var iterable1 = mock(CloseableIterable.class);
        final var iterator1 = mock(CloseableIterator.class);
        when(iterable1.iterator()).thenReturn(iterator1);
        when(iterator1.hasNext()).thenThrow(exception);

        final MultiFileReader.ReaderFactory readerFactory = mock(MultiFileReader.ReaderFactory.class);
        final DataFileMetadata singleFile = new DataFileMetadata("single_file", 0, 0, 0, 0);
        when(readerFactory.create(eq(singleFile))).thenReturn(iterable1);

        final MultiFileReader reader = new MultiFileReader(List.of(singleFile), readerFactory);
        assertThatThrownBy(reader::read).isSameAs(exception);
    }
}
