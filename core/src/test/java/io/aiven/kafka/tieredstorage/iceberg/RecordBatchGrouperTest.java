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

import io.aiven.kafka.tieredstorage.iceberg.data.RowSchema;

import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RecordBatchGrouperTest {
    @Test
    void empty() throws IOException {
        final MultiFileReader reader = mock(MultiFileReader.class);
        final RecordBatchGrouper grouper = new RecordBatchGrouper(reader);
        assertThat(grouper.nextBatch()).isNull();
    }

    @Test
    void singleBatch() throws IOException {
        final GenericData.Record record1 = recordWithBatchIdentity(100L, 1L, 0);
        final GenericData.Record record2 = recordWithBatchIdentity(100L, 1L, 0);
        final GenericData.Record record3 = recordWithBatchIdentity(100L, 1L, 0);

        final MultiFileReader reader = mock(MultiFileReader.class);
        when(reader.read()).thenReturn(record1, record2, record3, null);

        final RecordBatchGrouper grouper = new RecordBatchGrouper(reader);

        assertThat(grouper.nextBatch()).containsExactly(record1, record2, record3);
        assertThat(grouper.nextBatch()).isNull();
    }

    @Test
    void multipleBatches() throws IOException {
        final GenericData.Record record1 = recordWithBatchIdentity(100L, 1L, 0);
        final GenericData.Record record2 = recordWithBatchIdentity(100L, 1L, 0);
        final GenericData.Record record3 = recordWithBatchIdentity(150L, 1L, 1);
        final GenericData.Record record4 = recordWithBatchIdentity(200L, 1L, 2);

        final MultiFileReader reader = mock(MultiFileReader.class);
        when(reader.read()).thenReturn(record1, record2, record3, record4, null);

        final RecordBatchGrouper grouper = new RecordBatchGrouper(reader);
        assertThat(grouper.nextBatch()).containsExactly(record1, record2);
        assertThat(grouper.nextBatch()).containsExactly(record3);
        assertThat(grouper.nextBatch()).containsExactly(record4);
        assertThat(grouper.nextBatch()).isNull();
    }

    /**
     * Tests that if a previous batch reoccurs, we still return everything correctly.
     * Not expected to happen for real.
     */
    @Test
    void returningToPreviousBatches() throws IOException {
        final GenericData.Record record1 = recordWithBatchIdentity(100L, 1L, 0);
        final GenericData.Record record2 = recordWithBatchIdentity(150L, 1L, 1);
        final GenericData.Record record3 = recordWithBatchIdentity(150L, 1L, 1);
        final GenericData.Record record4 = recordWithBatchIdentity(100L, 1L, 0);

        final MultiFileReader reader = mock(MultiFileReader.class);
        when(reader.read()).thenReturn(record1, record2, record3, record4, null);

        final RecordBatchGrouper grouper = new RecordBatchGrouper(reader);
        assertThat(grouper.nextBatch()).containsExactly(record1);
        assertThat(grouper.nextBatch()).containsExactly(record2, record3);
        assertThat(grouper.nextBatch()).containsExactly(record4);
        assertThat(grouper.nextBatch()).isNull();
    }

    private GenericData.Record recordWithBatchIdentity(
        final long baseOffset,
        final long producerId,
        final int baseSequence
    ) {
        final GenericData.Record kafka = mock(GenericData.Record.class);
        when(kafka.get(eq(RowSchema.Fields.BATCH_BASE_OFFSET))).thenReturn(baseOffset);
        when(kafka.get(eq(RowSchema.Fields.BATCH_PRODUCER_ID))).thenReturn(producerId);
        when(kafka.get(eq(RowSchema.Fields.BATCH_BASE_SEQUENCE))).thenReturn(baseSequence);
        final GenericData.Record record = mock(GenericData.Record.class);
        when(record.get(eq(RowSchema.Fields.KAFKA))).thenReturn(kafka);
        return record;
    }
}
