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

package io.aiven.kafka.tieredstorage;

import java.io.File;
import java.io.IOException;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.RecordBatch;

/**
 * Checks if segment are compressed or not.
 * To be used when segment files are received on archival.
 */
public class SegmentCompressionChecker {

    /**
     * @param file Kafka log segment file
     * @return true if log segment is compressed, otherwise returns false
     */
    public static boolean check(final File file) throws InvalidRecordBatchException {
        try (final FileRecords records = FileRecords.open(file, false, true, 0, false)) {
            final RecordBatch batch = fistRecordBatch(records);
            return batch.compressionType() != CompressionType.NONE;
        } catch (final IOException | KafkaException e) {
            throw new InvalidRecordBatchException("Failed to read and validate first batch", e);
        }
    }

    private static RecordBatch fistRecordBatch(final FileRecords records) throws InvalidRecordBatchException {
        final RecordBatch batch = records.firstBatch();
        if (batch == null) {
            throw new InvalidRecordBatchException("Record batch is null");
        }
        batch.ensureValid();
        return batch;
    }
}
