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

package io.aiven.kafka.tieredstorage.iceberg.kafka;

import java.util.List;

/**
 * A Kafka log batch: the batch header fields and its records.
 * The types are the ones Kafka uses for these fields.
 */
public record KafkaBatch(
    long baseOffset,
    int partitionLeaderEpoch,
    byte magic,
    int timestampTypeId,
    byte compressionTypeId,
    long maxTimestamp,
    long producerId,
    short producerEpoch,
    int baseSequence,
    List<KafkaRecord> records
) {
}
