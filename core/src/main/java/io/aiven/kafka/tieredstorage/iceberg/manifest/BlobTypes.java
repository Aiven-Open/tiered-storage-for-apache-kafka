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

package io.aiven.kafka.tieredstorage.iceberg.manifest;

interface BlobTypes {
    String OFFSET_INDEX = "aiven-tiered-storage-offset-index";
    String TIMESTAMP_INDEX = "aiven-tiered-storage-timestamp-index";
    String PRODUCER_SNAPSHOT_INDEX = "aiven-tiered-storage-producer-snapshot-index";
    String TRANSACTION_INDEX = "aiven-tiered-storage-transaction-index";
    String LEADER_EPOCH_INDEX = "aiven-tiered-storage-leader-epoch-index";
    String FILE_LIST = "aiven-tiered-storage-file-list";
}
