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

package io.aiven.kafka.tieredstorage.manifest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType;

public class SegmentIndexesV1Builder {
    private final Map<IndexType, SegmentIndexV1> indexes = new HashMap<>(IndexType.values().length);
    private int currentPosition = 0;

    public SegmentIndexesV1Builder add(final IndexType indexType, final int size) {
        if (indexes.containsKey(indexType)) {
            throw new IllegalStateException("Index " + indexType + " is already added");
        }
        indexes.put(indexType, new SegmentIndexV1(currentPosition, size));
        currentPosition += size;
        return this;
    }

    // for testing and logging purposes
    public List<IndexType> indexes() {
        final var indexTypes = new ArrayList<>(indexes.keySet());
        Collections.sort(indexTypes);
        return indexTypes;
    }

    public SegmentIndexesV1 build() {
        if (indexes.size() < 4) {
            throw new IllegalStateException("Not enough indexes have been added; at least 4 required. "
                + "Indexes included: " + indexes());
        }
        if (indexes.size() == 4 && indexes.containsKey(IndexType.TRANSACTION)) {
            throw new IllegalStateException("OFFSET, TIMESTAMP, PRODUCER_SNAPSHOT, "
                + "and LEADER_EPOCH indexes are required");
        }
        return new SegmentIndexesV1(
            indexes.get(IndexType.OFFSET),
            indexes.get(IndexType.TIMESTAMP),
            indexes.get(IndexType.PRODUCER_SNAPSHOT),
            indexes.get(IndexType.LEADER_EPOCH),
            indexes.get(IndexType.TRANSACTION)
        );
    }
}
