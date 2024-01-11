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

package io.aiven.kafka.tieredstorage.fetch.index;

import java.util.Objects;

import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import io.aiven.kafka.tieredstorage.storage.ObjectKey;

public class SegmentIndexKey {
    public final ObjectKey indexesKey;
    public final RemoteStorageManager.IndexType indexType;

    public SegmentIndexKey(final ObjectKey indexesKey, final RemoteStorageManager.IndexType indexType) {
        this.indexesKey = indexesKey;
        this.indexType = indexType;
    }

    @Override
    public String toString() {
        return "SegmentIndexKey{"
            + "indexesKey=" + indexesKey
            + ", indexType=" + indexType
            + '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SegmentIndexKey that = (SegmentIndexKey) o;
        return Objects.equals(indexesKey, that.indexesKey) && indexType == that.indexType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexesKey, indexType);
    }
}
