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

package io.aiven.kafka.tiered.storage.commons.chunkmanager;

import java.util.Objects;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;

public class ChunkKey {
    private final Uuid uuid;
    private final int chunkId;

    /**
     * @param uuid segment UUID, see {@link RemoteLogSegmentId#id()}.
     */
    public ChunkKey(final Uuid uuid, final int chunkId) {
        this.uuid = Objects.requireNonNull(uuid, "uuid cannot be null");
        this.chunkId = chunkId;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ChunkKey chunkKey = (ChunkKey) o;

        if (chunkId != chunkKey.chunkId) {
            return false;
        }
        return Objects.equals(uuid, chunkKey.uuid);
    }

    @Override
    public int hashCode() {
        int result = uuid != null ? uuid.hashCode() : 0;
        result = 31 * result + chunkId;
        return result;
    }
}
