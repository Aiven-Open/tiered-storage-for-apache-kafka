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

package io.aiven.kafka.tieredstorage.chunkmanager;

import java.nio.file.Path;
import java.util.Objects;

public class ChunkKey {
    public final String segmentFileName;
    public final int chunkId;

    public ChunkKey(final String objectKeyPath, final int chunkId) {
        Objects.requireNonNull(objectKeyPath, "objectKeyPath cannot be null");
        // get last part of segment path + chunk id, as it's used for creating file names
        this.segmentFileName = Path.of(objectKeyPath).getFileName().toString();
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
        return Objects.equals(segmentFileName, chunkKey.segmentFileName);
    }

    @Override
    public int hashCode() {
        int result = segmentFileName.hashCode();
        result = 31 * result + chunkId;
        return result;
    }

    @Override
    public String toString() {
        return "ChunkKey("
            + "segmentFileName=" + segmentFileName
            + ", chunkId=" + chunkId
            + ")";
    }

    public String path() {
        return segmentFileName + "-" + chunkId;
    }
}
