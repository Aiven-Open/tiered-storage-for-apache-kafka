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

package io.aiven.kafka.tieredstorage.fetch;

import java.nio.file.Path;
import java.util.Objects;

import io.aiven.kafka.tieredstorage.storage.BytesRange;

public class FetchPartKey {
    public final String segmentFileName;
    public final BytesRange range;

    public FetchPartKey(final String objectKeyPath, final BytesRange range) {
        Objects.requireNonNull(objectKeyPath, "objectKeyPath cannot be null");
        // get last part of segment path + chunk id, as it's used for creating file names
        this.segmentFileName = Path.of(objectKeyPath).getFileName().toString();
        this.range = Objects.requireNonNull(range, "range cannot be null");
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final FetchPartKey that = (FetchPartKey) o;
        return Objects.equals(segmentFileName, that.segmentFileName) && Objects.equals(range, that.range);
    }

    @Override
    public int hashCode() {
        return Objects.hash(segmentFileName, range);
    }

    @Override
    public String toString() {
        return "FetchPartKey("
            + "segmentFileName=" + segmentFileName
            + ", range=" + range
            + ")";
    }

    public String path() {
        return segmentFileName + "-" + range.from + "-" + range.to;
    }
}
