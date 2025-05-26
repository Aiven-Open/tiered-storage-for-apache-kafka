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

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DataFileMetadata {
    private final String location;
    private final Integer keySchemaId;
    private final Integer valueSchemaId;
    private final long startOffset;
    private final long startByteOffset;

    @JsonCreator
    public DataFileMetadata(@JsonProperty(value = "location", required = true) final String location,
                            @JsonProperty(value = "keySchemaId") final Integer keySchemaId,
                            @JsonProperty(value = "valueSchemaId") final Integer valueSchemaId,
                            @JsonProperty(value = "startOffset", required = true) final long startOffset,
                            @JsonProperty(value = "startByteOffset", required = true) final long startByteOffset) {
        this.location = location;
        this.keySchemaId = keySchemaId;
        this.valueSchemaId = valueSchemaId;
        this.startOffset = startOffset;
        this.startByteOffset = startByteOffset;
    }

    @JsonProperty("location")
    public String location() {
        return this.location;
    }

    @JsonProperty("keySchemaId")
    public Integer keySchemaId() {
        return this.keySchemaId;
    }

    @JsonProperty("valueSchemaId")
    public Integer valueSchemaId() {
        return this.valueSchemaId;
    }

    @JsonProperty("startOffset")
    public long startOffset() {
        return this.startOffset;
    }

    @JsonProperty("startByteOffset")
    public long startByteOffset() {
        return this.startByteOffset;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final DataFileMetadata dataFileMetadata = (DataFileMetadata) o;
        return Objects.equals(location, dataFileMetadata.location)
            && Objects.equals(keySchemaId, dataFileMetadata.keySchemaId)
            && Objects.equals(valueSchemaId, dataFileMetadata.valueSchemaId)
            && startOffset == dataFileMetadata.startOffset
            && startByteOffset == dataFileMetadata.startByteOffset;
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(location);
        result = 31 * result + Objects.hashCode(keySchemaId);
        result = 31 * result + Objects.hashCode(valueSchemaId);
        result = 31 * result + Long.hashCode(startOffset);
        result = 31 * result + Long.hashCode(startByteOffset);
        return result;
    }

    @Override
    public String toString() {
        return "DataFile{"
            + "location=" + location
            + ", keySchemaId=" + keySchemaId
            + ", valueSchemaId=" + valueSchemaId
            + ", startOffset=" + startOffset
            + ", startByteOffset=" + startByteOffset
            + '}';
    }
}
