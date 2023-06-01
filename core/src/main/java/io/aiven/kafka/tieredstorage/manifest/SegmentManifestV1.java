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

import java.util.Objects;
import java.util.Optional;

import io.aiven.kafka.tieredstorage.manifest.index.ChunkIndex;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SegmentManifestV1 implements SegmentManifest {
    private final ChunkIndex chunkIndex;
    private final boolean compression;
    private final SegmentEncryptionMetadataV1 encryption;

    @JsonCreator
    public SegmentManifestV1(@JsonProperty(value = "chunkIndex", required = true)
                             final ChunkIndex chunkIndex,
                             @JsonProperty(value = "compression", required = true)
                             final boolean compression,
                             @JsonProperty("encryption")
                             final SegmentEncryptionMetadataV1 encryption) {
        this.chunkIndex = Objects.requireNonNull(chunkIndex, "chunkIndex cannot be null");
        this.compression = compression;

        this.encryption = encryption;
    }

    @Override
    @JsonProperty("chunkIndex")
    public ChunkIndex chunkIndex() {
        return chunkIndex;
    }

    @Override
    @JsonProperty("compression")
    public boolean compression() {
        return compression;
    }

    @Override
    @JsonProperty("encryption")
    @JsonInclude(JsonInclude.Include.NON_ABSENT)
    public Optional<SegmentEncryptionMetadata> encryption() {
        return Optional.ofNullable(encryption);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final SegmentManifestV1 that = (SegmentManifestV1) o;

        if (compression != that.compression) {
            return false;
        }
        if (!chunkIndex.equals(that.chunkIndex)) {
            return false;
        }
        return Objects.equals(encryption, that.encryption);
    }

    @Override
    public int hashCode() {
        int result = chunkIndex.hashCode();
        result = 31 * result + (compression ? 1 : 0);
        result = 31 * result + (encryption != null ? encryption.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "SegmentManifestV1("
            + "chunkIndex=" + chunkIndex
            + ", compression=" + compression
            + ", encryption=" + encryption
            + ")";
    }
}
