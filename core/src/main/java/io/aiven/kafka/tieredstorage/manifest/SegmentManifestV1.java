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

import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import io.aiven.kafka.tieredstorage.manifest.index.ChunkIndex;
import io.aiven.kafka.tieredstorage.security.DataKeyAndAAD;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SegmentManifestV1 implements SegmentManifest {
    private final ChunkIndex chunkIndex;
    private final SegmentIndexesV1 segmentIndexes;
    private final boolean compression;
    private final SegmentEncryptionMetadataV1 encryption;
    private final RemoteLogSegmentMetadata remoteLogSegmentMetadata;

    @JsonCreator
    public SegmentManifestV1(
        @JsonProperty(value = "chunkIndex", required = true) final ChunkIndex chunkIndex,
        @JsonProperty(value = "segmentIndexes", required = true) final SegmentIndexesV1 segmentIndexes,
        @JsonProperty(value = "compression", required = true) final boolean compression,
        @JsonProperty("encryption") final SegmentEncryptionMetadataV1 encryption
    ) {
        this(chunkIndex, segmentIndexes, compression, encryption, null);
    }

    private SegmentManifestV1(final ChunkIndex chunkIndex,
                             final SegmentIndexesV1 segmentIndexes,
                             final boolean compression,
                             final SegmentEncryptionMetadataV1 encryption,
                             final RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        this.chunkIndex = Objects.requireNonNull(chunkIndex, "chunkIndex cannot be null");
        this.segmentIndexes = Objects.requireNonNull(segmentIndexes, "segmentIndexes cannot be null");

        this.compression = compression;
        this.encryption = encryption;

        this.remoteLogSegmentMetadata = remoteLogSegmentMetadata;
    }

    public static Builder newBuilder(
        final ChunkIndex chunkIndex,
        final SegmentIndexesV1 segmentIndexes
    ) {
        return new Builder(chunkIndex, segmentIndexes);
    }

    @Override
    @JsonProperty("chunkIndex")
    public ChunkIndex chunkIndex() {
        return chunkIndex;
    }

    @Override
    @JsonProperty("segmentIndexes")
    public SegmentIndexes segmentIndexes() {
        return segmentIndexes;
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
    // We don't need to deserialize it
    @JsonProperty(value = "remoteLogSegmentMetadata", access = JsonProperty.Access.READ_ONLY)
    public RemoteLogSegmentMetadata remoteLogSegmentMetadata() {
        return remoteLogSegmentMetadata;
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
        // We don't want remoteLogSegmentMetadata to participate in hash code and equality checks.
        return Objects.equals(encryption, that.encryption);
    }

    @Override
    public int hashCode() {
        int result = chunkIndex.hashCode();
        result = 31 * result + (compression ? 1 : 0);
        result = 31 * result + (encryption != null ? encryption.hashCode() : 0);
        // We don't want remoteLogSegmentMetadata to participate in hash code and equality checks.
        return result;
    }

    @Override
    public String toString() {
        return "SegmentManifestV1("
            + "chunkIndex=" + chunkIndex
            + ", segmentIndexes=" + segmentIndexes
            + ", compression=" + compression
            + ", encryption=" + encryption
            + ")";
    }

    public static class Builder {
        final ChunkIndex chunkIndex;
        final SegmentIndexesV1 segmentIndexes;
        boolean compression = false;
        SegmentEncryptionMetadataV1 encryptionMetadata = null;
        RemoteLogSegmentMetadata rlsm = null;

        public Builder(
            final ChunkIndex chunkIndex,
            final SegmentIndexesV1 segmentIndexes
        ) {
            this.chunkIndex = chunkIndex;
            this.segmentIndexes = segmentIndexes;
        }

        public Builder withCompressionEnabled(final boolean requiresCompression) {
            this.compression = requiresCompression;
            return this;
        }

        public Builder withEncryptionMetadata(final SegmentEncryptionMetadataV1 encryptionMetadata) {
            this.encryptionMetadata = Objects.requireNonNull(encryptionMetadata, "encryptionMetadata cannot be null");
            return this;
        }

        public Builder withEncryptionKey(final DataKeyAndAAD dataKeyAndAAD) {
            this.encryptionMetadata = new SegmentEncryptionMetadataV1(
                Objects.requireNonNull(dataKeyAndAAD, "dataKeyAndAAD cannot be null")
            );
            return this;
        }

        public Builder withRlsm(final RemoteLogSegmentMetadata rlsm) {
            this.rlsm = Objects.requireNonNull(rlsm, "remoteLogSegmentMetadata cannot be null");
            return this;
        }

        public SegmentManifestV1 build() {
            return new SegmentManifestV1(chunkIndex, segmentIndexes, compression, encryptionMetadata, rlsm);
        }
    }
}
