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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;

public class SegmentManifestWriter implements AutoCloseable {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final PuffinWriter writer;
    private final long snapshotId;
    private final long snapshotSequenceNumber;
    private final Set<String> writtenBlobs = new HashSet<>();
    private final OutputFile outputFile;

    public SegmentManifestWriter(final OutputFile outputFile,
                                 final long snapshotId,
                                 final long snapshotSequenceNumber) {
        Objects.requireNonNull(outputFile, "outputFile cannot be null");
        this.outputFile = outputFile;
        this.writer = Puffin.write(this.outputFile)
            .createdBy("Aiven Kafka tiered storage plugin")
            .build();
        this.snapshotId = snapshotId;
        this.snapshotSequenceNumber = snapshotSequenceNumber;
    }

    public void writeOffsetIndex(final byte[] data) {
        writeBlob(BlobTypes.OFFSET_INDEX, data);
    }

    public void writeTimestampIndex(final byte[] data) {
        writeBlob(BlobTypes.TIMESTAMP_INDEX, data);
    }

    public void writeProducerSnapshotIndex(final byte[] data) {
        writeBlob(BlobTypes.PRODUCER_SNAPSHOT_INDEX, data);
    }

    public void writeTransactionIndex(final byte[] data) {
        writeBlob(BlobTypes.TRANSACTION_INDEX, data);
    }

    public void writeLeaderEpochIndex(final byte[] data) {
        writeBlob(BlobTypes.LEADER_EPOCH_INDEX, data);
    }

    public void writeFileList(final List<DataFileMetadata> fileList) throws JsonProcessingException {
        writeBlob(BlobTypes.FILE_LIST, MAPPER.writeValueAsBytes(fileList));
    }

    private void writeBlob(final String type, final byte[] data) {
        Objects.requireNonNull(data, "data cannot be null");
        if (writtenBlobs.contains(type)) {
            throw new IllegalStateException(type + " blob already written");
        }

        final Blob blob = new Blob(
            type,
            List.of(),
            snapshotId,
            snapshotSequenceNumber,
            ByteBuffer.wrap(data));
        writer.write(blob);
        writtenBlobs.add(type);
    }

    public GenericStatisticsFile toStatisticsFile() {
        return new GenericStatisticsFile(
            snapshotId,
            outputFile.location(),
            writer.fileSize(),
            writer.footerSize(),
            writer.writtenBlobsMetadata().stream()
                .map(GenericBlobMetadata::from)
                .toList());
    }

    @Override
    public void close() throws IOException {
        writer.finish();
        writer.close();
    }
}
