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
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.puffin.BlobMetadata;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinReader;

public class SegmentManifestReader implements AutoCloseable {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final PuffinReader reader;

    public SegmentManifestReader(final InputFile inputFile) {
        Objects.requireNonNull(inputFile, "inputFile cannot be null");
        this.reader = Puffin.read(inputFile).build();
    }

    public ByteBuffer readOffsetIndex() throws IOException {
        return readBlob(BlobTypes.OFFSET_INDEX);
    }

    public ByteBuffer readTimestampIndex() throws IOException {
        return readBlob(BlobTypes.TIMESTAMP_INDEX);
    }

    public ByteBuffer readProducerSnapshotIndex() throws IOException {
        return readBlob(BlobTypes.PRODUCER_SNAPSHOT_INDEX);
    }

    public ByteBuffer readTransactionIndex() throws IOException {
        return readBlob(BlobTypes.TRANSACTION_INDEX);
    }

    public ByteBuffer readLeaderEpochIndex() throws IOException {
        return readBlob(BlobTypes.LEADER_EPOCH_INDEX);
    }

    public List<DataFileMetadata> readFileList() throws IOException {
        final JavaType collectionType = MAPPER.getTypeFactory()
            .constructCollectionType(List.class, DataFileMetadata.class);
        final ByteBuffer byteBuffer = readBlob(BlobTypes.FILE_LIST);
        return MAPPER.readValue(byteBuffer.compact().array(), collectionType);
    }

    private ByteBuffer readBlob(final String type) throws IOException {
        for (final BlobMetadata blob : reader.fileMetadata().blobs()) {
            if (blob.type().equals(type)) {
                final var data = reader.readAll(List.of(blob));
                final var iter = data.iterator();
                if (iter.hasNext()) {
                    return iter.next().second();
                } else {
                    return null;
                }
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
