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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.Files;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SegmentManifestWriterAndReaderTest {
    @Test
    void writeAndRead(@TempDir final Path tmpFolder) throws IOException {
        final byte[] offsetIndexData = new byte[10];
        final byte[] timestampIndexData = new byte[11];
        final byte[] producerSnapshotIndexData = new byte[12];
        final byte[] transactionIndexData = new byte[13];
        final byte[] leaderEpochIndexData = new byte[14];
        final List<DataFileMetadata> dataFileMetadata = List.of(
            new DataFileMetadata("aaa", 0, 0, 0, 0),
            new DataFileMetadata("bbb", 0, 0, 10, 100),
            new DataFileMetadata("ccc", 0, 0, 20, 200)
        );

        final File file = tmpFolder.resolve("xxx").toFile();

        final OutputFile outputFile = Files.localOutput(file);
        try (final SegmentManifestWriter writer = new SegmentManifestWriter(outputFile, 10, 20)) {
            writer.writeOffsetIndex(offsetIndexData);
            writer.writeTimestampIndex(timestampIndexData);
            writer.writeProducerSnapshotIndex(producerSnapshotIndexData);
            writer.writeTransactionIndex(transactionIndexData);
            writer.writeLeaderEpochIndex(leaderEpochIndexData);
            writer.writeFileList(dataFileMetadata);
        }

        final InputFile inputFile = Files.localInput(file);
        try (final SegmentManifestReader segmentManifestReader = new SegmentManifestReader(inputFile)) {
            assertThat(segmentManifestReader.readOffsetIndex().compact().array()).isEqualTo(offsetIndexData);
            assertThat(segmentManifestReader.readTimestampIndex().compact().array()).isEqualTo(timestampIndexData);
            assertThat(segmentManifestReader.readProducerSnapshotIndex().compact().array())
                .isEqualTo(producerSnapshotIndexData);
            assertThat(segmentManifestReader.readTransactionIndex().compact().array()).isEqualTo(transactionIndexData);
            assertThat(segmentManifestReader.readLeaderEpochIndex().compact().array()).isEqualTo(leaderEpochIndexData);
            assertThat(segmentManifestReader.readFileList()).containsExactlyElementsOf(dataFileMetadata);
        }
    }

    @Test
    void writeAndReadWithStatisticsFile(@TempDir final Path tmpFolder) throws IOException {
        final byte[] offsetIndexData = new byte[10];
        final byte[] timestampIndexData = new byte[11];
        final byte[] producerSnapshotIndexData = new byte[12];
        final byte[] transactionIndexData = new byte[13];
        final byte[] leaderEpochIndexData = new byte[14];
        final List<DataFileMetadata> dataFiles = List.of(
            new DataFileMetadata("aaa", 0, 0, 0, 0),
            new DataFileMetadata("bbb", 0, 0, 10, 100),
            new DataFileMetadata("ccc", 0, 0, 20, 200)
        );
        final File file = tmpFolder.resolve("xxx").toFile();
        final OutputFile outputFile = Files.localOutput(file);
        final SegmentManifestWriter writer = new SegmentManifestWriter(outputFile, 10, 20);
        try (writer) {
            writer.writeOffsetIndex(offsetIndexData);
            writer.writeTimestampIndex(timestampIndexData);
            writer.writeProducerSnapshotIndex(producerSnapshotIndexData);
            writer.writeTransactionIndex(transactionIndexData);
            writer.writeLeaderEpochIndex(leaderEpochIndexData);
            writer.writeFileList(dataFiles);
        }
        final GenericStatisticsFile statisticsFile = writer.toStatisticsFile();

        assertThat(statisticsFile.snapshotId()).isEqualTo(10);
        assertThat(statisticsFile.fileSizeInBytes()).isEqualTo(1165);
        assertThat(statisticsFile.fileFooterSizeInBytes()).isEqualTo(827);
        assertThat(statisticsFile.path()).isEqualTo(file.getAbsolutePath());

        assertThat(statisticsFile.blobMetadata())
            .extracting(BlobMetadata::type)
            .containsExactlyInAnyOrder(
                "aiven-tiered-storage-offset-index",
                "aiven-tiered-storage-timestamp-index",
                "aiven-tiered-storage-producer-snapshot-index",
                "aiven-tiered-storage-transaction-index",
                "aiven-tiered-storage-leader-epoch-index",
                "aiven-tiered-storage-file-list"
            );

        assertThat(statisticsFile.blobMetadata())
            .allSatisfy(metadata -> {
                assertThat(metadata.sourceSnapshotId()).isEqualTo(10);
                assertThat(metadata.sourceSnapshotSequenceNumber()).isEqualTo(20);
            });
    }

    @Test
    void writeTwice(@TempDir final Path tmpFolder) throws IOException {
        final File file = tmpFolder.resolve("xxx").toFile();
        final OutputFile outputFile = Files.localOutput(file);
        try (final SegmentManifestWriter writer = new SegmentManifestWriter(outputFile, 10, 20)) {
            writer.writeOffsetIndex(new byte[1]);
            assertThatThrownBy(() -> writer.writeOffsetIndex(new byte[2]))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("aiven-tiered-storage-offset-index blob already written");

            writer.writeTimestampIndex(new byte[1]);
            assertThatThrownBy(() -> writer.writeTimestampIndex(new byte[2]))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("aiven-tiered-storage-timestamp-index blob already written");

            writer.writeProducerSnapshotIndex(new byte[1]);
            assertThatThrownBy(() -> writer.writeProducerSnapshotIndex(new byte[2]))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("aiven-tiered-storage-producer-snapshot-index blob already written");

            writer.writeTransactionIndex(new byte[1]);
            assertThatThrownBy(() -> writer.writeTransactionIndex(new byte[2]))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("aiven-tiered-storage-transaction-index blob already written");

            writer.writeLeaderEpochIndex(new byte[1]);
            assertThatThrownBy(() -> writer.writeLeaderEpochIndex(new byte[2]))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("aiven-tiered-storage-leader-epoch-index blob already written");
        }
    }

    @Test
    void readNonExistent(@TempDir final Path tmpFolder) throws IOException {
        final File file = tmpFolder.resolve("xxx").toFile();

        final OutputFile outputFile = Files.localOutput(file);
        try (final SegmentManifestWriter writer = new SegmentManifestWriter(outputFile, 10, 20)) {
            writer.writeOffsetIndex(new byte[10]);
        }

        final InputFile inputFile = Files.localInput(file);
        try (final SegmentManifestReader segmentManifestReader = new SegmentManifestReader(inputFile)) {
            assertThat(segmentManifestReader.readTimestampIndex()).isNull();
        }
    }
}
