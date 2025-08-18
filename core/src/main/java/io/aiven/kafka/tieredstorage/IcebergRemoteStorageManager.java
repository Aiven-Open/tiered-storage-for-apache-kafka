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

package io.aiven.kafka.tieredstorage;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.utils.ByteBufferInputStream;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.internals.log.LogSegment;
import org.apache.kafka.storage.internals.log.OffsetIndex;

import io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig;
import io.aiven.kafka.tieredstorage.iceberg.BatchEnumeration;
import io.aiven.kafka.tieredstorage.iceberg.IcebergTableManager;
import io.aiven.kafka.tieredstorage.iceberg.MultiFileReader;
import io.aiven.kafka.tieredstorage.iceberg.RecordBatchGrouper;
import io.aiven.kafka.tieredstorage.iceberg.StructureProvider;
import io.aiven.kafka.tieredstorage.iceberg.data.IcebergWriter;
import io.aiven.kafka.tieredstorage.iceberg.data.ParquetAvroValueReaders;
import io.aiven.kafka.tieredstorage.iceberg.data.RowSchema;
import io.aiven.kafka.tieredstorage.iceberg.manifest.DataFileMetadata;
import io.aiven.kafka.tieredstorage.iceberg.manifest.SegmentManifestReader;
import io.aiven.kafka.tieredstorage.iceberg.manifest.SegmentManifestWriter;
import io.aiven.kafka.tieredstorage.metadata.SegmentCustomMetadataBuilder;
import io.aiven.kafka.tieredstorage.metadata.SegmentCustomMetadataField;
import io.aiven.kafka.tieredstorage.metadata.SegmentCustomMetadataSerde;
import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.aiven.kafka.tieredstorage.iceberg.StructureProvider.SchemaAndId;

public class IcebergRemoteStorageManager extends InternalRemoteStorageManager {
    private static final Logger LOG = LoggerFactory.getLogger(IcebergRemoteStorageManager.class);

    private final Catalog catalog;
    private final IcebergTableManager icebergTableManager;
    private final ObjectKeyFactory objectKeyFactory;
    private final Set<SegmentCustomMetadataField> customMetadataFields;
    private final SegmentCustomMetadataSerde customMetadataSerde;
    private final Namespace icebergNamespace;
    private final StructureProvider structureProvider;

    IcebergRemoteStorageManager(final Logger log, final Time time,
                                final RemoteStorageManagerConfig config) {
        super(log, time, config);
        this.customMetadataFields = config.customMetadataKeysIncluded();
        this.customMetadataSerde = new SegmentCustomMetadataSerde();

        this.objectKeyFactory = new ObjectKeyFactory(config.keyPrefix(), config.keyPrefixMask());
        this.catalog = config.icebergCatalog();
        this.structureProvider = config.structureProvider();
        this.icebergTableManager = new IcebergTableManager(catalog);
        this.icebergNamespace = config.icebergNamespace();
        LOG.info("IcebergRemoteStorageManager initialized successfully");
    }

    @Override
    public Optional<RemoteLogSegmentMetadata.CustomMetadata> copyLogSegmentData(
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
        final LogSegmentData logSegmentData,
        final UploadMetricReporter uploadMetricReporter) throws RemoteStorageException {
        try {
            Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentId must not be null");
            Objects.requireNonNull(logSegmentData, "logSegmentData must not be null");
            final String topicName =
                remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition().topic();

            LOG.info("Copying log segment data, metadata: {}", remoteLogSegmentMetadata);

            final LogSegmentProcessingResult processingResult = processLogSegment(
                remoteLogSegmentMetadata, logSegmentData, topicName);

            final Transaction transaction = processingResult.table().newTransaction();
            final AppendFiles appendFiles = transaction.newAppend();
            processingResult.dataFiles().forEach(appendFiles::appendFile);
            appendFiles.commit();

            final Table table = processingResult.table();
            final Snapshot snapshot = table.currentSnapshot();
            final OutputFile manifestOutputFile =
                table.io().newOutputFile(
                    table.location() + "/metadata/" + remoteLogSegmentMetadata.remoteLogSegmentId().id() + ".puffin");

            final List<DataFileMetadata> list =
                processingResult.dataFilePaths().stream()
                    .map(location -> new DataFileMetadata(location,
                        processingResult.keySchemaId(),
                        processingResult.valueSchemaId(),
                        remoteLogSegmentMetadata.startOffset(), 0L))
                    .toList();
            final SegmentManifestWriter manifestWriter;
            try (final SegmentManifestWriter writer = new SegmentManifestWriter(manifestOutputFile,
                snapshot.snapshotId(),
                snapshot.sequenceNumber())) {
                writer.writeOffsetIndex(Files.readAllBytes(logSegmentData.offsetIndex()));
                writer.writeTimestampIndex(Files.readAllBytes(logSegmentData.timeIndex()));
                writer.writeProducerSnapshotIndex(Files.readAllBytes(logSegmentData.producerSnapshotIndex()));
                if (logSegmentData.transactionIndex().isPresent()) {
                    writer.writeTransactionIndex(Files.readAllBytes(logSegmentData.transactionIndex().get()));
                }
                writer.writeLeaderEpochIndex(logSegmentData.leaderEpochIndex().array());
                writer.writeFileList(list);
                manifestWriter = writer;
            }
            final GenericStatisticsFile statisticsFile = manifestWriter.toStatisticsFile();
            transaction.updateStatistics().setStatistics(statisticsFile).commit();
            transaction.commitTransaction();
            final var customMetadataBuilder =
                new SegmentCustomMetadataBuilder(customMetadataFields, objectKeyFactory,
                    remoteLogSegmentMetadata);
            customMetadataBuilder.addUploadResult(ObjectKeyFactory.Suffix.MANIFEST,
                statisticsFile.fileSizeInBytes());
            uploadMetricReporter.report(ObjectKeyFactory.Suffix.MANIFEST, statisticsFile.fileSizeInBytes());
            LOG.info("Segment file data successfully appended to Iceberg table");
            return buildCustomMetadata(customMetadataBuilder);

        } catch (final Throwable e) {
            LOG.error("Error copying log segment data", e);
            throw new RemoteStorageException(e);
        }
    }

    private LogSegmentProcessingResult processLogSegment(
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
        final LogSegmentData logSegmentData,
        final String topicName) throws Exception {

        final File inputFile = logSegmentData.logSegment().toFile().getParentFile();
        LOG.debug("Converting segment file into Iceberg, path: {}", inputFile.getAbsolutePath());

        final OffsetIndex offsetIndex = new OffsetIndex(logSegmentData.offsetIndex().toFile(),
            remoteLogSegmentMetadata.startOffset());

        IcebergWriter writer = null;
        Integer keySchemaId = null;
        Integer valueSchemaId = null;
        Table table = null;

        if (catalog.tableExists(TableIdentifier.of(icebergNamespace, topicName))) {
            table = catalog.loadTable(TableIdentifier.of(icebergNamespace, topicName));
        }

        try (final LogSegment segment = LogSegment.open(inputFile, remoteLogSegmentMetadata.startOffset(),
            new LogConfig(Map.of()), Time.SYSTEM, 0, false)) {

            for (final var batch : segment.log().batches()) {
                for (final Record record : batch) {
                    final ParsedRecord parsedRecord = extractRecordData(record, topicName);

                    keySchemaId = parsedRecord.key().schemaId();
                    valueSchemaId = parsedRecord.value().schemaId();

                    if (writer == null) {
                        final TableIdentifier tableIdentifier = TableIdentifier.of(icebergNamespace, topicName);
                        table = icebergTableManager.getOrCreateTable(tableIdentifier, parsedRecord.recordSchema());
                        writer = new IcebergWriter(table);
                    }

                    writeRecordToIceberg(writer, remoteLogSegmentMetadata, batch, parsedRecord, offsetIndex);
                }
            }
        } catch (final IOException e) {
            LOG.error("Error processing log segment", e);
            throw new RemoteStorageException(e);
        }

        final List<DataFile> dataFiles = writer.complete();
        final List<String> dataFilePaths = dataFiles.stream()
            .map(ContentFile::location)
            .toList();

        return new LogSegmentProcessingResult(table, dataFiles, dataFilePaths, keySchemaId, valueSchemaId);
    }

    private ParsedRecord extractRecordData(
        final Record record,
        final String topicName) throws Exception {

        final Deserialized deserializedKey = getDeserializedKey(record, topicName);
        final Deserialized deserializedValue = getDeserializedValue(record, topicName);

        final Schema recordSchema = RowSchema.createRowSchema(
                deserializedKey.schema,
                deserializedValue.schema);

        return new ParsedRecord(
            record.offset(),
            record.timestamp(),
            deserializedKey,
            deserializedValue,
            recordSchema,
            record.headers()
        );
    }

    private Deserialized getDeserializedKey(final Record record, final String topicName) throws IOException {
        if (record.hasKey()) {
            final byte[] rawKey = new byte[record.keySize()];
            record.key().get(rawKey);
            final Integer schemaId = getSchemaId(rawKey);
            final SchemaAndId<Schema> schema = structureProvider.getSchemaById(schemaId);
            final Object keyRecord = structureProvider.deserializeKey(topicName, null, rawKey);
            return new Deserialized(rawKey, keyRecord, schema.schemaId(), schema.schema());
        } else {
            final SchemaAndId<Schema> schema = structureProvider.getSchemaById(null);
            return new Deserialized(null, null, null, schema.schema());
        }
    }

    private Deserialized getDeserializedValue(final Record record, final String topicName) throws IOException {
        if (record.hasValue()) {
            final byte[] rawValue = new byte[record.valueSize()];
            record.value().get(rawValue);
            final Integer schemaId = getSchemaId(rawValue);
            final SchemaAndId<Schema> schema = structureProvider.getSchemaById(schemaId);
            final Object valueRecord = structureProvider.deserializeValue(topicName, null, rawValue);
            return new Deserialized(rawValue, valueRecord, schema.schemaId(), schema.schema());
        } else {
            final SchemaAndId<Schema> schema = structureProvider.getSchemaById(null);
            return new Deserialized(null, null, null, schema.schema());
        }
    }

    private record Deserialized(byte[] raw, Object record, Integer schemaId, Schema schema) {
    }

    private record ParsedRecord(
        long recordOffset,
        long recordTimestamp,
        Deserialized key,
        Deserialized value,
        Schema recordSchema,
        Header[] headers
    ) {
    }

    private void writeRecordToIceberg(
        final IcebergWriter writer,
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
        final RecordBatch batch,
        final ParsedRecord parsedRecord,
        final OffsetIndex offsetIndex
    ) throws Exception {
        final GenericData.Record kafkaPart = new GenericData.Record(RowSchema.KAFKA);
        kafkaPart.put(RowSchema.Fields.PARTITION,
            remoteLogSegmentMetadata.topicIdPartition().topicPartition().partition());
        kafkaPart.put(RowSchema.Fields.OFFSET, parsedRecord.recordOffset());
        kafkaPart.put(RowSchema.Fields.TIMESTAMP, parsedRecord.recordTimestamp());
        kafkaPart.put(RowSchema.Fields.BATCH_BYTE_OFFSET, offsetIndex.lookup(parsedRecord.recordOffset()).position);
        kafkaPart.put(RowSchema.Fields.BATCH_BASE_OFFSET, batch.baseOffset());
        kafkaPart.put(RowSchema.Fields.BATCH_PARTITION_LEADER_EPOCH, batch.partitionLeaderEpoch());
        kafkaPart.put(RowSchema.Fields.BATCH_MAGIC, batch.magic());
        kafkaPart.put(RowSchema.Fields.BATCH_TIMESTAMP_TYPE, batch.timestampType().id);
        kafkaPart.put(RowSchema.Fields.BATCH_COMPRESSION_TYPE, batch.compressionType().id);
        kafkaPart.put(RowSchema.Fields.BATCH_MAX_TIMESTAMP, batch.maxTimestamp());
        kafkaPart.put(RowSchema.Fields.BATCH_PRODUCER_ID, batch.producerId());
        kafkaPart.put(RowSchema.Fields.BATCH_PRODUCER_EPOCH, batch.producerEpoch());
        kafkaPart.put(RowSchema.Fields.BATCH_BASE_SEQUENCE, batch.baseSequence());

        final GenericData.Record finalRecord = new GenericData.Record(parsedRecord.recordSchema());
        finalRecord.put("kafka", kafkaPart);
        if (parsedRecord.key().record() != null) {
            finalRecord.put("key", parsedRecord.key().record());
        } else {
            finalRecord.put("key_raw", parsedRecord.key().raw());
        }
        if (parsedRecord.value().record() != null) {
            finalRecord.put("value", parsedRecord.value().record());
        } else {
            finalRecord.put("value_raw", parsedRecord.value().raw());
        }
        finalRecord.put("headers", Arrays.asList(parsedRecord.headers()));

        writer.write(finalRecord);
    }

    private record LogSegmentProcessingResult(
        Table table,
        List<DataFile> dataFiles,
        List<String> dataFilePaths,
        Integer keySchemaId,
        Integer valueSchemaId) {}

    private static int getSchemaId(final byte[] value) {
        final ByteBuffer buffer = ByteBuffer.wrap(value);
        buffer.get();
        return buffer.getInt();
    }

    private Optional<RemoteLogSegmentMetadata.CustomMetadata> buildCustomMetadata(
        final SegmentCustomMetadataBuilder customMetadataBuilder
    ) {
        final var customFields = customMetadataBuilder.build();
        if (!customFields.isEmpty()) {
            final var customMetadataBytes = customMetadataSerde.serialize(customFields);
            return Optional.of(new RemoteLogSegmentMetadata.CustomMetadata(customMetadataBytes));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public InputStream fetchLogSegment(final RemoteLogSegmentMetadata remoteLogSegmentMetadata, final BytesRange range)
        throws RemoteStorageException {
        final int startPosition = range.firstPosition();
        LOG.info("fetchLogSegment from position {}", startPosition);
        final String topic = remoteLogSegmentMetadata.topicIdPartition().topic();
        final Table table = catalog.loadTable(TableIdentifier.of(icebergNamespace, topic));

        try {
            final ObjectKey key =
                () -> table.location() + "/metadata/" + remoteLogSegmentMetadata.remoteLogSegmentId().id() + ".puffin";

            // TODO read only files that contain startPosition
            final List<DataFileMetadata> remoteFilePaths = getRemoteFilePaths(remoteLogSegmentMetadata, key);
            final RecordBatchGrouper recordBatchGrouper =
                new RecordBatchGrouper(new MultiFileReader(remoteFilePaths, dataFileMetadata -> {
                    final FileIO io = table.io();

                    final SchemaAndId<Schema> keySchema =
                            structureProvider.getSchemaById(dataFileMetadata.keySchemaId());
                    final SchemaAndId<Schema> valueSchema =
                        structureProvider.getSchemaById(dataFileMetadata.valueSchemaId());

                    final Schema recordSchema = RowSchema.createRowSchema(keySchema.schema(), valueSchema.schema());

                    return Parquet.read(io.newInputFile(dataFileMetadata.location()))
                        .project(table.schema())
                        .createReaderFunc((s, mt) -> ParquetAvroValueReaders.buildReader(s, mt, recordSchema))
                        .filter(Expressions.greaterThanOrEqual("kafka.batch_byte_offset",
                            remoteLogSegmentMetadata.startOffset()))
                        .build();
                }));
            return new LazySequenceInputStream(new BatchEnumeration(recordBatchGrouper, this.structureProvider, topic));
        } catch (final IOException e) {
            LOG.error("Failed to read remote data", e);
            throw new RemoteStorageException(e);
        } catch (final Exception e) {
            throw new RemoteStorageException(e);
        }
    }

    private List<DataFileMetadata> getRemoteFilePaths(
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
        final ObjectKey key
    ) throws IOException {
        final String topicName = remoteLogSegmentMetadata.topicIdPartition().topic();
        final Table table = catalog.loadTable(TableIdentifier.of(icebergNamespace, topicName));
        final FileIO io = table.io();
        try (final SegmentManifestReader manifestReader = new SegmentManifestReader(io.newInputFile(key.value()))) {
            return manifestReader.readFileList();
        }
    }

    private static class LazySequenceInputStream extends SequenceInputStream {
        private final BatchEnumeration closeableEnumeration;

        LazySequenceInputStream(final BatchEnumeration e) {
            super(e);
            this.closeableEnumeration = e;
        }

        public void close() throws IOException {
            closeableEnumeration.close();
            super.close();
        }
    }

    @Override
    InputStream fetchIndex(
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
        final RemoteStorageManager.IndexType indexType
    ) throws RemoteStorageException {
        final String topicName = remoteLogSegmentMetadata.topicIdPartition().topic();
        final Table table = catalog.loadTable(TableIdentifier.of(icebergNamespace, topicName));
        final ObjectKey manifestKey =
            () -> table.location() + "/metadata/" + remoteLogSegmentMetadata.remoteLogSegmentId().id() + ".puffin";
        final FileIO io = table.io();
        try (
            final SegmentManifestReader manifestReader = new SegmentManifestReader(
                io.newInputFile(manifestKey.value()))) {

            return switch (indexType) {
                case OFFSET -> toInputStream(manifestReader.readOffsetIndex());
                case TIMESTAMP -> toInputStream(manifestReader.readTimestampIndex());
                case PRODUCER_SNAPSHOT -> toInputStream(manifestReader.readProducerSnapshotIndex());
                case TRANSACTION -> toInputStream(manifestReader.readTransactionIndex());
                case LEADER_EPOCH -> toInputStream(manifestReader.readLeaderEpochIndex());
            };
        } catch (final Exception e) {
            throw new RemoteStorageException(e);
        }
    }

    private static InputStream toInputStream(final ByteBuffer buffer) {
        if (buffer == null) {
            return InputStream.nullInputStream();
        } else {
            return new ByteBufferInputStream(buffer);
        }
    }

    @Override
    public void deleteLogSegmentData(final RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        //No-op for now, considering that Iceberg further manages the data.
    }
}
