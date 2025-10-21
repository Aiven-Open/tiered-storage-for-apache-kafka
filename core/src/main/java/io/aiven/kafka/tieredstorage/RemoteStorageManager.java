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

package io.aiven.kafka.tieredstorage;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata.CustomMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;

import io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig;
import io.aiven.kafka.tieredstorage.manifest.SegmentFormat;
import io.aiven.kafka.tieredstorage.metrics.Metrics;
import io.aiven.kafka.tieredstorage.security.RsaEncryptionProvider;
import io.aiven.kafka.tieredstorage.storage.BytesRange;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig.METRICS_NUM_SAMPLES_CONFIG;
import static io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig.METRICS_RECORDING_LEVEL_CONFIG;
import static io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG;

public class RemoteStorageManager implements org.apache.kafka.server.log.remote.storage.RemoteStorageManager {
    private static final Logger log = LoggerFactory.getLogger(RemoteStorageManager.class);

    private KafkaRemoteStorageManager kafkaRsm;
    private IcebergRemoteStorageManager icebergRsm;
    private InternalRemoteStorageManagerSelector irsmSelector;

    private final Time time;
    private Metrics metrics;

    private SegmentFormat segmentFormat;
    private ObjectKeyFactory objectKeyFactory;

    public RemoteStorageManager() {
        this(Time.SYSTEM);
    }

    // for testing
    RemoteStorageManager(final Time time) {
        this.time = time;
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        Objects.requireNonNull(configs, "configs must not be null");
        final RemoteStorageManagerConfig config = new RemoteStorageManagerConfig(configs);
        final MetricConfig metricConfig = new MetricConfig()
            .samples(config.getInt(METRICS_NUM_SAMPLES_CONFIG))
            .timeWindow(config.getLong(METRICS_SAMPLE_WINDOW_MS_CONFIG), TimeUnit.MILLISECONDS)
            .recordLevel(Sensor.RecordingLevel.forName(config.getString(METRICS_RECORDING_LEVEL_CONFIG)));
        metrics = new Metrics(time, metricConfig);

        segmentFormat = config.segmentFormat();

        objectKeyFactory = new ObjectKeyFactory(config.keyPrefix(), config.keyPrefixMask());
        final RsaEncryptionProvider rsaEncryptionProvider = RemoteStorageManagerUtils.getRsaEncryptionProvider(config);

        final ObjectMapper mapper = RemoteStorageManagerUtils.getObjectMapper(config, rsaEncryptionProvider);

        this.kafkaRsm = new KafkaRemoteStorageManager(log, time, config);
        this.icebergRsm = segmentFormat == SegmentFormat.ICEBERG
            ? new IcebergRemoteStorageManager(log, time, config)
            : null;
        this.irsmSelector = new InternalRemoteStorageManagerSelector(segmentFormat, kafkaRsm, icebergRsm);
    }

    @Override
    public Optional<CustomMetadata> copyLogSegmentData(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                                       final LogSegmentData logSegmentData)
        throws RemoteStorageException {
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentId must not be null");
        Objects.requireNonNull(logSegmentData, "logSegmentData must not be null");

        log.info("Copying log segment data, metadata: {}", remoteLogSegmentMetadata);

        final long startedMs = time.milliseconds();

        final UploadMetricReporter uploadMetricReporter = (suffix, bytes) -> {
            metrics.recordObjectUpload(
                remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition(),
                suffix, bytes
            );
        };
        final var customMetadata = switch (segmentFormat) {
            case KAFKA ->
                kafkaRsm.copyLogSegmentData(remoteLogSegmentMetadata, logSegmentData, uploadMetricReporter);
            case ICEBERG ->
                icebergRsm.copyLogSegmentData(remoteLogSegmentMetadata, logSegmentData, uploadMetricReporter);
        };

        metrics.recordSegmentCopyTime(
            remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition(),
            startedMs, time.milliseconds());

        log.info("Copying log segment data completed successfully, metadata: {}", remoteLogSegmentMetadata);

        return customMetadata;
    }

    @Override
    public InputStream fetchLogSegment(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                       final int startPosition) throws RemoteStorageException {
        return this.fetchLogSegment(
            remoteLogSegmentMetadata,
            startPosition,
            remoteLogSegmentMetadata.segmentSizeInBytes() - 1
        );
    }

    @Override
    public InputStream fetchLogSegment(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                       final int startPosition,
                                       final int endPosition) throws RemoteStorageException {
        final BytesRange range = BytesRange.of(
            startPosition,
            Math.min(endPosition, remoteLogSegmentMetadata.segmentSizeInBytes() - 1)
        );
        metrics.recordSegmentFetch(
            remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition(),
            range.size());
        log.trace("Fetching log segment {} with range: {}", remoteLogSegmentMetadata, range);

        return this.irsmSelector
            .call(irsm -> irsm.fetchLogSegment(remoteLogSegmentMetadata, range));
    }

    @Override
    public InputStream fetchIndex(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                  final IndexType indexType) throws RemoteStorageException {
        log.trace("Fetching index {} for {}", indexType, remoteLogSegmentMetadata);
        return this.irsmSelector
            .call(irsm -> irsm.fetchIndex(remoteLogSegmentMetadata, indexType));
    }

    @Override
    public void deleteLogSegmentData(final RemoteLogSegmentMetadata remoteLogSegmentMetadata)
        throws RemoteStorageException {

        log.info("Deleting log segment data for {}", remoteLogSegmentMetadata);
        metrics.recordSegmentDelete(remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition(),
            remoteLogSegmentMetadata.segmentSizeInBytes());

        try {
            final long startedMs = time.milliseconds();

            kafkaRsm.deleteLogSegmentData(remoteLogSegmentMetadata);
            // TODO expose segment format in custom metadata to avoid double deletion
            // TODO call when implemented
            //icebergRsm.deleteLogSegmentData(remoteLogSegmentMetadata);

            metrics.recordSegmentDeleteTime(
                remoteLogSegmentMetadata.remoteLogSegmentId().topicIdPartition().topicPartition(),
                startedMs, time.milliseconds());
        } catch (final RemoteStorageException e) {
            metrics.recordSegmentDeleteError(remoteLogSegmentMetadata.remoteLogSegmentId()
                .topicIdPartition().topicPartition());
            throw e;
        }

        log.info("Deleting log segment data for completed successfully {}", remoteLogSegmentMetadata);
    }

    @Override
    public void close() throws IOException {
        if (metrics != null) {
            metrics.close();
        }
        if (kafkaRsm != null) {
            kafkaRsm.close();
        }
        if (icebergRsm != null) {
            icebergRsm.close();
        }
    }
}
