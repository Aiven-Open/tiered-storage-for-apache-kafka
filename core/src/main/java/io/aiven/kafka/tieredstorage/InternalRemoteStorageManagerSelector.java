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

import java.io.InputStream;
import java.util.Objects;

import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import io.aiven.kafka.tieredstorage.manifest.SegmentFormat;
import io.aiven.kafka.tieredstorage.storage.BytesRange;

/**
 * Selects an {@link InternalRemoteStorageManager} based on the segment format.
 */
public class InternalRemoteStorageManagerSelector {
    private final SegmentFormat segmentFormat;
    private final InternalRemoteStorageManager kafkaRsm;
    private final InternalRemoteStorageManager icebergRsm;

    public InternalRemoteStorageManagerSelector(final SegmentFormat segmentFormat,
                                                final InternalRemoteStorageManager kafkaRsm,
                                                final InternalRemoteStorageManager icebergRsm) {
        this.segmentFormat = Objects.requireNonNull(segmentFormat, "segmentFormat cannot be null");

        if (segmentFormat == SegmentFormat.KAFKA
            && kafkaRsm == null) {
            throw new IllegalArgumentException("kafkaRsm cannot be null when format is KAFKA");
        }
        this.kafkaRsm = kafkaRsm;

        if (segmentFormat == SegmentFormat.ICEBERG
            && icebergRsm == null) {
            throw new IllegalArgumentException("icebergRsm cannot be null when format is ICEBERG");
        }
        this.icebergRsm = icebergRsm;
    }

    /**
     * Call the selected {@link InternalRemoteStorageManager}.
     *
     * <p>If both are present, they are called in the order where {@code segmentFormat} defines the precedence.
     * @param call function to call (normally
     *     {@link InternalRemoteStorageManager#fetchLogSegment(RemoteLogSegmentMetadata, BytesRange)} or
     *     {@link InternalRemoteStorageManager#fetchIndex(RemoteLogSegmentMetadata, RemoteStorageManager.IndexType)}).
     * @return result {@link InputStream}.
     * @throws RemoteResourceNotFoundException when no manifest is found.
     * @throws RemoteStorageException generic error.
     */
    public InputStream call(final Call call) throws RemoteResourceNotFoundException, RemoteStorageException {
        // TODO expose segment format in custom metadata to avoid double looking
        final InternalRemoteStorageManager firstRsm;
        final InternalRemoteStorageManager secondRsm;
        switch (segmentFormat) {
            case KAFKA -> {
                firstRsm = kafkaRsm;
                secondRsm = icebergRsm;
            }

            case ICEBERG -> {
                firstRsm = icebergRsm;
                secondRsm = kafkaRsm;
            }

            default -> throw new RemoteStorageException("Unknown segment format: " + segmentFormat);
        }

        try {
            return call.call(firstRsm);
        } catch (final SegmentManifestNotFoundException firstException) {
            if (secondRsm != null) {
                try {
                    return call.call(secondRsm);
                } catch (final SegmentManifestNotFoundException e) {
                    throw new RemoteResourceNotFoundException(firstException);
                }
            } else {
                throw new RemoteResourceNotFoundException(firstException);
            }
        }

    }

    @FunctionalInterface
    interface Call {
        InputStream call(final InternalRemoteStorageManager irsm)
            throws RemoteStorageException, SegmentManifestNotFoundException;
    }
}
