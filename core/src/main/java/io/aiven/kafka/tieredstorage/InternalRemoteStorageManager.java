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

import java.io.Closeable;
import java.io.InputStream;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig;
import io.aiven.kafka.tieredstorage.storage.BytesRange;

import org.slf4j.Logger;

abstract class InternalRemoteStorageManager implements Closeable {
    protected final Logger log;
    protected final Time time;

    InternalRemoteStorageManager(
        final Logger log, final Time time,
        final RemoteStorageManagerConfig config
    ) {
        this.log = Objects.requireNonNull(log);
        this.time = Objects.requireNonNull(time);
    }

    abstract Optional<RemoteLogSegmentMetadata.CustomMetadata> copyLogSegmentData(
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
        final LogSegmentData logSegmentData,
        final UploadMetricReporter uploadMetricReporter
    ) throws RemoteStorageException;

    abstract InputStream fetchLogSegment(
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
        final BytesRange range
    ) throws RemoteStorageException, SegmentManifestNotFoundException;

    abstract InputStream fetchIndex(
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
        final RemoteStorageManager.IndexType indexType
    ) throws RemoteStorageException, SegmentManifestNotFoundException;

    abstract void deleteLogSegmentData(
        final RemoteLogSegmentMetadata remoteLogSegmentMetadata
    ) throws RemoteStorageException;
}
