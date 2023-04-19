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

package io.aiven.kafka.tieredstorage.commons;

import io.aiven.kafka.tieredstorage.commons.storage.ObjectStorageFactory;
import io.aiven.kafka.tieredstorage.commons.transform.TransformFinisher;
import io.aiven.kafka.tieredstorage.commons.transform.TransformPipeline;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.file.Files;
import java.util.Map;

import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

public class UniversalRemoteStorageManager implements RemoteStorageManager {
    ObjectStorageFactory storage;
    TransformPipeline pipeline;
    UniversalRemoteStorageManagerConfig config;

    @Override
    public void configure(final Map<String, ?> configs) {
        config = new UniversalRemoteStorageManagerConfig(configs);
        try {
            pipeline = TransformPipeline.newBuilder().fromConfig(config).build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        storage = config.objectStorageFactory();
        throw new UnsupportedOperationException();
    }

    @Override
    public void copyLogSegmentData(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                   final LogSegmentData logSegmentData) throws RemoteStorageException {
        try {
            final byte[] original = Files.readAllBytes(logSegmentData.logSegment());
            final TransformFinisher result = pipeline.inboundChain(original).complete();
            final String key = ObjectKey.key(config.keyPrefix(), remoteLogSegmentMetadata, ObjectKey.Suffix.LOG);
            storage.fileUploader().upload(new SequenceInputStream(result), key);
            //...
        } catch (IOException e) {
            throw new RemoteStorageException(e);
        }
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream fetchLogSegment(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                       final int startPosition) throws RemoteStorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream fetchLogSegment(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                       final int startPosition,
                                       final int endPosition) throws RemoteStorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream fetchIndex(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                  final IndexType indexType) throws RemoteStorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void deleteLogSegmentData(final RemoteLogSegmentMetadata remoteLogSegmentMetadata)
        throws RemoteStorageException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException();
    }
}
