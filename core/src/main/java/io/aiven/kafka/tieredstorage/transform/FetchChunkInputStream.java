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

package io.aiven.kafka.tieredstorage.transform;

import java.io.SequenceInputStream;

import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import io.aiven.kafka.tieredstorage.ChunkManager;
import io.aiven.kafka.tieredstorage.manifest.SegmentManifest;
import io.aiven.kafka.tieredstorage.storage.BytesRange;

public class FetchChunkInputStream extends SequenceInputStream {
    /**
     *
     * @param chunkManager provides chunk input to fetch from
     * @param remoteLogSegmentMetadata required by chunkManager
     * @param manifest provides to index to build response from
     * @param range original offset range start/end position
     */
    public FetchChunkInputStream(final ChunkManager chunkManager,
                                 final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                 final SegmentManifest manifest,
                                 final BytesRange range) {
        super(new FetchChunkEnumeration(chunkManager, remoteLogSegmentMetadata, manifest, range));
    }
}
