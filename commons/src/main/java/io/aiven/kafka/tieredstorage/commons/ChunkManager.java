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

import java.io.InputStream;
import java.util.concurrent.Future;

import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import io.aiven.kafka.tieredstorage.commons.manifest.SegmentManifest;

public interface ChunkManager {
    /**
     * Gets a chunk of a segment.
     * @return a future of {@link InputStream} of the chunk, plain text (i.e. decrypted and decompressed).
     */
    Future<InputStream> getChunk(RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                 SegmentManifest manifest,
                                 int chunkId);
}
