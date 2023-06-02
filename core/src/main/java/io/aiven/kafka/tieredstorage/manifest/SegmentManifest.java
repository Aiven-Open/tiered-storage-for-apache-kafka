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

package io.aiven.kafka.tieredstorage.manifest;

import java.util.Optional;

import io.aiven.kafka.tieredstorage.manifest.index.ChunkIndex;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * The segment manifest.
 *
 * <p>Contains various metadata about an uploaded segment.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "version")
@JsonSubTypes({
    @JsonSubTypes.Type(value = SegmentManifestV1.class, name = "1")
})
public interface SegmentManifest {
    ChunkIndex chunkIndex();

    boolean compression();

    Optional<SegmentEncryptionMetadata> encryption();
}