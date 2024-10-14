/*
 * Copyright 2024 Aiven Oy
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

package io.aiven.kafka.tieredstorage.misc;

import org.apache.kafka.common.config.ConfigDef;

import io.aiven.kafka.tieredstorage.config.ChunkCacheConfig;
import io.aiven.kafka.tieredstorage.config.ChunkManagerFactoryConfig;
import io.aiven.kafka.tieredstorage.config.DiskChunkCacheConfig;
import io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig;
import io.aiven.kafka.tieredstorage.fetch.index.MemorySegmentIndexesCache;
import io.aiven.kafka.tieredstorage.fetch.manifest.MemorySegmentManifestCache;
import io.aiven.kafka.tieredstorage.storage.azure.AzureBlobStorageConfig;
import io.aiven.kafka.tieredstorage.storage.filesystem.FileSystemStorageConfig;
import io.aiven.kafka.tieredstorage.storage.gcs.GcsStorageConfig;
import io.aiven.kafka.tieredstorage.storage.s3.S3StorageConfig;

import static io.aiven.kafka.tieredstorage.config.ChunkManagerFactoryConfig.FETCH_CHUNK_CACHE_PREFIX;
import static io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig.FETCH_INDEXES_CACHE_PREFIX;
import static io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig.SEGMENT_MANIFEST_CACHE_PREFIX;
import static io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig.STORAGE_PREFIX;

/**
 * Gather all config definitions across the project and generate a documentation page
 **/
public class ConfigDocs {
    public static void main(final String[] args) {
        printSectionTitle("Core components");

        printSubsectionTitle("RemoteStorageManagerConfig");
        final var rsmConfigDef = RemoteStorageManagerConfig.configDef();
        System.out.println(rsmConfigDef.toEnrichedRst());

        printSubsectionTitle("SegmentManifestCacheConfig");
        System.out.println("Under ``" + SEGMENT_MANIFEST_CACHE_PREFIX + "``\n");
        final var segmentManifestCacheDef = MemorySegmentManifestCache.configDef();
        System.out.println(segmentManifestCacheDef.toEnrichedRst());

        printSubsectionTitle("SegmentIndexesCacheConfig");
        System.out.println("Under ``" + FETCH_INDEXES_CACHE_PREFIX + "``\n");
        final var segmentIndexesCacheDef = MemorySegmentIndexesCache.configDef();
        System.out.println(segmentIndexesCacheDef.toEnrichedRst());

        printSubsectionTitle("ChunkManagerFactoryConfig");
        final var chunkCacheFactoryDef = ChunkManagerFactoryConfig.configDef();
        System.out.println(chunkCacheFactoryDef.toEnrichedRst());

        printSubsectionTitle("MemoryChunkCacheConfig");
        System.out.println("Under ``" + FETCH_CHUNK_CACHE_PREFIX + "``\n");
        final var memChunkCacheDef = ChunkCacheConfig.configDef(new ConfigDef());
        System.out.println(memChunkCacheDef.toEnrichedRst());

        printSubsectionTitle("DiskChunkCacheConfig");
        System.out.println("Under ``" + FETCH_CHUNK_CACHE_PREFIX + "``\n");
        final var diskChunkCacheDef = DiskChunkCacheConfig.configDef();
        System.out.println(diskChunkCacheDef.toEnrichedRst());

        printSectionTitle("Storage Backends");
        System.out.println("Under ``" + STORAGE_PREFIX + "``\n");

        printSubsectionTitle("AzureBlobStorageStorageConfig");
        final var azBlobStorageConfigDef = AzureBlobStorageConfig.configDef();
        System.out.println(azBlobStorageConfigDef.toEnrichedRst());

        printSubsectionTitle("AzureBlobStorageStorageConfig");
        final var googleCloudConfigDef = GcsStorageConfig.configDef();
        System.out.println(googleCloudConfigDef.toEnrichedRst());

        printSubsectionTitle("S3StorageConfig");
        final var s3StorageConfigDef = S3StorageConfig.configDef();
        System.out.println(s3StorageConfigDef.toEnrichedRst());
        
        printSubsectionTitle("FilesystemStorageConfig");
        System.out.println("> Only for development/testing purposes");
        final var fsStorageConfigDef = FileSystemStorageConfig.configDef();
        System.out.println(fsStorageConfigDef.toEnrichedRst());
    }

    static void printSectionTitle(final String title) {
        System.out.println("=================\n"
            + title + "\n"
            + "=================");
    }

    static void printSubsectionTitle(final String title) {
        System.out.println("-----------------\n"
            + title + "\n"
            + "-----------------");
    }
}
