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
import io.aiven.kafka.tieredstorage.storage.oci.OciStorageConfig;
import io.aiven.kafka.tieredstorage.storage.s3.S3StorageConfig;

import static io.aiven.kafka.tieredstorage.config.ChunkManagerFactoryConfig.FETCH_CHUNK_CACHE_PREFIX;
import static io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig.FETCH_INDEXES_CACHE_PREFIX;
import static io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig.SEGMENT_MANIFEST_CACHE_PREFIX;
import static io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig.STORAGE_PREFIX;
import static java.lang.System.out;

/**
 * Gather all config definitions across the project and generate a documentation page
 **/
public class ConfigsDocs {
    public static void main(final String[] args) {
        printSectionTitle("Core components");
        out.println(".. Generated from *Config.java classes by " + ConfigsDocs.class.getCanonicalName());
        out.println();

        printSubsectionTitle("RemoteStorageManagerConfig");
        final var rsmConfigDef = RemoteStorageManagerConfig.configDef();
        out.println(rsmConfigDef.toEnrichedRst());
        out.println();

        printSubsectionTitle("SegmentManifestCacheConfig");
        out.println("Under ``" + SEGMENT_MANIFEST_CACHE_PREFIX + "``\n");
        final var segmentManifestCacheDef = MemorySegmentManifestCache.configDef();
        out.println(segmentManifestCacheDef.toEnrichedRst());
        out.println();

        printSubsectionTitle("SegmentIndexesCacheConfig");
        out.println("Under ``" + FETCH_INDEXES_CACHE_PREFIX + "``\n");
        final var segmentIndexesCacheDef = MemorySegmentIndexesCache.configDef();
        out.println(segmentIndexesCacheDef.toEnrichedRst());
        out.println();

        printSubsectionTitle("ChunkManagerFactoryConfig");
        final var chunkCacheFactoryDef = ChunkManagerFactoryConfig.configDef();
        out.println(chunkCacheFactoryDef.toEnrichedRst());
        out.println();

        printSubsectionTitle("MemoryChunkCacheConfig");
        out.println("Under ``" + FETCH_CHUNK_CACHE_PREFIX + "``\n");
        final var memChunkCacheDef = ChunkCacheConfig.configDef(new ConfigDef());
        out.println(memChunkCacheDef.toEnrichedRst());
        out.println();

        printSubsectionTitle("DiskChunkCacheConfig");
        out.println("Under ``" + FETCH_CHUNK_CACHE_PREFIX + "``\n");
        final var diskChunkCacheDef = DiskChunkCacheConfig.configDef();
        out.println(diskChunkCacheDef.toEnrichedRst());
        out.println();

        printSectionTitle("Storage Backends");
        out.println("Under ``" + STORAGE_PREFIX + "``\n");

        printSubsectionTitle("AzureBlobStorageStorageConfig");
        final var azBlobStorageConfigDef = AzureBlobStorageConfig.configDef();
        out.println(azBlobStorageConfigDef.toEnrichedRst());
        out.println();

        printSubsectionTitle("AzureBlobStorageStorageConfig");
        final var googleCloudConfigDef = GcsStorageConfig.configDef();
        out.println(googleCloudConfigDef.toEnrichedRst());
        out.println();

        printSubsectionTitle("S3StorageConfig");
        final var s3StorageConfigDef = S3StorageConfig.configDef();
        out.println(s3StorageConfigDef.toEnrichedRst());
        out.println();

        printSubsectionTitle("OciStorageConfig");
        final var ociStorageConfigDef = OciStorageConfig.configDef();
        out.println(ociStorageConfigDef.toEnrichedRst());
        out.println();

        printSubsectionTitle("FilesystemStorageConfig");
        out.println(".. Only for development/testing purposes");
        final var fsStorageConfigDef = FileSystemStorageConfig.configDef();
        out.println(fsStorageConfigDef.toEnrichedRst());
        out.println();
    }

    static void printSectionTitle(final String title) {
        out.println("=================\n"
            + title + "\n"
            + "=================");
    }

    static void printSubsectionTitle(final String title) {
        out.println("-----------------\n"
            + title + "\n"
            + "-----------------");
    }
}
