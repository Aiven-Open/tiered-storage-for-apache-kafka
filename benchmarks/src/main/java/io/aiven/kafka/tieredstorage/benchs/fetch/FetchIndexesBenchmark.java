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

package io.aiven.kafka.tieredstorage.benchs.fetch;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteResourceNotFoundException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType;

import io.aiven.kafka.tieredstorage.RemoteStorageManager;
import io.aiven.kafka.tieredstorage.chunkmanager.cache.DiskBasedChunkCache;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 4, time = 10)
@Measurement(iterations = 16, time = 30)
@BenchmarkMode({Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class FetchIndexesBenchmark {

    // s3://jeqo-test1/tiered-storage-demo/t1-QW1...-Q/0/00000000000000057362-ABC....indexes
    final TopicIdPartition tip =
        new TopicIdPartition(Uuid.fromString("QW1-OwYOSt6w-CBTuczz-Q"), 0, "t1");
    final RemoteLogSegmentMetadata meta = new RemoteLogSegmentMetadata(
        new RemoteLogSegmentId(tip, Uuid.fromString("AYSp9LTtQyqRJF6u5bHdVg")), 57362L, 58302L - 1,
        0, 0, 0, 200 * 1024 * 1024, Map.of(0, 0L));

    RemoteStorageManager rsm = new RemoteStorageManager();

    @Setup(Level.Trial)
    public void setup() throws IOException {
        final var tmpDir = Files.createTempDirectory("rsm-cache");
        final var compression = false;
        final var encryption = false;
        final var cacheClass = DiskBasedChunkCache.class.getCanonicalName();
        // Configure the RSM.
        final var cacheDir = tmpDir.resolve("cache");
        Files.createDirectories(cacheDir);

        final var props = new Properties();
        props.load(Files.newInputStream(Path.of("rsm.properties")));
        final Map<String, String> config = new HashMap<>();
        props.forEach((k, v) -> config.put((String) k, (String) v));
        // 4MiB
        final int chunkSize = 4 * 1024 * 1024;
        config.putAll(Map.of(
            "chunk.size", Integer.toString(chunkSize),
            "compression.enabled", Boolean.toString(compression),
            "encryption.enabled", Boolean.toString(encryption),
            "chunk.cache.class", cacheClass,
            "chunk.cache.path", cacheDir.toString(),
            "chunk.cache.size", Integer.toString(100 * 1024 * 1024),
            "custom.metadata.fields.include", "REMOTE_SIZE,OBJECT_PREFIX,OBJECT_KEY"
        ));

        rsm.configure(config);
    }

    @Benchmark
    public void fetchIndexesV1(final Blackhole b) throws RemoteStorageException {
        final var offsetindex = rsm.fetchIndex(meta, IndexType.OFFSET);
        final var timeindex = rsm.fetchIndex(meta, IndexType.TIMESTAMP);
        try {
            final var txnindex = rsm.fetchIndex(meta, IndexType.TRANSACTION);
            b.consume(offsetindex);
            b.consume(timeindex);
            b.consume(txnindex);
        } catch (final RemoteResourceNotFoundException e) {
            b.consume(offsetindex);
            b.consume(timeindex);
        }
    }

    @Benchmark
    public void fetchIndexesV2(final Blackhole b) throws RemoteStorageException {
        final var indexes = rsm.fetchIndexes(
            meta,
            Set.of(IndexType.OFFSET, IndexType.TIMESTAMP, IndexType.TRANSACTION));
        b.consume(indexes);
    }


    @Benchmark
    public void fetchAllIndexesV2(final Blackhole b) throws RemoteStorageException {
        final var indexes = rsm.fetchAllIndexes(meta);
        b.consume(indexes);
    }
}
