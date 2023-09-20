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

package io.aiven.kafka.tieredstorage.benchmark;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.RemoteLogInputStream;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

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
import org.openjdk.jmh.profile.AsyncProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 2, time = 10)
@Measurement(iterations = 3, time = 30)
@BenchmarkMode({Mode.SampleTime, Mode.Throughput})
@OutputTimeUnit(TimeUnit.SECONDS)
public class RemoteStorageManagerFetchLogBenchmark {
    RemoteStorageManager rsm = new RemoteStorageManager();

    @Setup(Level.Trial)
    public void setup() throws IOException {

        final var tmpDir = Files.createTempDirectory("rsm-cache");
        final var compression = false;
        final var encryption = true;
        final var cacheClass = DiskBasedChunkCache.class.getCanonicalName();
        // Configure the RSM.
        final var cacheDir = tmpDir.resolve("cache");
        Files.createDirectories(cacheDir);

        final var props = new Properties();
        props.load(Files.newInputStream(Path.of("rsm.properties")));
        final Map<String, String> config = new HashMap<>();
        props.forEach((k, v) -> config.put((String) k, (String) v));
        // 5MiB
        final int chunkSize = 5 * 1024 * 1024;
        config.putAll(Map.of(
            "chunk.size", Integer.toString(chunkSize),
            "compression.enabled", Boolean.toString(compression),
            "encryption.enabled", Boolean.toString(encryption),
            "chunk.cache.class", cacheClass,
            "chunk.cache.path", cacheDir.toString(),
            "chunk.cache.size", Integer.toString(100 * 1024 * 1024),
            "custom.metadata.fields.include", "REMOTE_SIZE,OBJECT_PREFIX,OBJECT_KEY",
            "key.prefix",
            "jorge-quilcate-test/13b37dc8-c8b6-4421-9ee2-032710da1e5d/c737975b-68fc-49ae-a434-4c740fba0c96/"
        ));

        rsm.configure(config);
    }

    @Benchmark
    public int read(final Blackhole blackhole) {
        final var tip =
            new TopicIdPartition(Uuid.fromString("xPDCxWn7SdC5HGRVktllxg"), 0, "test-topic-0000000-1riOu0I");
        final RemoteLogSegmentMetadata meta = new RemoteLogSegmentMetadata(
            new RemoteLogSegmentId(tip, Uuid.fromString("i7VvlUbeQR-ncOPMXpILYg")), 0, 2185L,
            0, 0, 0, 200 * 1024 * 1024, Map.of(0, 0L));
        InputStream content = null;
        try {
            content = rsm.fetchLogSegment(meta, 0);
            final var rlis = new RemoteLogInputStream(content);
            final var bytes = rlis.nextBatch();
            blackhole.consume(bytes);
            return bytes.sizeInBytes();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (content != null) {
                Utils.closeQuietly(content, "yay");
            }
        }
    }

    public static void main(final String[] args) throws Exception {
        new Runner(new OptionsBuilder()
            .include(RemoteStorageManagerFetchLogBenchmark.class.getSimpleName())
            .addProfiler(AsyncProfiler.class, "-output=flamegraph;event=cpu")
            .build()).run();
    }
}
