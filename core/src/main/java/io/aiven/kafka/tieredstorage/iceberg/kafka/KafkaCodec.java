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

package io.aiven.kafka.tieredstorage.iceberg.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import net.jpountz.lz4.LZ4FrameInputStream;
import net.jpountz.lz4.LZ4FrameOutputStream;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

/**
 * The compression codecs of Kafka record batches, with Kafka's compression type ids.
 */
public enum KafkaCodec {
    NONE(0),
    GZIP(1),
    SNAPPY(2),
    LZ4(3),
    ZSTD(4);

    private final int id;

    KafkaCodec(final int id) {
        this.id = id;
    }

    public int id() {
        return id;
    }

    public static KafkaCodec forId(final int id) {
        for (final KafkaCodec codec : values()) {
            if (codec.id == id) {
                return codec;
            }
        }
        throw new IllegalArgumentException("Unknown compression type " + id);
    }

    InputStream decompress(final InputStream in) throws IOException {
        return switch (this) {
            case NONE -> in;
            case GZIP -> new GZIPInputStream(in);
            case SNAPPY -> new SnappyInputStream(in);
            case LZ4 -> new LZ4FrameInputStream(in);
            case ZSTD -> new ZstdInputStream(in);
        };
    }

    OutputStream compress(final OutputStream out) throws IOException {
        return switch (this) {
            case NONE -> out;
            case GZIP -> new GZIPOutputStream(out);
            case SNAPPY -> new SnappyOutputStream(out);
            case LZ4 -> new LZ4FrameOutputStream(out);
            case ZSTD -> new ZstdOutputStream(out);
        };
    }
}
