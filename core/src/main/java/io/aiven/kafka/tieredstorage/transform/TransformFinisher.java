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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Enumeration;
import java.util.Objects;

import io.aiven.kafka.tieredstorage.manifest.index.AbstractChunkIndexBuilder;
import io.aiven.kafka.tieredstorage.manifest.index.ChunkIndex;
import io.aiven.kafka.tieredstorage.manifest.index.FixedSizeChunkIndexBuilder;
import io.aiven.kafka.tieredstorage.manifest.index.VariableSizeChunkIndexBuilder;

import io.github.bucket4j.Bucket;

// TODO test transforms and detransforms with property-based tests

/**
 * The transformation finisher.
 *
 * <p>It converts enumeration of {@code byte[]} into enumeration of {@link InputStream},
 * so that it could be used in {@link SequenceInputStream}.
 *
 * <p>It's responsible for building the chunk index.
 */
public class TransformFinisher implements Enumeration<InputStream> {
    private final TransformChunkEnumeration inner;
    private final AbstractChunkIndexBuilder chunkIndexBuilder;
    private final int originalFileSize;
    private ChunkIndex chunkIndex = null;
    private final Bucket rateLimitingBucket;

    public TransformFinisher(final TransformChunkEnumeration inner, final Bucket rateLimitingBucket) {
        this(inner, 0, rateLimitingBucket);
    }

    public TransformFinisher(
        final TransformChunkEnumeration inner,
        final int originalFileSize,
        final Bucket rateLimitingBucket
    ) {
        this.inner = Objects.requireNonNull(inner, "inner cannot be null");
        this.originalFileSize = originalFileSize;

        if (originalFileSize < 0) {
            throw new IllegalArgumentException(
                "originalFileSize must be non-negative, " + originalFileSize + " given");
        }

        final Integer transformedChunkSize = inner.transformedChunkSize();
        if (originalFileSize == 0) {
            this.chunkIndexBuilder = null;
        } else if (transformedChunkSize == null) {
            this.chunkIndexBuilder = new VariableSizeChunkIndexBuilder(inner.originalChunkSize(), originalFileSize);
        } else {
            this.chunkIndexBuilder = new FixedSizeChunkIndexBuilder(
                inner.originalChunkSize(), originalFileSize, transformedChunkSize);
        }

        this.rateLimitingBucket = rateLimitingBucket;
    }

    @Override
    public boolean hasMoreElements() {
        return inner.hasMoreElements();
    }

    @Override
    public InputStream nextElement() {
        final var chunk = inner.nextElement();
        if (chunkIndexBuilder != null) {
            if (hasMoreElements()) {
                this.chunkIndexBuilder.addChunk(chunk.length);
            } else {
                this.chunkIndex = this.chunkIndexBuilder.finish(chunk.length);
            }
        }

        return new ByteArrayInputStream(chunk);
    }

    public ChunkIndex chunkIndex() {
        if (chunkIndex == null && originalFileSize > 0) {
            throw new IllegalStateException("Chunk index was not built, was finisher used?");
        }
        return this.chunkIndex;
    }

    public InputStream toInputStream() {
        final SequenceInputStream sequencedInputStream = new SequenceInputStream(this);
        if (rateLimitingBucket == null) {
            return sequencedInputStream;
        } else {
            return new RateLimitedInputStream(sequencedInputStream, rateLimitingBucket);
        }
    }
}
