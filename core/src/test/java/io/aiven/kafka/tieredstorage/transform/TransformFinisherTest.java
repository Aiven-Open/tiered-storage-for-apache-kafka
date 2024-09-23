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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

import io.aiven.kafka.tieredstorage.Chunk;
import io.aiven.kafka.tieredstorage.manifest.index.ChunkIndex;
import io.aiven.kafka.tieredstorage.manifest.index.FixedSizeChunkIndex;
import io.aiven.kafka.tieredstorage.manifest.index.VariableSizeChunkIndex;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
class TransformFinisherTest {
    @Mock
    TransformChunkEnumeration inner;

    @Test
    void getIndexBeforeUsing() {
        final TransformChunkEnumeration enumerator = new FakeDataEnumerator(3);
        final TransformFinisher finisher = TransformFinisher.newBuilder(enumerator, 7).build();
        assertThatThrownBy(() -> finisher.chunkIndex())
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Chunk index was not built, was finisher used?");
    }

    @Test
    void nullInnerEnumeration() {
        assertThatThrownBy(() -> TransformFinisher.newBuilder(null, 100).build())
            .isInstanceOf(NullPointerException.class)
            .hasMessage("inner cannot be null");
    }

    @Test
    void negativeOriginalFileSize() {
        assertThatThrownBy(() -> TransformFinisher.newBuilder(inner, -1).build())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("originalFileSize must be non-negative, -1 given");
    }

    @Test
    void nullOriginalFilePath() {
        assertThatThrownBy(() ->
            TransformFinisher.newBuilder(inner, 100)
                .withOriginalFilePath(null)
                .build())
            .isInstanceOf(NullPointerException.class)
            .hasMessage("originalFilePath cannot be null");
    }

    @Test
    void emptyOriginalFilePath() {
        final var finisher = TransformFinisher.newBuilder(inner, 100).build();
        assertThat(finisher.maybeOriginalFilePath()).isEmpty();
    }

    @Test
    void presentOriginalFilePath(@TempDir final Path tmpDir) throws IOException {
        final var originalFilePath = tmpDir.resolve("test.log");
        Files.writeString(originalFilePath, "test");

        final var finisher = TransformFinisher.newBuilder(inner, 100)
            .withOriginalFilePath(originalFilePath)
            .build();
        assertThat(finisher.maybeOriginalFilePath()).isPresent();
        assertThat(finisher.maybeOriginalFilePath().get()).hasContent("test");
    }

    @ParameterizedTest
    @MethodSource("provideForBuildIndexAndReturnCorrectInputStreams")
    void buildIndexAndReturnCorrectInputStreams(final Integer transformedChunkSize,
                                                final Class<ChunkIndex> indexType) throws IOException {
        final TransformChunkEnumeration enumerator = new FakeDataEnumerator(transformedChunkSize);
        final TransformFinisher finisher = TransformFinisher.newBuilder(enumerator, 7).build();
        assertThat(finisher.hasMoreElements()).isTrue();
        assertThat(finisher.nextElement().readAllBytes()).isEqualTo(new byte[] {0, 1, 2});
        assertThat(finisher.hasMoreElements()).isTrue();
        assertThat(finisher.nextElement().readAllBytes()).isEqualTo(new byte[] {3, 4, 5});
        assertThat(finisher.hasMoreElements()).isTrue();
        assertThat(finisher.nextElement().readAllBytes()).isEqualTo(new byte[] {6});
        assertThat(finisher.hasMoreElements()).isFalse();

        final ChunkIndex chunkIndex = finisher.chunkIndex();
        assertThat(chunkIndex).isInstanceOf(indexType);
        assertThat(chunkIndex.chunks()).containsExactly(
            new Chunk(0, 0, 3, 0, 3),
            new Chunk(1, 3, 3, 3, 3),
            new Chunk(2, 6, 1, 6, 1)
        );
    }

    static Object[][] provideForBuildIndexAndReturnCorrectInputStreams() {
        return new Object[][] {
            new Object[] {3, FixedSizeChunkIndex.class},
            new Object[] {null, VariableSizeChunkIndex.class},
        };
    }

    private static class FakeDataEnumerator implements TransformChunkEnumeration {
        private final Integer transformedChunkSize;

        private final Iterator<byte[]> iter = List.of(
            new byte[] {0, 1, 2},
            new byte[] {3, 4, 5},
            new byte[] {6}
        ).iterator();

        private FakeDataEnumerator(final Integer transformedChunkSize) {
            this.transformedChunkSize = transformedChunkSize;
        }

        @Override
        public int originalChunkSize() {
            return 3;
        }

        @Override
        public Integer transformedChunkSize() {
            return transformedChunkSize;
        }

        @Override
        public boolean hasMoreElements() {
            return iter.hasNext();
        }

        @Override
        public byte[] nextElement() {
            return iter.next();
        }
    }
}
