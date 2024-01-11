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

package io.aiven.kafka.tieredstorage.fetch;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType;

import io.aiven.kafka.tieredstorage.fetch.index.SegmentIndexKey;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SegmentIndexKeyTest {
    static final ObjectKey OBJECT_KEY_1 = () -> "topic/" + Uuid.randomUuid();
    static final ObjectKey OBJECT_KEY_2 = () -> "topic/" + Uuid.randomUuid();

    @Test
    void identical() {
        final var ck1 = new SegmentIndexKey(OBJECT_KEY_1, IndexType.OFFSET);
        final var ck2 = new SegmentIndexKey(OBJECT_KEY_1, IndexType.OFFSET);
        assertThat(ck1).isEqualTo(ck2);
        assertThat(ck2).isEqualTo(ck1);
        assertThat(ck1).hasSameHashCodeAs(ck2);
    }

    @Test
    void differentObjectKey() {
        final var ck1 = new SegmentIndexKey(OBJECT_KEY_1, IndexType.OFFSET);
        final var ck2 = new SegmentIndexKey(OBJECT_KEY_2, IndexType.OFFSET);
        assertThat(ck1).isNotEqualTo(ck2);
        assertThat(ck2).isNotEqualTo(ck1);
        assertThat(ck1).doesNotHaveSameHashCodeAs(ck2);
    }

    @Test
    void differentIndexTypes() {
        final var ck1 = new SegmentIndexKey(OBJECT_KEY_1, IndexType.OFFSET);
        final var ck2 = new SegmentIndexKey(OBJECT_KEY_1, IndexType.TIMESTAMP);
        assertThat(ck1).isNotEqualTo(ck2);
        assertThat(ck2).isNotEqualTo(ck1);
        assertThat(ck1).doesNotHaveSameHashCodeAs(ck2);
    }
}
