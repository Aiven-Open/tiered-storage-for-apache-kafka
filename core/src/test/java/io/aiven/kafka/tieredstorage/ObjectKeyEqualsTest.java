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

package io.aiven.kafka.tieredstorage;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

public class ObjectKeyEqualsTest {
    @ParameterizedTest
    @ValueSource(classes = {ObjectKeyFactory.PlainObjectKey.class, ObjectKeyFactory.ObjectKeyWithMaskedPrefix.class})
    void identical(final Class<ObjectKeyFactory.PlainObjectKey> keyClass) throws ReflectiveOperationException {
        final var constructor = keyClass.getDeclaredConstructor(String.class, String.class);
        final var k1 = constructor.newInstance("prefix", "mainPathAndSuffix");
        final var k2 = constructor.newInstance("prefix", "mainPathAndSuffix");
        assertThat(k1).isEqualTo(k2);
        assertThat(k2).isEqualTo(k1);
        assertThat(k1).hasSameHashCodeAs(k2);
    }

    @ParameterizedTest
    @ValueSource(classes = {ObjectKeyFactory.PlainObjectKey.class, ObjectKeyFactory.ObjectKeyWithMaskedPrefix.class})
    void differentPrefix(final Class<ObjectKeyFactory.PlainObjectKey> keyClass) throws ReflectiveOperationException {
        final var constructor = keyClass.getDeclaredConstructor(String.class, String.class);
        final var k1 = constructor.newInstance("prefix1", "mainPathAndSuffix");
        final var k2 = constructor.newInstance("prefix2", "mainPathAndSuffix");
        assertThat(k1).isNotEqualTo(k2);
        assertThat(k2).isNotEqualTo(k1);
        assertThat(k1).doesNotHaveSameHashCodeAs(k2);
    }

    @ParameterizedTest
    @ValueSource(classes = {ObjectKeyFactory.PlainObjectKey.class, ObjectKeyFactory.ObjectKeyWithMaskedPrefix.class})
    void differentMainPathAndSuffix(final Class<ObjectKeyFactory.PlainObjectKey> keyClass)
        throws ReflectiveOperationException {
        final var constructor = keyClass.getDeclaredConstructor(String.class, String.class);
        final var k1 = constructor.newInstance("prefix", "mainPathAndSuffix1");
        final var k2 = constructor.newInstance("prefix", "mainPathAndSuffix2");
        assertThat(k1).isNotEqualTo(k2);
        assertThat(k2).isNotEqualTo(k1);
        assertThat(k1).doesNotHaveSameHashCodeAs(k2);
    }
}
