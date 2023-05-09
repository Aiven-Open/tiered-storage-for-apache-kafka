/*
 * Copyright 2021 Aiven Oy
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

package io.aiven.kafka.tieredstorage.commons.security;

import io.aiven.kafka.tieredstorage.commons.EncryptionAwareTest;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class EncryptionProviderTest extends EncryptionAwareTest {

    @Test
    void alwaysGeneratesNewKey() {
        final var dataKey1 = encryptionProvider.createDataKeyAndAAD().dataKey;
        final var dataKey2 = encryptionProvider.createDataKeyAndAAD().dataKey;

        assertThat(dataKey1).isNotEqualTo(dataKey2);
    }

    @Test
    void decryptGeneratedKey() {
        final var dataKey = encryptionProvider.createDataKeyAndAAD().dataKey;
        final var encryptedKey = encryptionProvider.encryptDataKey(dataKey);
        final var restoredKey = encryptionProvider.decryptDataKey(encryptedKey);

        assertThat(restoredKey).isEqualTo(dataKey);
    }
}
