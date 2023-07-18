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

package io.aiven.kafka.tieredstorage.security;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EncryptedDataKeyTest {
    static final byte[] SOME_BYTES = {0, 1, 2, 3};

    @Test
    void constructorNoNullId() {
        assertThatThrownBy(() -> new EncryptedDataKey(null, SOME_BYTES))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("keyEncryptionKeyId cannot be null");
    }

    @Test
    void constructorNoBlankId() {
        assertThatThrownBy(() -> new EncryptedDataKey("", SOME_BYTES))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("keyEncryptionKeyId cannot be blank");
    }

    @Test
    void constructorNoNullKey() {
        assertThatThrownBy(() -> new EncryptedDataKey("id", null))
            .isInstanceOf(NullPointerException.class)
            .hasMessage("encryptedDataKey cannot be null");
    }

    @Test
    void constructorNoEmptyKey() {
        assertThatThrownBy(() -> new EncryptedDataKey("id", new byte[0]))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("encryptedDataKey cannot be empty");
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "AAECAw==",  // no :
        "xx::AAECAw==",  // double ::
        "xx:AAECAw==:xx",  // two :
        "id:INVALIDBASE64", // invalid Base64
        "id:",  // empty key
        ":",  // empty id and key
        ":AAECAw==",  // empty id
        "  :AAECAw=="  // blank id
    })
    void parseBadStrings(final String badString) {
        assertThatThrownBy(() -> EncryptedDataKey.parse(badString))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Malformed encrypted data key string: " + badString);
    }

    @Test
    void parseGoodString() {
        final EncryptedDataKey parsed = EncryptedDataKey.parse("id:AAECAw==");
        assertThat(parsed.keyEncryptionKeyId).isEqualTo("id");
        assertThat(parsed.encryptedDataKey).isEqualTo(SOME_BYTES);
    }

    @Test
    void serialize() {
        final EncryptedDataKey encryptedDataKey = new EncryptedDataKey("id", SOME_BYTES);
        assertThat(encryptedDataKey.serialize()).isEqualTo("id:AAECAw==");
    }
}
