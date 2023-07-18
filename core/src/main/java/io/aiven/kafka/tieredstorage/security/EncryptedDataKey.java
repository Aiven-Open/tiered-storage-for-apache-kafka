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

import java.util.Base64;
import java.util.Objects;

public class EncryptedDataKey {
    public final String keyEncryptionKeyId;
    public final byte[] encryptedDataKey;

    public EncryptedDataKey(final String keyEncryptionKeyId, final byte[] encryptedDataKey) {
        this.keyEncryptionKeyId = Objects.requireNonNull(keyEncryptionKeyId, "keyEncryptionKeyId cannot be null");
        if (keyEncryptionKeyId.isBlank()) {
            throw new IllegalArgumentException("keyEncryptionKeyId cannot be blank");
        }
        this.encryptedDataKey = Objects.requireNonNull(encryptedDataKey, "encryptedDataKey cannot be null");
        if (this.encryptedDataKey.length == 0) {
            throw new IllegalArgumentException("encryptedDataKey cannot be empty");
        }
    }

    public static EncryptedDataKey parse(final String keyText) {
        final int colonI = keyText.indexOf(':');
        if (colonI < 0) {
            throw new IllegalArgumentException("Malformed encrypted data key string: " + keyText);
        }
        // Sanity check for more than one ':'
        final int lastColonI = keyText.lastIndexOf(':');
        if (lastColonI != colonI) {
            throw new IllegalArgumentException("Malformed encrypted data key string: " + keyText);
        }

        final String keyEncryptionKeyId = keyText.substring(0, colonI);
        final String dataKeyBase64 = keyText.substring(colonI + 1);
        try {
            final byte[] decoded = Base64.getDecoder().decode(dataKeyBase64);
            return new EncryptedDataKey(keyEncryptionKeyId, decoded);
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException("Malformed encrypted data key string: " + keyText, e);
        }
    }

    public String serialize() {
        return keyEncryptionKeyId + ":" + Base64.getEncoder().encodeToString(encryptedDataKey);
    }

    @Override
    public String toString() {
        return "EncryptedDataKey(" + serialize() + ")";
    }
}
