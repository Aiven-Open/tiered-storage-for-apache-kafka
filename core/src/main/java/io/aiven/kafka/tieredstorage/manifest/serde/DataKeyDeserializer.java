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

package io.aiven.kafka.tieredstorage.manifest.serde;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

import io.aiven.kafka.tieredstorage.security.EncryptedDataKey;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

class DataKeyDeserializer extends StdDeserializer<SecretKey> {
    private final Function<EncryptedDataKey, byte[]> keyDecryptor;

    DataKeyDeserializer(final Function<EncryptedDataKey, byte[]> keyDecryptor) {
        super(SecretKey.class);
        this.keyDecryptor = Objects.requireNonNull(keyDecryptor, "keyDecryptor cannot be null");
    }

    @Override
    public SecretKey deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException {
        final EncryptedDataKey encryptedDataKey;
        try {
            encryptedDataKey = EncryptedDataKey.parse(p.getText());
        } catch (final IllegalArgumentException e) {
            throw new JsonParseException(p, "Error parsing encrypted data key string", e);
        }
        final var decryptedKey = keyDecryptor.apply(encryptedDataKey);
        return new SecretKeySpec(decryptedKey, "AES");
    }
}
