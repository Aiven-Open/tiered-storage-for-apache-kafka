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

package io.aiven.kafka.tieredstorage.commons.security.metadata;

import javax.crypto.SecretKey;

import java.io.IOException;
import java.util.Base64;

import io.aiven.kafka.tieredstorage.commons.security.RsaEncryptionProvider;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class EncryptedRepositoryMetadata {

    private final RsaEncryptionProvider rsaEncryptionProvider;

    private final ObjectMapper objectMapper;

    static final int VERSION = 1;

    public EncryptedRepositoryMetadata(final RsaEncryptionProvider rsaEncryptionProvider) {
        this.rsaEncryptionProvider = rsaEncryptionProvider;
        this.objectMapper = new ObjectMapper();
    }

    public byte[] serialize(final SecretKey dataKey) throws JsonProcessingException {
        final var encryptedKey = Base64.getEncoder().encodeToString(rsaEncryptionProvider.encryptDataKey(dataKey));
        return objectMapper.writeValueAsBytes(new EncryptionMetadata(encryptedKey, VERSION));
    }

    public byte[] deserialize(final byte[] metadata) throws IOException {
        final EncryptionMetadata encryptionMetadata;
        try {
            encryptionMetadata = objectMapper.readValue(metadata, EncryptionMetadata.class);
        } catch (final Exception e) {
            throw new IOException("Couldn't read JSON metadata", e);
        }
        if (encryptionMetadata.version() != VERSION) {
            throw new IOException("Unsupported metadata version");
        }
        final var encryptedKey =
            Base64.getDecoder().decode(encryptionMetadata.encryptionMetadata());
        return rsaEncryptionProvider.decryptDataKey(encryptedKey);
    }

}
