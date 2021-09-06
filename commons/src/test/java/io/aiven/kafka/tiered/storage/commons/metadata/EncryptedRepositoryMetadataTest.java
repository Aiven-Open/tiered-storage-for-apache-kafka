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

package io.aiven.kafka.tiered.storage.commons.metadata;

import javax.crypto.SecretKey;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Base64;

import io.aiven.kafka.tiered.storage.commons.RsaKeyAwareTest;
import io.aiven.kafka.tiered.storage.commons.security.EncryptionKeyProvider;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EncryptedRepositoryMetadataTest extends RsaKeyAwareTest {

    static final String METADATA_JSON_PATTERN =
        "{ \"key\": \"%s\", \"version\": %s }";

    EncryptionKeyProvider encryptionKeyProvider;

    @BeforeEach
    void setUp() throws IOException {
        encryptionKeyProvider =
            EncryptionKeyProvider.of(
                Files.newInputStream(publicKeyPem),
                Files.newInputStream(privateKeyPem)
            );
    }

    @Test
    void shouldSerializeMetadata() throws IOException {
        final var encKey = encryptionKeyProvider.createKey();
        final var bytes = new EncryptedRepositoryMetadata(encryptionKeyProvider).serialize(encKey);

        final var encKeyMetadata = new ObjectMapper().readValue(bytes, EncryptionKeyMetadata.class);

        final var encryptedKey = Base64.getDecoder().decode(encKeyMetadata.key());

        assertEquals(encKey, encryptionKeyProvider.decryptKey(encryptedKey));
        assertEquals(EncryptedRepositoryMetadata.VERSION, encKeyMetadata.version());
    }

    @Test
    void shouldDeserializeMetadata() throws IOException {
        final var encKey = encryptionKeyProvider.createKey();
        final var encryptedKey = encryptionKeyProvider.encryptKey(encKey);

        final var json =
            String.format(
                METADATA_JSON_PATTERN,
                Base64.getEncoder().encodeToString(encryptedKey),
                EncryptedRepositoryMetadata.VERSION);

        final var savedKey = deserializeMetadata(json);

        assertEquals(encKey, savedKey);
    }

    @Test
    void deserializationShouldThrowIOExceptionForWrongJson() {

        final var encryptedKey =
            encryptionKeyProvider
                .encryptKey(encryptionKeyProvider.createKey());

        assertThrows(
            IOException.class,
            () -> deserializeMetadata(""));

        assertThrows(
            IOException.class,
            () -> deserializeMetadata("some_text"));

        assertThrows(
            IOException.class,
            () -> deserializeMetadata("\"asd\": 1"));

        assertThrows(
            IOException.class,
            () -> deserializeMetadata("{\"asd\": \"asd\""));

        final var badJsonObjectDefinitionWithKey =
            String.format(
                "{\"key\": \"%s\"",
                Base64.getEncoder().encodeToString(encryptedKey));
        assertThrows(
            IOException.class,
            () -> deserializeMetadata(badJsonObjectDefinitionWithKey));

    }

    @Test
    void deserializationShouldThrowIOExceptionForWrongVersion() {
        final var encryptedKey =
            encryptionKeyProvider
                .encryptKey(encryptionKeyProvider.createKey());

        final var jsonWithWrongVersion =
            String.format(
                METADATA_JSON_PATTERN,
                Base64.getEncoder().encodeToString(encryptedKey), 100);
        final var e = assertThrows(
            IOException.class,
            () -> deserializeMetadata(jsonWithWrongVersion));

        assertEquals("Unsupported metadata version", e.getMessage());
    }


    private SecretKey deserializeMetadata(final String json) throws IOException {
        return new EncryptedRepositoryMetadata(encryptionKeyProvider)
            .deserialize(json.getBytes(StandardCharsets.UTF_8));
    }


}
