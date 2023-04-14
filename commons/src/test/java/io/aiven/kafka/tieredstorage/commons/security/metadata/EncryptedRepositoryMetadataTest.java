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

import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Base64;

import io.aiven.kafka.tieredstorage.commons.RsaKeyAwareTest;
import io.aiven.kafka.tieredstorage.commons.security.EncryptionKeyProvider;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EncryptedRepositoryMetadataTest extends RsaKeyAwareTest {

    static final String METADATA_JSON_PATTERN =
        "{ \"encryptionMetadata\": \"%s\", \"version\": %s }";

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

        final var encKeyMetadata = new ObjectMapper().readValue(bytes, EncryptionMetadata.class);

        final var encryptedKey = Base64.getDecoder().decode(encKeyMetadata.encryptionMetadata());

        assertThat(new SecretKeySpec(encryptionKeyProvider.decryptKey(encryptedKey), "AES")).isEqualTo(encKey);
        assertThat(encKeyMetadata.version()).isEqualTo(EncryptedRepositoryMetadata.VERSION);
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

        assertThat(new SecretKeySpec(savedKey, "AES")).isEqualTo(encKey);
    }

    @Test
    void deserializationShouldThrowIOExceptionForWrongJson() {

        final var encryptedKey =
            encryptionKeyProvider
                .encryptKey(encryptionKeyProvider.createKey());

        assertThatThrownBy(() -> deserializeMetadata(""))
            .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> deserializeMetadata("some_text"))
            .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> deserializeMetadata("\"asd\": 1"))
            .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> deserializeMetadata("{\"asd\": \"asd\""))
            .isInstanceOf(IOException.class);

        final var badJsonObjectDefinitionWithKey =
            String.format(
                "{\"key\": \"%s\"",
                Base64.getEncoder().encodeToString(encryptedKey));
        assertThatThrownBy(() -> deserializeMetadata(badJsonObjectDefinitionWithKey))
            .isInstanceOf(IOException.class);

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
        assertThatThrownBy(() -> deserializeMetadata(jsonWithWrongVersion))
            .isInstanceOf(IOException.class)
            .hasMessage("Unsupported metadata version");
    }


    private byte[] deserializeMetadata(final String json) throws IOException {
        return new EncryptedRepositoryMetadata(encryptionKeyProvider)
            .deserialize(json.getBytes(StandardCharsets.UTF_8));
    }


}
