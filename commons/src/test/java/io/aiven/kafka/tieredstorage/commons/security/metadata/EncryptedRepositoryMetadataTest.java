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
import java.util.Base64;

import io.aiven.kafka.tieredstorage.commons.RsaKeyAwareTest;
import io.aiven.kafka.tieredstorage.commons.security.AesEncryptionProvider;
import io.aiven.kafka.tieredstorage.commons.security.RsaEncryptionProvider;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EncryptedRepositoryMetadataTest extends RsaKeyAwareTest {

    static final String METADATA_JSON_PATTERN =
        "{ \"encryptionMetadata\": \"%s\", \"version\": %s }";

    RsaEncryptionProvider rsaEncryptionProvider;
    AesEncryptionProvider aesEncryptionProvider;

    @BeforeEach
    void setUp() {
        rsaEncryptionProvider = RsaEncryptionProvider.of(publicKeyPem, privateKeyPem);
        aesEncryptionProvider = new AesEncryptionProvider();
    }

    @Test
    void shouldSerializeMetadata() throws IOException {
        final var dataKey = aesEncryptionProvider.createDataKey();
        final var bytes = new EncryptedRepositoryMetadata(rsaEncryptionProvider).serialize(dataKey);

        final var encryptionMetadata = new ObjectMapper().readValue(bytes, EncryptionMetadata.class);

        final var encryptedKey = Base64.getDecoder().decode(encryptionMetadata.encryptionMetadata());

        assertThat(new SecretKeySpec(rsaEncryptionProvider.decryptDataKey(encryptedKey), "AES")).isEqualTo(dataKey);
        assertThat(encryptionMetadata.version()).isEqualTo(EncryptedRepositoryMetadata.VERSION);
    }

    @Test
    void shouldDeserializeMetadata() throws IOException {
        final var dataKey = aesEncryptionProvider.createDataKey();
        final var encryptedKey = rsaEncryptionProvider.encryptDataKey(dataKey);

        final var json =
            String.format(
                METADATA_JSON_PATTERN,
                Base64.getEncoder().encodeToString(encryptedKey),
                EncryptedRepositoryMetadata.VERSION);

        final var savedKey = deserializeMetadata(json);

        assertThat(new SecretKeySpec(savedKey, "AES")).isEqualTo(dataKey);
    }

    @Test
    void deserializationShouldThrowIOExceptionForWrongJson() {
        final var encryptedKey = rsaEncryptionProvider.encryptDataKey(aesEncryptionProvider.createDataKey());

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
        final var encryptedKey = rsaEncryptionProvider.encryptDataKey(aesEncryptionProvider.createDataKey());

        final var jsonWithWrongVersion =
            String.format(
                METADATA_JSON_PATTERN,
                Base64.getEncoder().encodeToString(encryptedKey), 100);
        assertThatThrownBy(() -> deserializeMetadata(jsonWithWrongVersion))
            .isInstanceOf(IOException.class)
            .hasMessage("Unsupported metadata version");
    }

    private byte[] deserializeMetadata(final String json) throws IOException {
        return new EncryptedRepositoryMetadata(rsaEncryptionProvider)
            .deserialize(json.getBytes(StandardCharsets.UTF_8));
    }
}
