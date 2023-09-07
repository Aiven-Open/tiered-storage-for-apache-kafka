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

package io.aiven.kafka.tieredstorage.manifest;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.util.Base64;

import io.aiven.kafka.tieredstorage.RsaKeyAwareTest;
import io.aiven.kafka.tieredstorage.manifest.index.FixedSizeChunkIndex;
import io.aiven.kafka.tieredstorage.manifest.serde.EncryptionSerdeModule;
import io.aiven.kafka.tieredstorage.security.EncryptedDataKey;
import io.aiven.kafka.tieredstorage.security.RsaEncryptionProvider;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SegmentManifestV1SerdeTest extends RsaKeyAwareTest {
    static final FixedSizeChunkIndex INDEX =
        new FixedSizeChunkIndex(100, 1000, 110, 110);
    static final SecretKey DATA_KEY = new SecretKeySpec(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, "AES");
    static final byte[] AAD = {10, 11, 12, 13};

    static final String WITH_ENCRYPTION_WITHOUT_SECRET_KEY_JSON =
        "{\"version\":\"1\","
            + "\"chunkIndex\":{\"type\":\"fixed\",\"originalChunkSize\":100,"
            + "\"originalFileSize\":1000,\"transformedChunkSize\":110,\"finalTransformedChunkSize\":110},"
            + "\"compression\":false,\"encryption\":{\"aad\":\"CgsMDQ==\"}}";
    static final String WITHOUT_ENCRYPTION_JSON =
        "{\"version\":\"1\","
            + "\"chunkIndex\":{\"type\":\"fixed\",\"originalChunkSize\":100,"
            + "\"originalFileSize\":1000,\"transformedChunkSize\":110,\"finalTransformedChunkSize\":110},"
            + "\"compression\":false}";

    ObjectMapper mapper;
    RsaEncryptionProvider rsaEncryptionProvider;

    @BeforeEach
    void init() {
        mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());

        rsaEncryptionProvider = new RsaEncryptionProvider(KEY_ENCRYPTION_KEY_ID, keyRing);
        mapper.registerModule(EncryptionSerdeModule.create(rsaEncryptionProvider));
    }

    @Test
    void withEncryption() throws JsonProcessingException {
        final SegmentManifest manifest = new SegmentManifestV1(INDEX, false,
            new SegmentEncryptionMetadataV1(DATA_KEY, AAD));

        final String jsonStr = mapper.writeValueAsString(manifest);

        // Check that the key is encrypted.
        final ObjectNode deserializedJson = (ObjectNode) mapper.readTree(jsonStr);
        final String dataKeyText = deserializedJson.get("encryption").get("dataKey").asText();
        final String[] dataKeyTextParts = dataKeyText.split(":");
        assertThat(dataKeyTextParts).hasSize(2);
        assertThat(dataKeyTextParts[0]).isEqualTo(KEY_ENCRYPTION_KEY_ID);
        final byte[] encryptedKey = Base64.getDecoder().decode(dataKeyTextParts[1]);
        final SecretKeySpec dataKey = new SecretKeySpec(rsaEncryptionProvider.decryptDataKey(
            new EncryptedDataKey(KEY_ENCRYPTION_KEY_ID, encryptedKey)), "AES");
        assertThat(dataKey).isEqualTo(DATA_KEY);

        // Remove the secret key--i.e. the variable part--and compare the JSON representation.
        ((ObjectNode) deserializedJson.get("encryption")).remove("dataKey");
        assertThat(mapper.writeValueAsString(deserializedJson)).isEqualTo(WITH_ENCRYPTION_WITHOUT_SECRET_KEY_JSON);

        // Check deserialization.
        final SegmentManifest deserializedManifest = mapper.readValue(jsonStr, SegmentManifest.class);
        assertThat(deserializedManifest).isEqualTo(manifest);
    }

    @Test
    void withoutEncryption() throws JsonProcessingException {
        final SegmentManifest manifest = new SegmentManifestV1(INDEX, false, null);

        final String jsonStr = mapper.writeValueAsString(manifest);

        // Compare the JSON representation.
        assertThat(jsonStr).isEqualTo(WITHOUT_ENCRYPTION_JSON);

        // Check deserialization.
        final SegmentManifest deserializedManifest = mapper.readValue(jsonStr, SegmentManifest.class);
        assertThat(deserializedManifest).isEqualTo(manifest);
    }
}
