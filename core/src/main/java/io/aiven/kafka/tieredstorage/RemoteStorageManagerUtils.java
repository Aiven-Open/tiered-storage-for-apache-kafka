/*
 * Copyright 2025 Aiven Oy
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

import java.security.KeyPair;
import java.util.HashMap;
import java.util.Map;

import io.aiven.kafka.tieredstorage.config.RemoteStorageManagerConfig;
import io.aiven.kafka.tieredstorage.manifest.serde.EncryptionSerdeModule;
import io.aiven.kafka.tieredstorage.manifest.serde.KafkaTypeSerdeModule;
import io.aiven.kafka.tieredstorage.security.AesEncryptionProvider;
import io.aiven.kafka.tieredstorage.security.RsaEncryptionProvider;
import io.aiven.kafka.tieredstorage.security.RsaKeyReader;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

class RemoteStorageManagerUtils {
    static RsaEncryptionProvider getRsaEncryptionProvider(final RemoteStorageManagerConfig config) {
        if (config.encryptionEnabled()) {
            final Map<String, KeyPair> keyRing = new HashMap<>();
            config.encryptionKeyRing().forEach((keyId, keyPaths) ->
                keyRing.put(keyId, RsaKeyReader.read(keyPaths.publicKey, keyPaths.privateKey))
            );
            return new RsaEncryptionProvider(config.encryptionKeyPairId(), keyRing);
        } else {
            return null;
        }
    }

    static AesEncryptionProvider getAesEncryptionProvider(final RemoteStorageManagerConfig config) {
        if (config.encryptionEnabled()) {
            return new AesEncryptionProvider();
        } else {
            return null;
        }
    }

    static ObjectMapper getObjectMapper(final RemoteStorageManagerConfig config,
                                        final RsaEncryptionProvider rsaEncryptionProvider) {
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new Jdk8Module());
        objectMapper.registerModule(KafkaTypeSerdeModule.create());
        if (config.encryptionEnabled()) {
            objectMapper.registerModule(EncryptionSerdeModule.create(rsaEncryptionProvider));
        }
        return objectMapper;

    }
}
