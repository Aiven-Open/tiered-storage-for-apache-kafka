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

import io.aiven.kafka.tieredstorage.security.RsaEncryptionProvider;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;

public final class EncryptionSerdeModule {
    public static Module create(final RsaEncryptionProvider rsaEncryptionProvider) {
        final var module = new SimpleModule();

        module.addSerializer(SecretKey.class,
            new DataKeySerializer(rsaEncryptionProvider::encryptDataKey));
        module.addDeserializer(SecretKey.class,
            new DataKeyDeserializer(rsaEncryptionProvider::decryptDataKey));

        return module;
    }
}
