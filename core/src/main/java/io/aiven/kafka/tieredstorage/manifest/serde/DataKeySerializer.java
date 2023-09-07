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

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

import io.aiven.kafka.tieredstorage.security.EncryptedDataKey;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

class DataKeySerializer extends StdSerializer<SecretKey> {
    private final Function<byte[], EncryptedDataKey> dataKeyEncryptor;

    DataKeySerializer(final Function<byte[], EncryptedDataKey> dataKeyEncryptor) {
        super(SecretKey.class);
        this.dataKeyEncryptor = Objects.requireNonNull(dataKeyEncryptor, "dataKeyEncryptor cannot be null");
    }

    @Override
    public void serialize(final SecretKey value,
                          final JsonGenerator gen,
                          final SerializerProvider provider) throws IOException {
        final EncryptedDataKey encryptionResult = dataKeyEncryptor.apply(value.getEncoded());
        gen.writeString(encryptionResult.serialize());
    }
}
