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

package io.aiven.kafka.tieredstorage.core.manifest.serde;

import javax.crypto.SecretKey;

import java.io.IOException;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class DataKeyDeserializer extends StdDeserializer<SecretKey> {
    private final Function<byte[], SecretKey> keyDecryptor;

    public DataKeyDeserializer(final Function<byte[], SecretKey> keyDecryptor) {
        super(SecretKey.class);
        this.keyDecryptor = keyDecryptor;
    }

    @Override
    public SecretKey deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException {
        return keyDecryptor.apply(p.getBinaryValue());
    }
}
