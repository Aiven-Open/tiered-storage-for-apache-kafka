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

package io.aiven.kafka.tieredstorage.iceberg;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.header.Headers;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

public interface StructureProvider extends Configurable {
    ParsedSchema getSchemaById(final int schemaId) throws RestClientException, IOException;

    ByteBuffer serializeKey(final String topic, final Headers headers, final Object value);

    ByteBuffer serializeValue(final String topic, final Headers headers, final Object value);

    Object deserializeKey(final String topic, final Headers headers, final byte[] data);

    Object deserializeValue(final String topic, final Headers headers, final byte[] data);
}
