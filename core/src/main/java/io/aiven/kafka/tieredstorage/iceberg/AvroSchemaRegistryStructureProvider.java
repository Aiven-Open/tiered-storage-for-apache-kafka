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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.header.Headers;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;

public class AvroSchemaRegistryStructureProvider implements StructureProvider {
    private static final int SUBJECT_NOT_FOUND_ERROR_CODE = 40401;
    private static final int SCHEMA_NOT_FOUND_ERROR_CODE = 40403;
    private static final int JSON_PARSE_ERROR_CODE = 50005;

    private final KafkaAvroSerializer keySerializer;
    private final KafkaAvroDeserializer keyDeserializer;
    private final KafkaAvroSerializer valueSerializer;
    private final KafkaAvroDeserializer valueDeserializer;

    public AvroSchemaRegistryStructureProvider() {
        this.valueSerializer = new KafkaAvroSerializer();
        this.valueDeserializer = new KafkaAvroDeserializer();
        this.keySerializer = new KafkaAvroSerializer();
        this.keyDeserializer = new KafkaAvroDeserializer();
    }

    // Visible for test.
    AvroSchemaRegistryStructureProvider(final SchemaRegistryClient schemaRegistryClient) {
        Objects.requireNonNull(schemaRegistryClient, "schemaRegistryClient cannot be null");
        this.keySerializer = new KafkaAvroSerializer(schemaRegistryClient);
        this.keyDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
        this.valueSerializer = new KafkaAvroSerializer(schemaRegistryClient);
        this.valueDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
    }

    @Override
    public void configure(final Map<String, ?> configs) {
        Objects.requireNonNull(configs, "configs cannot be null");
        final var config = new AvroSchemaRegistryStructureProviderConfig(configs);

        final HashMap<String, Object> serdeConfig = new HashMap<>(config.serdeConfig());
        serdeConfig.put("auto.register.schemas", "false");

        keySerializer.configure(serdeConfig, true);
        keyDeserializer.configure(serdeConfig, true);
        valueSerializer.configure(serdeConfig, false);
        valueDeserializer.configure(serdeConfig, false);
    }

    @Override
    public SchemaAndId<Schema> getSchemaById(final Integer schemaId) throws IOException {
        if (schemaId == null) {
            return new SchemaAndId<>(Schema.createUnion(Schema.create(Schema.Type.BYTES),
                    Schema.create(Schema.Type.NULL)), null);
        }
        try {
            return new SchemaAndId<>(Schema.createUnion(Schema.create(Schema.Type.NULL),
                    (Schema) valueDeserializer.getSchemaRegistryClient().getSchemaById(schemaId).rawSchema()),
                    schemaId);
        } catch (final RestClientException e) {
            if (e.getErrorCode() == SCHEMA_NOT_FOUND_ERROR_CODE
                    || e.getErrorCode() == SUBJECT_NOT_FOUND_ERROR_CODE
                    || e.getErrorCode() == JSON_PARSE_ERROR_CODE) {
                return new SchemaAndId<>(Schema.createUnion(Schema.create(Schema.Type.BYTES),
                        Schema.create(Schema.Type.NULL)), null);
            } else {
                throw new RuntimeException("Failed to fetch schema with id " + schemaId + " from schema registry", e);
            }
        }
    }

    @Override
    public ByteBuffer serializeKey(final String topic, final Headers headers, final Object record) {
        return ByteBuffer.wrap(keySerializer.serialize(topic, headers, record));
    }

    @Override
    public Object deserializeKey(final String topic, final Headers headers, final byte[] data) {
        try {
            return keyDeserializer.deserialize(topic, headers, data);
        } catch (final Throwable e) {
            // If deserialization fails, return null. Further the raw bytes will be passed to Iceberg table
            return null;
        }
    }

    @Override
    public ByteBuffer serializeValue(final String topic, final Headers headers, final Object record) {
        return ByteBuffer.wrap(valueSerializer.serialize(topic, headers, record));
    }

    @Override
    public Object deserializeValue(final String topic, final Headers headers, final byte[] data) {
        try {
            return valueDeserializer.deserialize(topic, headers, data);
        } catch (final Throwable e) {
            // If deserialization fails, return null. Further the raw bytes will be passed to Iceberg table
            return null;
        }
    }
}
