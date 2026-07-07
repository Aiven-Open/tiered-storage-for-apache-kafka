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
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.internals.RecordHeaders;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AvroSchemaRegistryStructureProviderTest {

    private static final String TEST_TOPIC = "t1";
    private static final Schema TEST_SCHEMA = Schema.createRecord("s", "", "", false, List.of(
        new Schema.Field("f1", Schema.create(Schema.Type.LONG)),
        new Schema.Field("f2", Schema.create(Schema.Type.STRING))
    ));
    private final MockSchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    private final AvroSchemaRegistryStructureProvider structureProvider =
        new AvroSchemaRegistryStructureProvider(schemaRegistryClient);

    @BeforeEach
    void setUp() {
        structureProvider.configure(Map.of(
            "serde.schema.registry.url", "http://127.0.0.1:99999"
        ));
    }

    @Test
    void serializeAndDeserialize() throws Exception {
        final GenericData.Record record = new GenericData.Record(TEST_SCHEMA);
        record.put("f1", 123L);
        record.put("f2", "hello");

        final RecordHeaders headers = new RecordHeaders();

        // Pre-register schemas since auto-registration is disabled
        schemaRegistryClient.register(TEST_TOPIC + "-key", new io.confluent.kafka.schemaregistry.avro.AvroSchema(
            TEST_SCHEMA));
        schemaRegistryClient.register(TEST_TOPIC + "-value", new io.confluent.kafka.schemaregistry.avro.AvroSchema(
            TEST_SCHEMA));

        final ByteBuffer serializedKey = structureProvider.serializeKey(TEST_TOPIC, headers, record);
        final Object deserializedKey =
            structureProvider.deserializeKey(TEST_TOPIC, new RecordHeaders(), serializedKey.array());
        assertThat(deserializedKey).isEqualTo(record);

        final ByteBuffer serializedValue = structureProvider.serializeValue(TEST_TOPIC, headers, record);
        final Object deserializedValue = structureProvider.deserializeValue(TEST_TOPIC, new RecordHeaders(),
            serializedValue.array());
        assertThat(deserializedValue).isEqualTo(record);
    }

    @Test
    void shouldNotRegisterSchemasWhenSerializing() {
        final GenericData.Record record = new GenericData.Record(TEST_SCHEMA);
        record.put("f1", 123L);
        record.put("f2", "hello");

        final RecordHeaders headers = new RecordHeaders();

        assertThatThrownBy(() -> structureProvider.serializeValue(TEST_TOPIC, headers, record))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("Error retrieving Avro schema");
    }

    @Test
    void shouldFindValidSchemaById() throws RestClientException, IOException {
        final int schemaId = schemaRegistryClient.register(TEST_TOPIC + "-value", new AvroSchema(
                TEST_SCHEMA));

        final StructureProvider.SchemaAndId<Schema> schemaAndId = structureProvider.getSchemaById(schemaId);

        assertThat(schemaAndId.schemaId()).isEqualTo(schemaId);
        assertThat(schemaAndId.schema().getType()).isEqualTo(Schema.Type.UNION);
        assertThat(schemaAndId.schema().getTypes()).containsExactlyInAnyOrder(Schema.create(Schema.Type.NULL),
                TEST_SCHEMA);
    }

    @Test
    void shouldCreateBytesSchemaWhenSchemaNotFound() throws IOException {
        structureProvider.configure(Map.of(
                "serde.schema.registry.url", "http://127.0.0.1:8081"
        ));
        final StructureProvider.SchemaAndId<Schema> schemaAndId = structureProvider.getSchemaById(1);

        assertThat(schemaAndId.schema().getType()).isEqualTo(Schema.Type.UNION);
        assertThat(schemaAndId.schema().getTypes()).containsExactlyInAnyOrder(Schema.create(Schema.Type.NULL),
                Schema.create(Schema.Type.BYTES));
    }
}
