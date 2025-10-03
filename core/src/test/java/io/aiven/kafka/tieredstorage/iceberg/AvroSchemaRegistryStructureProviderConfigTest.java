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

import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class AvroSchemaRegistryStructureProviderConfigTest {
    @Test
    void minimalConfig() {
        final String schemaRegistryUrl = "http://127.0.0.1:8080";
        final Map<String, Object> configs = Map.of(
            "serde.schema.registry.url", schemaRegistryUrl,
            "serde.a.b.c", "xyz"
        );
        final var config = new AvroSchemaRegistryStructureProviderConfig(configs);
        assertThat(config.serdeConfig()).isEqualTo(Map.of(
            "schema.registry.url", schemaRegistryUrl,
            "a.b.c", "xyz"
        ));
    }
}
