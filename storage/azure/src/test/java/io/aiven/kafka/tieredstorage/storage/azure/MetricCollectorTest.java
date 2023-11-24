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

package io.aiven.kafka.tieredstorage.storage.azure;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class MetricCollectorTest {

    @Test
    void pathInDevWithAccountName() {
        final var props = Map.of("azure.account.name", "test-account",
            "azure.container.name", "cont1");
        final var metrics = new MetricCollector(new AzureBlobStorageConfig(props));
        final var matcher = metrics.pathPattern().matcher("/test-account/cont1/test-object");
        assertThat(matcher).matches();
    }

    @Test
    void pathInProdWithoutAccountName() {
        final var props = Map.of("azure.account.name", "test-account",
            "azure.container.name", "cont1");
        final var metrics = new MetricCollector(new AzureBlobStorageConfig(props));
        final var matcher = metrics.pathPattern().matcher("/cont1/test-object");
        assertThat(matcher).matches();
    }

    @ParameterizedTest
    @ValueSource(strings = {"comp=test", "comp=test&post=val", "pre=val&comp=test", "pre=val&comp=test&post=val"})
    void uploadQueryWithComp(final String query) {
        final var matcher = MetricCollector.MetricsPolicy.UPLOAD_QUERY_PATTERN.matcher(query);
        assertThat(matcher.find()).isTrue();
        assertThat(matcher.group("comp")).isEqualTo("test");
    }
}
