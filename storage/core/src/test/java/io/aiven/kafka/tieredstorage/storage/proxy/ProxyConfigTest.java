/*
 * Copyright 2024 Aiven Oy
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

package io.aiven.kafka.tieredstorage.storage.proxy;

import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProxyConfigTest {
    @Test
    void minimalConfig() {
        final var config = new ProxyConfig(Map.of(
            "host", "localhost",
            "port", "1080"
        ));
        assertThat(config.host()).isEqualTo("localhost");
        assertThat(config.port()).isEqualTo(1080);
        assertThat(config.username()).isNull();
        assertThat(config.password()).isNull();
    }

    @Test
    void fullConfig() {
        final var config = new ProxyConfig(Map.of(
            "host", "localhost",
            "port", "1080",
            "username", "username",
            "password", "password"
        ));
        assertThat(config.host()).isEqualTo("localhost");
        assertThat(config.port()).isEqualTo(1080);
        assertThat(config.username()).isEqualTo("username");
        assertThat(config.password()).isEqualTo("password");
    }

    @Test
    void noHost() {
        assertThatThrownBy(() -> new ProxyConfig(Map.of(
            "port", "1080"
        )))
            .isExactlyInstanceOf(ConfigException.class)
            .hasMessage("Missing required configuration \"host\" which has no default value.");
    }

    @Test
    void noPort() {
        assertThatThrownBy(() -> new ProxyConfig(Map.of(
            "host", "localhost"
        )))
            .isExactlyInstanceOf(ConfigException.class)
            .hasMessage("Missing required configuration \"port\" which has no default value.");
    }

    @Test
    void usernameWithoutPassword() {
        assertThatThrownBy(() -> new ProxyConfig(Map.of(
            "host", "localhost",
            "port", "1080",
            "username", "username"
        )))
            .isExactlyInstanceOf(ConfigException.class)
            .hasMessage("Username and password must be provided together");
    }

    @Test
    void passwordWithoutUsername() {
        assertThatThrownBy(() -> new ProxyConfig(Map.of(
            "host", "localhost",
            "port", "1080",
            "password", "password"
        ))).isExactlyInstanceOf(ConfigException.class)
            .hasMessage("Username and password must be provided together");
    }
}
