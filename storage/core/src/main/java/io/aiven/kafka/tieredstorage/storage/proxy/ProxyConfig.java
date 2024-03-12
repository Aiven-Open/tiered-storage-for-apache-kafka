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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class ProxyConfig extends AbstractConfig {
    public static final String PROXY_PREFIX = "proxy.";

    private static final String PROXY_HOST = "host";
    private static final String PROXY_HOST_DOC = "The SOCKS5 proxy host name or IP address";

    private static final String PROXY_PORT = "port";
    private static final String PROXY_PORT_DOC = "The SOCKS5 proxy port";

    private static final String PROXY_USERNAME = "username";
    private static final String PROXY_USERNAME_DOC = "The username for authentication at the SOCKS5 proxy";

    private static final String PROXY_PASSWORD = "password";
    private static final String PROXY_PASSWORD_DOC = "The password for authentication at the SOCKS5 proxy";

    private static final ConfigDef CONFIG;

    static {
        CONFIG = new ConfigDef();

        CONFIG.define(
            PROXY_HOST,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Importance.LOW,
            PROXY_HOST_DOC
        );

        CONFIG.define(
            PROXY_PORT,
            ConfigDef.Type.INT,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Importance.LOW,
            PROXY_PORT_DOC
        );

        CONFIG.define(
            PROXY_USERNAME,
            ConfigDef.Type.PASSWORD,
            null,
            ConfigDef.Importance.LOW,
            PROXY_USERNAME_DOC
        );

        CONFIG.define(
            PROXY_PASSWORD,
            ConfigDef.Type.PASSWORD,
            null,
            ConfigDef.Importance.LOW,
            PROXY_PASSWORD_DOC
        );
    }

    public ProxyConfig(final Map<String, ?> props) {
        super(CONFIG, props);
        validate();
    }

    private void validate() {
        if (username() == null && password() != null || username() != null && password() == null) {
            throw new ConfigException("Username and password must be provided together");
        }
    }

    public String host() {
        return getString(PROXY_HOST);
    }

    public int port() {
        return getInt(PROXY_PORT);
    }

    public String username() {
        final var secret = getPassword(PROXY_USERNAME);
        return secret != null ? secret.value() : null;
    }

    public String password() {
        final var secret = getPassword(PROXY_PASSWORD);
        return secret != null ? secret.value() : null;
    }
}
