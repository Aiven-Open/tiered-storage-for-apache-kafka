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

import java.net.Authenticator;
import java.net.InetSocketAddress;
import java.net.PasswordAuthentication;
import java.util.HashMap;
import java.util.Objects;

/**
 * The static authenticator for SOCKS5 proxies.
 *
 * <p>Due how this is implemented in Java, the authenticator is static. This is far from ideal,
 * but lacking a better option, we can live with this: Kafka installations shouldn't have other authenticators.
 */
public class Socks5ProxyAuthenticator extends Authenticator {
    /**
     * Register the credentials with the authenticator.
     *
     * <p>If this authenticator is not installed as the default in the runtime, it will be.
     */
    public static synchronized void register(
        final String host, final int port, final String username, final String password
    ) {
        final Authenticator currentDefault = Authenticator.getDefault();
        if (currentDefault != null) {
            if (!(currentDefault instanceof Socks5ProxyAuthenticator)) {
                // This is not really expected to happen in Kafka installations.
                throw new RuntimeException("Another Authenticator already registered");
            }
            ((Socks5ProxyAuthenticator) currentDefault).addCredentials(host, port, username, password);
        } else {
            final Socks5ProxyAuthenticator authenticator = new Socks5ProxyAuthenticator();
            Authenticator.setDefault(authenticator);
            authenticator.addCredentials(host, port, username, password);
        }
    }

    private final HashMap<InetSocketAddress, PasswordAuthentication> credentials = new HashMap<>();

    void addCredentials(final String host, final int port, final String username, final String password) {
        Objects.requireNonNull(host, "host cannot be null");
        if (port <= 0) {
            throw new IllegalArgumentException("port must be positive");
        }
        Objects.requireNonNull(username, "username cannot be null");
        Objects.requireNonNull(password, "password cannot be null");

        // InetSocketAddress is just a tuple holder in this case, so we avoid hostname resolution.
        final InetSocketAddress hostAndPort = InetSocketAddress.createUnresolved(host, port);
        if (credentials.containsKey(hostAndPort)) {
            throw new RuntimeException("Credentials already registered for this host ans port");
        } else {
            credentials.put(hostAndPort, new PasswordAuthentication(username, password.toCharArray()));
        }
    }

    @Override
    protected PasswordAuthentication getPasswordAuthentication() {
        if (!"SOCKS5".equals(this.getRequestingProtocol())) {
            return null;
        }

        // InetSocketAddress is just a tuple holder in this case, so we avoid hostname resolution.
        final InetSocketAddress hostAndPort =
            InetSocketAddress.createUnresolved(this.getRequestingHost(), this.getRequestingPort());
        return credentials.get(hostAndPort);
    }
}
