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
import java.net.PasswordAuthentication;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class Socks5ProxyAuthenticatorTest {
    // It's specifically invalid as a host name to check it works with such strings.
    private static final String HOSTNAME = "! 😬 aaa&333 @#";
    private static final String USERNAME = "user";
    private static final String PASSWORD = "password";

    @Test
    void successfulAuth() {
        final var auth = new Socks5ProxyAuthenticator();
        auth.addCredentials(HOSTNAME, 100, USERNAME, PASSWORD);

        final var result = auth.requestPasswordAuthenticationInstance(
            HOSTNAME, null, 100, "SOCKS5", null, null, null, Authenticator.RequestorType.SERVER);
        assertThat(result)
            .extracting(PasswordAuthentication::getUserName)
            .isEqualTo(USERNAME);
        assertThat(result)
            .extracting(PasswordAuthentication::getPassword)
            .isEqualTo(PASSWORD.toCharArray());
    }

    @Test
    void multipleHostsAndPorts() {
        final var auth = new Socks5ProxyAuthenticator();
        auth.addCredentials(HOSTNAME, 100, USERNAME, PASSWORD);
        auth.addCredentials("another host", 100, USERNAME, PASSWORD);
        auth.addCredentials(HOSTNAME, 101, USERNAME, PASSWORD);

        final var result = auth.requestPasswordAuthenticationInstance(
            HOSTNAME, null, 100, "SOCKS5", null, null, null, Authenticator.RequestorType.SERVER);
        assertThat(result)
            .extracting(PasswordAuthentication::getUserName)
            .isEqualTo(USERNAME);
        assertThat(result)
            .extracting(PasswordAuthentication::getPassword)
            .isEqualTo(PASSWORD.toCharArray());
    }

    @Test
    void authWithoutCredentialsRegistered() {
        final var auth = new Socks5ProxyAuthenticator();

        final var result = auth.requestPasswordAuthenticationInstance(
            HOSTNAME, null, 100, "SOCKS5", null, null, null, Authenticator.RequestorType.SERVER);
        assertThat(result).isNull();
    }

    @Test
    void differentHost() {
        final var auth = new Socks5ProxyAuthenticator();
        auth.addCredentials("some other host", 100, USERNAME, PASSWORD);

        final var result = auth.requestPasswordAuthenticationInstance(
            "127.0.0.1", null, 100, "SOCKS5", null, null, null, Authenticator.RequestorType.SERVER);
        assertThat(result).isNull();
    }

    @Test
    void differentPort() {
        final var auth = new Socks5ProxyAuthenticator();
        auth.addCredentials(HOSTNAME, 100, USERNAME, PASSWORD);

        final var result = auth.requestPasswordAuthenticationInstance(
            HOSTNAME, null, 200, "SOCKS5", null, null, null, Authenticator.RequestorType.SERVER);
        assertThat(result).isNull();
    }

    @Test
    void differentProtocol() {
        final var auth = new Socks5ProxyAuthenticator();
        auth.addCredentials(HOSTNAME, 100, USERNAME, PASSWORD);

        final var result = auth.requestPasswordAuthenticationInstance(
            HOSTNAME, null, 100, "HTTP", null, null, null, Authenticator.RequestorType.SERVER);
        assertThat(result).isNull();
    }

    @Test
    void addingShouldNotSupportIncorrectValues() {
        final var auth = new Socks5ProxyAuthenticator();
        assertThatThrownBy(() -> auth.addCredentials(null, 100, USERNAME, PASSWORD))
            .isExactlyInstanceOf(NullPointerException.class)
            .hasMessage("host cannot be null");
        assertThatThrownBy(() -> auth.addCredentials(HOSTNAME, 0, USERNAME, PASSWORD))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("port must be positive");
        assertThatThrownBy(() -> auth.addCredentials(HOSTNAME, 100, null, PASSWORD))
            .isExactlyInstanceOf(NullPointerException.class)
            .hasMessage("username cannot be null");
        assertThatThrownBy(() -> auth.addCredentials(HOSTNAME, 100, USERNAME, null))
            .isExactlyInstanceOf(NullPointerException.class)
            .hasMessage("password cannot be null");
    }

    @Test
    void overridingCredentialsShouldNotBeAllowed() {
        final var auth = new Socks5ProxyAuthenticator();
        auth.addCredentials(HOSTNAME, 100, USERNAME, PASSWORD);
        assertThatThrownBy(() -> auth.addCredentials(HOSTNAME, 100, "other", "other"))
            .isExactlyInstanceOf(RuntimeException.class)
            .hasMessage("Credentials already registered for this host and port");
    }

    @Test
    void addingSameCredentialsMultipleTimesShouldBeAllowed() {
        final var auth = new Socks5ProxyAuthenticator();
        auth.addCredentials(HOSTNAME, 100, USERNAME, PASSWORD);
        auth.addCredentials(HOSTNAME, 100, USERNAME, PASSWORD);

        final var result = auth.requestPasswordAuthenticationInstance(
            HOSTNAME, null, 100, "SOCKS5", null, null, null, Authenticator.RequestorType.SERVER);
        assertThat(result)
            .extracting(PasswordAuthentication::getUserName)
            .isEqualTo(USERNAME);
        assertThat(result)
            .extracting(PasswordAuthentication::getPassword)
            .isEqualTo(PASSWORD.toCharArray());
    }
}
