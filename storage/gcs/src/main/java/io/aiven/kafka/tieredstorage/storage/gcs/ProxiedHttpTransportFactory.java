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

package io.aiven.kafka.tieredstorage.storage.gcs;

import java.net.InetSocketAddress;
import java.net.Proxy;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.http.HttpTransportFactory;

class ProxiedHttpTransportFactory implements HttpTransportFactory {
    private final InetSocketAddress proxy;

    ProxiedHttpTransportFactory(final String host, final int port) {
        this.proxy = new InetSocketAddress(host, port);
    }

    @Override
    public HttpTransport create() {
        return new NetHttpTransport.Builder()
            .setProxy(new Proxy(Proxy.Type.SOCKS, proxy))
            .build();
    }
}
