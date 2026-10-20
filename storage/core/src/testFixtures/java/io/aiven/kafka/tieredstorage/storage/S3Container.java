/*
 * Copyright 2026 Aiven Oy
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

package io.aiven.kafka.tieredstorage.storage;

import java.net.URI;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * A throwaway S3-compatible object storage for integration and e2e tests.
 *
 * <p>This class is the only place in the test code that knows which implementation backs it:
 * the image, its credential variables and its S3 port. Tests must go through it and never
 * mention the implementation. To switch implementation, change only this class.
 * (The demos spell out the same details in their compose files.)
 */
public class S3Container extends GenericContainer<S3Container> {
    private static final String IMAGE = "rustfs/rustfs:1.0.3";

    public static final String ACCESS_KEY = "admin";
    public static final String SECRET_KEY = "password";
    public static final String REGION = "us-east-1";
    public static final int S3_PORT = 9000;

    public S3Container() {
        super(DockerImageName.parse(IMAGE));
        withEnv("RUSTFS_ACCESS_KEY", ACCESS_KEY);
        withEnv("RUSTFS_SECRET_KEY", SECRET_KEY);
        withExposedPorts(S3_PORT);
        // The port opens before the store is ready to serve S3 requests. /health is only a liveness probe,
        // /health/ready also waits for storage and IAM.
        waitingFor(Wait.forHttp("/health/ready").forPort(S3_PORT).forStatusCode(200));
    }

    /**
     * The endpoint as seen from the host running the tests.
     */
    public URI endpoint() {
        return URI.create("http://" + getHost() + ":" + getMappedPort(S3_PORT));
    }
}
