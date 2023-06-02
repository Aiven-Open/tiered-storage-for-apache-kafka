/*
 * Copyright 2021 Aiven Oy
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

package io.aiven.kafka.tieredstorage;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.Security;
import java.security.spec.EncodedKeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;
import org.junit.jupiter.api.BeforeAll;

public abstract class RsaKeyAwareTest {

    public static KeyPair rsaKeyPair;

    public static String publicKeyPem;

    public static String privateKeyPem;

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    @BeforeAll
    static void generateRsaKeyPair()
            throws NoSuchAlgorithmException, IOException, NoSuchProviderException {
        final var keyPair = KeyPairGenerator.getInstance("RSA", "BC");
        keyPair.initialize(2048, SecureRandom.getInstanceStrong());
        rsaKeyPair = keyPair.generateKeyPair();

        publicKeyPem = encodeAsPem(new X509EncodedKeySpec(rsaKeyPair.getPublic().getEncoded()));
        privateKeyPem = encodeAsPem(new PKCS8EncodedKeySpec(rsaKeyPair.getPrivate().getEncoded()));
    }

    protected static String encodeAsPem(final EncodedKeySpec encodedKeySpec) throws IOException {
        final StringWriter result = new StringWriter();
        try (var pemWriter = new PemWriter(new BufferedWriter(result))) {
            final var pemObject = new PemObject("SOME KEY", encodedKeySpec.getEncoded());
            pemWriter.writeObject(pemObject);
            pemWriter.flush();
        }
        return result.toString();
    }
}
