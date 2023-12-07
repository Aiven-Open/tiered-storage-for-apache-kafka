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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.Security;
import java.security.spec.EncodedKeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Map;

import io.aiven.kafka.tieredstorage.security.RsaKeyReader;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemWriter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

public abstract class RsaKeyAwareTest {

    public static KeyPair rsaKeyPair;

    public static final String KEY_ENCRYPTION_KEY_ID = "static-key-id";

    public static Path publicKeyPem;

    public static Path privateKeyPem;

    public static Map<String, KeyPair> keyRing;

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    @BeforeAll
    static void generateRsaKeyPair(@TempDir final Path tmpFolder)
        throws NoSuchAlgorithmException, IOException, NoSuchProviderException {
        final var keyPair = KeyPairGenerator.getInstance("RSA", "BC");
        keyPair.initialize(2048, SecureRandom.getInstanceStrong());
        rsaKeyPair = keyPair.generateKeyPair();

        publicKeyPem = tmpFolder.resolve(Paths.get("test_public.pem"));
        privateKeyPem = tmpFolder.resolve(Paths.get("test_private.pem"));

        writePemFile(publicKeyPem, new X509EncodedKeySpec(rsaKeyPair.getPublic().getEncoded()));
        writePemFile(privateKeyPem, new PKCS8EncodedKeySpec(rsaKeyPair.getPrivate().getEncoded()));

        keyRing = Map.of(KEY_ENCRYPTION_KEY_ID, RsaKeyReader.read(publicKeyPem, privateKeyPem));
    }

    protected static void writePemFile(final Path path, final EncodedKeySpec encodedKeySpec) throws IOException {
        try (var pemWriter = new PemWriter(Files.newBufferedWriter(path))) {
            final var pemObject = new PemObject("SOME KEY", encodedKeySpec.getEncoded());
            pemWriter.writeObject(pemObject);
            pemWriter.flush();
        }
    }


}
