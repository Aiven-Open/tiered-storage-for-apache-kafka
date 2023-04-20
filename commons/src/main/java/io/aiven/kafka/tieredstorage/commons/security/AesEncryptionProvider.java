package io.aiven.kafka.tieredstorage.commons.security;

import static io.aiven.kafka.tieredstorage.commons.security.RsaEncryptionProvider.KEY_SIZE;

import io.aiven.kafka.tieredstorage.commons.manifest.SegmentEncryptionMetadata;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class AesEncryptionProvider implements Encryption, Decryption {

    public static final int KEY_AND_AAD_SIZE_BYTES = KEY_SIZE / 8 / 2;
    public static final String AES_TRANSFORMATION = "AES/GCM/NoPadding";

    private final KeyGenerator aesKeyGenerator;

    public AesEncryptionProvider(KeyGenerator aesKeyGenerator) {
        this.aesKeyGenerator = aesKeyGenerator;
    }

    public SecretKey createKey() {
        return aesKeyGenerator.generateKey();
    }

    public SecretKeyAndAAD createKeyAndAAD() {
        final byte[] keyAndAAD = createKey().getEncoded();
        final byte[] encryptionKey = new byte[KEY_AND_AAD_SIZE_BYTES];
        System.arraycopy(keyAndAAD, 0, encryptionKey, 0, 32);
        final byte[] aad = new byte[KEY_AND_AAD_SIZE_BYTES];
        System.arraycopy(keyAndAAD, 32, aad, 0, 32);
        return new SecretKeyAndAAD(new SecretKeySpec(encryptionKey, "AES"), aad);
    }

    public Cipher encryptionCipher(final SecretKeyAndAAD encryptionKeyAndAAD) {
        final Cipher encryptCipher = createEncryptingCipher(encryptionKeyAndAAD.key, AES_TRANSFORMATION);
        encryptCipher.updateAAD(encryptionKeyAndAAD.aad);
        return encryptCipher;
    }

    public Cipher decryptionCipher(final byte[] encryptedChunk,
                                   final SegmentEncryptionMetadata encryptionMetadata) {
        final Cipher encryptCipher = createDecryptingCipher(encryptionMetadata.secretKey(),
            new IvParameterSpec(encryptedChunk, 0, encryptionMetadata.ivSize()),
            AES_TRANSFORMATION);
        encryptCipher.updateAAD(encryptionMetadata.aad());
        return encryptCipher;
    }
}
