package io.aiven.kafka.tieredstorage.commons.security;

import io.aiven.kafka.tieredstorage.commons.manifest.SegmentEncryptionMetadata;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;

public class AesCipherSupplier {

    public static Cipher encryptionCipher(final EncryptionKeyAndAAD encryptionKeyAndAAD) {
        try {
            final Cipher encryptCipher = getCipher();
            encryptCipher.init(Cipher.ENCRYPT_MODE, encryptionKeyAndAAD.key, SecureRandom.getInstanceStrong());
            encryptCipher.updateAAD(encryptionKeyAndAAD.aad);
            return encryptCipher;
        } catch (final NoSuchAlgorithmException | InvalidKeyException e) {
            throw new RuntimeException(e);
        }
    }

    private static Cipher getCipher() {
        try {
            return Cipher.getInstance("AES/GCM/NoPadding", "BC");
        } catch (final NoSuchAlgorithmException | NoSuchProviderException | NoSuchPaddingException e) {
            throw new RuntimeException(e);
        }
    }

    public static Cipher decryptionCipher(final byte[] encryptedChunk,
                                          final SegmentEncryptionMetadata encryptionMetadata) {
        try {
            final Cipher encryptCipher = getCipher();
            encryptCipher.init(Cipher.DECRYPT_MODE, encryptionMetadata.secretKey(),
                new IvParameterSpec(encryptedChunk, 0, encryptionMetadata.ivSize()),
                SecureRandom.getInstanceStrong());
            encryptCipher.updateAAD(encryptionMetadata.aad());
            return encryptCipher;
        } catch (final NoSuchAlgorithmException | InvalidKeyException | InvalidAlgorithmParameterException e) {
            throw new RuntimeException(e);
        }
    }
}
