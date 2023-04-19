/*
 * Copyright 2023 Aiven Oy
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

package io.aiven.kafka.tieredstorage.commons.transform;

import io.aiven.kafka.tieredstorage.commons.UniversalRemoteStorageManagerConfig;
import io.aiven.kafka.tieredstorage.commons.security.EncryptionKeyProvider;
import java.io.IOException;
import java.nio.file.Files;
import javax.crypto.Cipher;

import java.util.function.Function;
import java.util.function.Supplier;

import io.aiven.kafka.tieredstorage.commons.manifest.index.ChunkIndex;

public class TransformPipeline {
    private final int chunkSize;
    private final Function<InboundTransformChain, InboundTransformChain> inboundChainSupplier;
    private final Function<OutboundTransformChain, OutboundTransformChain> outboundChainSupplier;

    public TransformPipeline(
        int chunkSize, final Function<InboundTransformChain, InboundTransformChain> inboundChainSupplier,
        final Function<OutboundTransformChain, OutboundTransformChain> outboundChainSupplier) {
        this.chunkSize = chunkSize;
        this.inboundChainSupplier = inboundChainSupplier;
        this.outboundChainSupplier = outboundChainSupplier;
    }

    public static TransformPipeline.Builder newBuilder() {
        return new Builder();
    }

    public InboundTransformChain inboundChain(final byte[] original) {
        return inboundChainSupplier.apply(new InboundTransformChain(original, chunkSize));
    }

    public OutboundTransformChain outboundChain(final byte[] uploadedData, final ChunkIndex chunkIndex) {
        return outboundChainSupplier.apply(new OutboundTransformChain(uploadedData, chunkIndex));
    }

    public static class Builder {
        private int chunkSize;
        private boolean withEncryption = false;
        private int ivSize = -1;
        private Supplier<Cipher> inboundCipherSupplier = null;
        private Function<byte[], Cipher> outboundCipherSupplier = null;
        private boolean withCompression = false;

        public Builder withChunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }


        public Builder withEncryption(final int ivSize,
                                      final Supplier<Cipher> inboundCipherSuppler,
                                      final Function<byte[], Cipher> outboundCipherSupplier) {
            this.withEncryption = true;
            this.ivSize = ivSize;
            this.inboundCipherSupplier = inboundCipherSuppler;
            this.outboundCipherSupplier = outboundCipherSupplier;
            return this;
        }

        public Builder withCompression() {
            withCompression = true;
            return this;
        }

        public Builder fromConfig(UniversalRemoteStorageManagerConfig config) throws IOException {
            withChunkSize(config.chunkSize());
            if (config.compressionEnabled()) {
                withCompression();
            }
            if (config.encryptionEnabled()) {
                final EncryptionKeyProvider keyProvider = EncryptionKeyProvider.of(
                    Files.newInputStream(config.encryptionPublicKeyFile()),
                    Files.newInputStream(config.encryptionPrivateKeyFile())
                );
                final int ivSize = keyProvider.getEncryptingCipher().getIV().length;
                withEncryption(
                    ivSize,
                    keyProvider::getEncryptingCipher,
                    bytes -> keyProvider.getDecryptionCipher(bytes, ivSize)
                );
            }
            return this;
        }

        public TransformPipeline build() {
            final Function<InboundTransformChain, InboundTransformChain> inboundFunction = inboundTransformChain -> {
                if (withCompression) {
                    inboundTransformChain.chain(CompressionChunkEnumeration::new);
                }
                if (withEncryption) {
                    inboundTransformChain.chain(inboundTransform -> new EncryptionChunkEnumeration(inboundTransform,
                        inboundCipherSupplier));
                }
                return inboundTransformChain;
            };
            final Function<OutboundTransformChain, OutboundTransformChain> outboundFunction =
                outboundTransformChain -> {
                    if (withEncryption) {
                        outboundTransformChain.chain(
                            outboundTransform -> new DecryptionChunkEnumeration(outboundTransform, ivSize,
                                outboundCipherSupplier));
                    }
                    if (withCompression) {
                        outboundTransformChain.chain(DecompressionChunkEnumeration::new);
                    }
                    return outboundTransformChain;
                };
            return new TransformPipeline(chunkSize, inboundFunction, outboundFunction);
        }
    }
}
