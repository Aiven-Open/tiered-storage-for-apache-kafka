package io.aiven.kafka.tiered.storage.commons.transform;

import java.io.ByteArrayInputStream;
import java.util.function.Function;

public class InboundChain {
    int originalSize;
    ChunkInboundTransform inner;


    public InboundChain(byte[] original, int chunkSize) {
        originalSize = original.length;
        this.inner = new ChunkInboundTransformSlicer(new ByteArrayInputStream(original), chunkSize);
    }

    public void chain(Function<ChunkInboundTransform, ChunkInboundTransform> transformSupplier) {
        this.inner = transformSupplier.apply(this.inner);
    }

    public InboundResult complete() {
        return new InboundResult(inner, originalSize);
    }
}
