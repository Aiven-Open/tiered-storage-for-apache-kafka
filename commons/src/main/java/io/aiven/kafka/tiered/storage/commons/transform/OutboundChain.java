package io.aiven.kafka.tiered.storage.commons.transform;

import io.aiven.kafka.tiered.storage.commons.chunkindex.ChunkIndex;
import java.io.ByteArrayInputStream;
import java.util.function.Function;

public class OutboundChain {
    OutboundTransform inner;

    public OutboundChain(byte[] uploadedData, ChunkIndex chunkIndex) {
        this.inner = new OutboundJoiner(new ByteArrayInputStream(uploadedData), chunkIndex.chunks());
    }

    public void chain(Function<OutboundTransform, OutboundTransform> transform) {
        this.inner = transform.apply(this.inner);
    }
}
