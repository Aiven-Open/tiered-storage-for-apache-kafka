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

package io.aiven.kafka.tieredstorage.manifest.serde;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public class KafkaTypeSerdeModule {
    public static Module create() {
        final var module = new SimpleModule();

        module.addSerializer(Uuid.class, new UuidSerializer());

        module.setMixInAnnotation(TopicPartition.class, TopicPartitionSerdeMixin.class);
        module.setMixInAnnotation(TopicIdPartition.class, TopicIdPartitionSerdeMixin.class);
        module.setMixInAnnotation(RemoteLogSegmentId.class, RemoteLogSegmentIdMixin.class);
        module.setMixInAnnotation(RemoteLogSegmentMetadata.class, RemoteLogSegmentMetadataMixin.class);

        return module;
    }

    private static class UuidSerializer extends StdSerializer<Uuid> {
        UuidSerializer() {
            super(Uuid.class);
        }

        @Override
        public void serialize(final Uuid value,
                              final JsonGenerator gen,
                              final SerializerProvider provider) throws IOException {
            gen.writeString(value.toString());
        }
    }

    @JsonPropertyOrder({"topic", "partition"})
    private abstract static class TopicPartitionSerdeMixin {
        @JsonProperty("topic")
        public abstract String topic();

        @JsonProperty("partition")
        public abstract int partition();
    }

    @JsonPropertyOrder({"topicId", "topicPartition"})
    private abstract static class TopicIdPartitionSerdeMixin {
        @JsonProperty("topicId")
        public abstract Uuid topicId();

        @JsonProperty("topicPartition")
        public abstract TopicPartition topicPartition();
    }

    @JsonPropertyOrder({"topicIdPartition", "id"})
    private abstract static class RemoteLogSegmentIdMixin {
        @JsonProperty("topicIdPartition")
        abstract TopicIdPartition topicIdPartition();

        @JsonProperty("id")
        abstract Uuid id();
    }

    @JsonPropertyOrder({
        "remoteLogSegmentId", "startOffset", "endOffset",
        "maxTimestampMs", "brokerId", "eventTimestampMs", "segmentLeaderEpochs"
    })
    private abstract static class RemoteLogSegmentMetadataMixin {
        @JsonProperty("remoteLogSegmentId")
        public abstract RemoteLogSegmentId remoteLogSegmentId();

        @JsonProperty("startOffset")
        public abstract long startOffset();

        @JsonProperty("endOffset")
        public abstract long endOffset();

        @JsonProperty("maxTimestampMs")
        public abstract long maxTimestampMs();

        @JsonProperty("brokerId")
        public abstract int brokerId();

        @JsonProperty("eventTimestampMs")
        public abstract long eventTimestampMs();

        @JsonProperty("segmentLeaderEpochs")
        public abstract Map<Integer, Long> segmentLeaderEpochs();
    }
}
