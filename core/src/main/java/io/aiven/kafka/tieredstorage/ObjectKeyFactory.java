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

package io.aiven.kafka.tieredstorage;

import java.text.NumberFormat;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;

import io.aiven.kafka.tieredstorage.storage.ObjectKey;

import static io.aiven.kafka.tieredstorage.metadata.SegmentCustomMetadataField.OBJECT_KEY;
import static io.aiven.kafka.tieredstorage.metadata.SegmentCustomMetadataField.OBJECT_PREFIX;

/**
 * Maps Kafka segment files to object paths/keys in the storage backend.
 */
public final class ObjectKeyFactory {

    /**
     * Supported files and extensions, including log, index types, and segment manifest.
     *
     * @see org.apache.kafka.server.log.remote.storage.RemoteStorageManager.IndexType
     */
    public enum Suffix {
        LOG("log"),
        OFFSET_INDEX("index"),
        TIME_INDEX("timeindex"),
        PRODUCER_SNAPSHOT("snapshot"),
        TXN_INDEX("txnindex"),
        LEADER_EPOCH_CHECKPOINT("leader-epoch-checkpoint"),
        MANIFEST("rsm-manifest");

        public final String value;

        Suffix(final String value) {
            this.value = value;
        }

        static Suffix fromIndexType(final RemoteStorageManager.IndexType indexType) {
            switch (indexType) {
                case OFFSET: return OFFSET_INDEX;
                case TIMESTAMP: return TIME_INDEX;
                case PRODUCER_SNAPSHOT: return PRODUCER_SNAPSHOT;
                case TRANSACTION: return TXN_INDEX;
                case LEADER_EPOCH: return LEADER_EPOCH_CHECKPOINT;
                default:
                    throw new IllegalArgumentException("Unknown index type " + indexType);
            }
        }
    }

    private final String prefix;

    /**
     * @param prefix the prefix to add to all created keys.
     */
    public ObjectKeyFactory(final String prefix) {
        this.prefix = prefix == null ? "" : prefix;
    }

    /**
     * Creates the object key/path in the following format:
     *
     * <pre>
     * $(prefix)$(main_path).$(suffix)
     * </pre>
     *
     * <p>For example:
     * {@code someprefix/topic-MWJ6FHTfRYy67jzwZdeqSQ/7/00000000000000001234-tqimKeZwStOEOwRzT3L5oQ.log}
     *
     * @see ObjectKeyFactory#mainPath(RemoteLogSegmentMetadata)
     */
    public ObjectKey key(final RemoteLogSegmentMetadata remoteLogSegmentMetadata, final Suffix suffix) {
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata cannot be null");
        Objects.requireNonNull(suffix, "suffix cannot be null");

        return new PlainObjectKey(prefix, mainPath(remoteLogSegmentMetadata) + "." + suffix.value);
    }

    /**
     * Creates the object key/path prioritizing fields in custom metadata with the following format:
     *
     * <pre>
     * $(prefix)$(main_path).$(suffix)
     * </pre>
     *
     * <p>For example:
     * {@code someprefix/topic-MWJ6FHTfRYy67jzwZdeqSQ/7/00000000000000001234-tqimKeZwStOEOwRzT3L5oQ.log}
     */
    public ObjectKey key(final Map<Integer, Object> fields,
                         final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                         final Suffix suffix) {
        Objects.requireNonNull(fields, "fields cannot be null");
        Objects.requireNonNull(remoteLogSegmentMetadata, "remoteLogSegmentMetadata cannot be null");
        Objects.requireNonNull(suffix, "suffix cannot be null");

        final var prefix = (String) fields.getOrDefault(OBJECT_PREFIX.index(), this.prefix);
        final var main = (String) fields.getOrDefault(OBJECT_KEY.index(), mainPath(remoteLogSegmentMetadata));
        return new PlainObjectKey(prefix, main + "." + suffix.value);
    }

    /**
     * Prepares the main part of the key path containing remote log segment metadata, following this format:
     *
     * <pre>
     * $(topic_name)-$(topic_uuid)/$(partition)/$(000+start_offset length=20)-$(segment_uuid)
     * </pre>
     */
    public static String mainPath(final RemoteLogSegmentMetadata remoteLogSegmentMetadata) {
        final RemoteLogSegmentId remoteLogSegmentId = remoteLogSegmentMetadata.remoteLogSegmentId();
        final TopicIdPartition topicIdPartition = remoteLogSegmentId.topicIdPartition();

        return topicIdPartition.topicPartition().topic() + "-" + topicIdPartition.topicId()
            + "/" + topicIdPartition.topicPartition().partition()
            + "/" + filenamePrefixFromOffset(remoteLogSegmentMetadata.startOffset()) + "-" + remoteLogSegmentId.id();
    }

    public String prefix() {
        return prefix;
    }

    /**
     * Makes log segment file name from offset bytes. All this does is pad out the offset number with zeros
     * so that ls sorts the files numerically.
     *
     * @param offset The offset to use in the file name
     * @return The filename
     * @implNote Taken from {@literal kafka.log.Log.filenamePrefixFromOffset}.
     */
    private static String filenamePrefixFromOffset(final long offset) {
        final NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }

    /**
     * The object key that consists of a prefix and main part + suffix.
     *
     * <p>Its string representation is identical to its value.
     */
    static class PlainObjectKey implements ObjectKey {
        private final String prefix;
        private final String mainPathAndSuffix;

        PlainObjectKey(final String prefix, final String mainPathAndSuffix) {
            this.prefix = Objects.requireNonNull(prefix, "prefix cannot be null");
            this.mainPathAndSuffix = Objects.requireNonNull(mainPathAndSuffix, "mainPathAndSuffix cannot be null");
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final PlainObjectKey that = (PlainObjectKey) o;
            return Objects.equals(prefix, that.prefix)
                && Objects.equals(mainPathAndSuffix, that.mainPathAndSuffix);
        }

        @Override
        public int hashCode() {
            int result = prefix.hashCode();
            result = 31 * result + mainPathAndSuffix.hashCode();
            return result;
        }

        @Override
        public String value() {
            return prefix + mainPathAndSuffix;
        }

        @Override
        public String toString() {
            return value();
        }
    }
}
