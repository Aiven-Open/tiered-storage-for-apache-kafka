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

package io.aiven.kafka.tiered.storage.s3;

import java.text.NumberFormat;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentId;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;

public class S3StorageUtils {

    public static String getFileKey(final RemoteLogSegmentMetadata remoteLogSegmentMetadata,
                                    final String suffix) {
        final RemoteLogSegmentId remoteLogSegmentId = remoteLogSegmentMetadata.remoteLogSegmentId();
        return fileNamePrefix(remoteLogSegmentId) + remoteLogSegmentId.id() + "-"
                + filenamePrefixFromOffset(remoteLogSegmentMetadata.startOffset()) + "." + suffix;
    }

    private static String fileNamePrefix(final RemoteLogSegmentId remoteLogSegmentId) {
        final TopicIdPartition topicIdPartition = remoteLogSegmentId.topicIdPartition();
        return topicIdPartition.topicPartition().topic() + "-" + topicIdPartition.topicId() + "/"
                + topicIdPartition.topicPartition().partition() + "/";
    }

    /**
     * Make log segment file name from offset bytes. All this does is pad out the offset number with zeros
     * so that ls sorts the files numerically.
     *
     * <p>Taken from kafka.log.Log.filenamePrefixFromOffset.
     *
     * @param offset The offset to use in the file name
     * @return The filename
     *
     */
    public static String filenamePrefixFromOffset(final long offset) {
        final NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }
}
