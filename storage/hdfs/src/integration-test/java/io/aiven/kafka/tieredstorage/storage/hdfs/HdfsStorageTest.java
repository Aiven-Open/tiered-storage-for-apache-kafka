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

package io.aiven.kafka.tieredstorage.storage.hdfs;

import java.io.IOException;
import java.util.Map;

import io.aiven.kafka.tieredstorage.storage.BaseStorageTest;
import io.aiven.kafka.tieredstorage.storage.StorageBackend;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;
import io.aiven.kafka.tieredstorage.storage.TestObjectKey;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.aiven.kafka.tieredstorage.storage.hdfs.HdfsStorageConfig.HDFS_CONF_PREFIX;
import static io.aiven.kafka.tieredstorage.storage.hdfs.HdfsStorageConfig.HDFS_ROOT_CONFIG;
import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.http.HttpServer2.BIND_ADDRESS;
import static org.assertj.core.api.Assertions.assertThat;

public class HdfsStorageTest extends BaseStorageTest {

    private static final String MS_INIT_MODE_KEY = "hadoop.metrics.init.mode";
    private static final String HDFS_ROOT_DIR = "/tmp/test/";

    private MiniDFSCluster miniDfsCluster;

    @BeforeEach
    public void initCluster() throws IOException {
        System.setProperty(MS_INIT_MODE_KEY, "STANDBY");

        final Configuration configuration = new Configuration();
        configuration.set(BIND_ADDRESS, "127.0.0.1");

        miniDfsCluster = new MiniDFSCluster.Builder(configuration)
            .numDataNodes(3)
            .build();
        miniDfsCluster.waitActive();
        miniDfsCluster.getFileSystem().setWorkingDirectory(new Path(HDFS_ROOT_DIR));
    }

    @AfterEach
    public void shutdownCluster() {
        miniDfsCluster.shutdown(true);
    }

    @Override
    protected StorageBackend storage() {
        final HdfsStorage hdfsStorage = new HdfsStorage();
        final Map<String, Object> storeConfig = Map.of(
            HDFS_ROOT_CONFIG, "/tmp/test/",
            HDFS_CONF_PREFIX + FS_DEFAULT_NAME_KEY, miniDfsCluster.getURI()
        );
        hdfsStorage.configure(storeConfig);
        return hdfsStorage;
    }

    @Test
    void testDeleteAllEmptyParents() throws IOException, StorageBackendException {
        final FileSystem fileSystem = miniDfsCluster.getFileSystem();
        final String parentDir = "dirWithOtherFiles/emptyParentDir/emptyDir/";
        final String segmentPath = parentDir + "kafkaSegment";

        fileSystem.mkdirs(new Path(parentDir));
        fileSystem.createNewFile(new Path(segmentPath));
        fileSystem.createNewFile(new Path("dirWithOtherFiles/otherFile"));

        storage().delete(new TestObjectKey(segmentPath));

        assertThat(fileSystem.exists(
            new Path("dirWithOtherFiles/emptyParentDir/emptyDir/kafkaSegment"))).isFalse();
        assertThat(fileSystem.exists(
            new Path("dirWithOtherFiles/emptyParentDir/emptyDir/"))).isFalse();
        assertThat(fileSystem.exists(
            new Path("dirWithOtherFiles/emptyParentDir/"))).isFalse();
        assertThat(fileSystem.exists(
            new Path("dirWithOtherFiles/otherFile"))).isTrue();
        assertThat(fileSystem.exists(
            new Path("dirWithOtherFiles/"))).isTrue();
    }

    @Test
    void testDeleteAllParentsButRoot() throws IOException, StorageBackendException {
        final FileSystem fileSystem = miniDfsCluster.getFileSystem();
        final String parentDir = "emptyParentDir/emptyDir/";
        final String segmentPath = parentDir + "kafkaSegment";

        fileSystem.mkdirs(new Path(parentDir));
        fileSystem.createNewFile(new Path(segmentPath));

        storage().delete(new TestObjectKey(segmentPath));

        assertThat(fileSystem.exists(
            new Path("emptyParentDir/emptyDir/kafkaSegment"))).isFalse();
        assertThat(fileSystem.exists(
            new Path("emptyParentDir/emptyDir/"))).isFalse();
        assertThat(fileSystem.exists(
            new Path("emptyParentDir/"))).isFalse();
    }

    @Test
    void testDontDeleteNonEmptyDirectory() throws IOException, StorageBackendException {
        final FileSystem fileSystem = miniDfsCluster.getFileSystem();
        final String parentDir = "emptyParentDir/nonEmptyDir/";
        final String segmentPath = parentDir + "kafkaSegment";

        fileSystem.mkdirs(new Path(parentDir));
        fileSystem.createNewFile(new Path(segmentPath));
        fileSystem.createNewFile(new Path(parentDir + "otherFile"));

        storage().delete(new TestObjectKey(segmentPath));

        assertThat(fileSystem.exists(
            new Path("emptyParentDir/nonEmptyDir/kafkaSegment"))).isFalse();
        assertThat(fileSystem.exists(
            new Path("emptyParentDir/nonEmptyDir/otherFile"))).isTrue();
        assertThat(fileSystem.exists(
            new Path("emptyParentDir/nonEmptyDir/"))).isTrue();
    }
}
