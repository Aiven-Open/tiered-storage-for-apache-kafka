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

package io.aiven.kafka.tieredstorage.e2e;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;

public class HdfsSingleBrokerTest extends SingleBrokerTest {
    private static final int NAME_NODE_PORT = 8020;
    private static final int NAME_NODE_UI_PORT = 9870;
    private static final int DATA_NODE_PORT = 9867;

    private static final String NAME_NODE_NETWORK_ALIAS = "namenode";
    private static final String DATA_NODE_NETWORK_ALIAS = "datanode";
    private static final String HDFS_WORKING_DIRECTORY = "/tmp/kafka/";
    private static final String DEFAULT_LOCALHOST_FS = "hdfs://localhost";

    private static final Map<String, String> HADOOP_CONTAINER_ENV = Map.of(
            "CORE-SITE.XML_fs.default.name", "hdfs://namenode",
            "CORE-SITE.XML_fs.defaultFS", "hdfs://namenode",
            "HDFS-SITE.XML_dfs.namenode.rpc-address", "namenode:8020",
            "HDFS-SITE.XML_dfs.replication", "1"
    );

    static final GenericContainer<?> HDFS_NAMENODE_CONTAINER =
        new GenericContainer<>(DockerImageName.parse("apache/hadoop:3"))
            .withExposedPorts(NAME_NODE_UI_PORT, NAME_NODE_PORT)
            .withCommand("hdfs namenode")
            .withNetwork(NETWORK)
            .waitingFor(Wait.forListeningPorts(NAME_NODE_PORT))
            .withStartupTimeout(Duration.of(3, ChronoUnit.MINUTES))
            .withNetworkAliases(NAME_NODE_NETWORK_ALIAS)
            .withEnv(HADOOP_CONTAINER_ENV)
            .withEnv("ENSURE_NAMENODE_DIR", "/tmp/hadoop-root/dfs/name");

    static final GenericContainer<?> HDFS_DATANODE_CONTAINER =
        new GenericContainer<>(DockerImageName.parse("apache/hadoop:3"))
            .withExposedPorts(DATA_NODE_PORT)
            .withCommand("hdfs datanode")
            .withNetwork(NETWORK)
            .withNetworkAliases(DATA_NODE_NETWORK_ALIAS)
            .waitingFor(Wait.forListeningPorts(DATA_NODE_PORT))
            .withStartupTimeout(Duration.of(3, ChronoUnit.MINUTES))
            .withEnv(HADOOP_CONTAINER_ENV);

    private static FileSystem fileSystem;

    @BeforeAll
    static void init() throws Exception {
        HDFS_NAMENODE_CONTAINER.start();
        HDFS_DATANODE_CONTAINER.start();

        setupKafka(HdfsSingleBrokerTest::setupKafka);

        final Configuration hadoopConf = new Configuration();
        final int nameNodePort = HDFS_NAMENODE_CONTAINER.getMappedPort(NAME_NODE_PORT);
        hadoopConf.set(FS_DEFAULT_NAME_KEY, DEFAULT_LOCALHOST_FS + ":" + nameNodePort);
        fileSystem = FileSystem.get(hadoopConf);

        UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("hadoop"));
        fileSystem.setWorkingDirectory(new Path(HDFS_WORKING_DIRECTORY));
    }

    private static void setupKafka(final KafkaContainer kafkaContainer) {
        kafkaContainer.withEnv("KAFKA_RSM_CONFIG_STORAGE_BACKEND_CLASS",
                "io.aiven.kafka.tieredstorage.storage.hdfs.HdfsStorage")
            .withEnv("KAFKA_REMOTE_LOG_STORAGE_MANAGER_CLASS_PATH",
                "/tiered-storage-for-apache-kafka/core/*:/tiered-storage-for-apache-kafka/hdfs/*")
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_HDFS_ROOT", HDFS_WORKING_DIRECTORY)
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_HDFS_CORE-SITE_PATH", "/etc/core-site.xml")
            .withEnv("KAFKA_RSM_CONFIG_STORAGE_HDFS_HDFS-SITE_PATH", "/etc/hdfs-site.xml")
            .withEnv("HADOOP_USER_NAME", "hadoop")
            .withCopyFileToContainer(MountableFile.forClasspathResource("core-site.xml"),
                "/etc/core-site.xml")
            .withCopyFileToContainer(MountableFile.forClasspathResource("hdfs-site.xml"),
                "/etc/hdfs-site.xml")
            .dependsOn(HDFS_NAMENODE_CONTAINER, HDFS_DATANODE_CONTAINER);
    }

    @AfterAll
    static void cleanup() throws IOException {
        stopKafka();

        HDFS_DATANODE_CONTAINER.stop();
        HDFS_NAMENODE_CONTAINER.stop();
        fileSystem.close();

        cleanupStorage();
    }

    @Override
    boolean assertNoTopicDataOnTierStorage(final String topicName, final Uuid topicId) {
        final String topicDir = String.format("%s-%s", topicName, topicId.toString());
        try {
            return !fileSystem.exists(new Path(topicDir));
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    List<String> remotePartitionFiles(final TopicIdPartition topicIdPartition) {
        final String partitionDirRelativePath = String.format("%s-%s/%s",
            topicIdPartition.topic(),
            topicIdPartition.topicId().toString(),
            topicIdPartition.partition());
        try {
            return listFiles(partitionDirRelativePath)
                .map(FileStatus::getPath)
                .map(Path::getName)
                .sorted()
                .collect(Collectors.toList());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Stream<FileStatus> listFiles(final String directory) throws IOException {
        final Path directoryPath = new Path(directory);
        return Arrays.stream(fileSystem.listStatus(directoryPath));
    }
}
