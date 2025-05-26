/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Copyright 2025 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */

package io.aiven.kafka.tieredstorage.iceberg.data;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.PropertyUtil;

/**
 * The structure, definitions and idea of these defines was taken from
 * <a href="https://github.com/apache/iceberg/blob/main/kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/data/RecordUtils.java">RecordUtils</a>
 */
class TableWriterFactory {

    public static TaskWriter<Record> createTableWriter(final Table table) {
        final Map<String, String> tableProps = Maps.newHashMap(table.properties());
        tableProps.putAll(Map.of());

        final String formatStr =
            tableProps.getOrDefault(
                TableProperties.DEFAULT_FILE_FORMAT, TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
        final FileFormat format = FileFormat.fromString(formatStr);

        final long targetFileSize =
            PropertyUtil.propertyAsLong(
                tableProps,
                TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
                TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

        final Set<Integer> identifierFieldIds = table.schema().identifierFieldIds();

        final FileAppenderFactory<Record> appenderFactory;
        if (identifierFieldIds == null || identifierFieldIds.isEmpty()) {
            appenderFactory =
                new GenericAppenderFactory(table.schema(), table.spec(), null, null, null)
                    .setAll(tableProps);
        } else {
            appenderFactory =
                new GenericAppenderFactory(
                    table.schema(),
                    table.spec(),
                    Ints.toArray(identifierFieldIds),
                    TypeUtil.select(table.schema(), Sets.newHashSet(identifierFieldIds)),
                    null)
                    .setAll(tableProps);
        }

        // (partition ID + task ID + operation ID) must be unique
        final OutputFileFactory fileFactory =
            OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
                .defaultSpec(table.spec())
                .operationId(UUID.randomUUID().toString())
                .format(format)
                .build();

        return new UnpartitionedWriter<>(
            table.spec(), format, appenderFactory, fileFactory, table.io(), targetFileSize);
    }

    private TableWriterFactory() {
    }
}
