/*
 * Copyright 2025 Aiven Oy
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

package io.aiven.kafka.tieredstorage.iceberg;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.avro.Schema;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.util.Tasks;

/**
 * The structure, definitions and idea of these defines was taken from
 * <a href="https://github.com/apache/iceberg/blob/main/kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/data/IcebergWriterFactory.java">IcebergWriterFactory</a>
 */
public class IcebergTableManager {
    private final Catalog catalog;

    public IcebergTableManager(final Catalog catalog) {
        this.catalog = catalog;
    }

    public Table getOrCreateTable(final TableIdentifier identifier, final Schema schema) {
        createNamespaceIfNotExist(catalog, identifier.namespace());

        final org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(schema);

        final AtomicReference<Table> result = new AtomicReference<>();
        Tasks.range(1)
            .retry(3)
            .run(
                notUsed -> {
                    try {
                        final Table table =
                            catalog.createTable(identifier, icebergSchema, PartitionSpec.unpartitioned());
                        //Initial commit to get snapshotId and sequence number
                        addInitialCommit(table);
                        result.set(table);
                    } catch (final AlreadyExistsException e) {
                        final Table table = catalog.loadTable(identifier);
                        if (table.currentSnapshot() == null) {
                            addInitialCommit(table);
                        }
                        result.set(table);
                    }
                });
        return result.get();
    }

    private static void addInitialCommit(final Table table) {
        final AppendFiles append = table.newAppend();
        append.commit();
        table.refresh();
    }

    static void createNamespaceIfNotExist(final Catalog catalog, final Namespace identifierNamespace) {
        if (!(catalog instanceof SupportsNamespaces)) {
            return;
        }

        final String[] levels = identifierNamespace.levels();
        for (int index = 0; index < levels.length; index++) {
            final Namespace namespace = Namespace.of(Arrays.copyOfRange(levels, 0, index + 1));
            try {
                ((SupportsNamespaces) catalog).createNamespace(namespace);
            } catch (final AlreadyExistsException | ForbiddenException ex) {
                // Ignoring the error as forcefully creating the namespace even if it exists
                // to avoid double namespaceExists() check.
            }
        }
    }
}
