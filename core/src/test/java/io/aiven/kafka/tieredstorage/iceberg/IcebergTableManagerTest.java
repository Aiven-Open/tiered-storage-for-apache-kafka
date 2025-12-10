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

import org.apache.avro.Schema;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IcebergTableManagerTest {

    @Mock
    private InMemoryCatalog catalog;

    @Mock
    private Table table;

    private IcebergTableManager tableManager;
    private Schema avroSchema;
    private TableIdentifier tableIdentifier;

    @BeforeEach
    void setUp() {
        tableManager = new IcebergTableManager(catalog);

        avroSchema = Schema.createRecord("TestRecord", "Test record", "test.namespace", false);
        avroSchema.setFields(java.util.List.of(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), "Record ID", null),
            new Schema.Field("name", Schema.create(Schema.Type.STRING), "Record name", null)
        ));

        tableIdentifier = TableIdentifier.of("test_namespace", "test_table");
    }

    @Test
    void createNewTable() {
        when(catalog.createTable(eq(tableIdentifier), any(org.apache.iceberg.Schema.class),
            any(PartitionSpec.class)))
            .thenReturn(table);

        final AppendFiles tableAppend = mock(AppendFiles.class);
        when(table.newAppend()).thenReturn(tableAppend);

        final Table result = tableManager.getOrCreateTable(tableIdentifier, avroSchema);

        assertThat(result).isEqualTo(table);
        verify(catalog).createTable(eq(tableIdentifier), any(org.apache.iceberg.Schema.class),
            any(PartitionSpec.class));
        verify(catalog, never()).loadTable(any());
    }

    @Test
    void loadExistingTable() {
        when(catalog.createTable(eq(tableIdentifier), any(org.apache.iceberg.Schema.class),
            any(PartitionSpec.class)))
            .thenThrow(new AlreadyExistsException("Table already exists"));
        when(catalog.loadTable(tableIdentifier)).thenReturn(table);

        final AppendFiles tableAppend = mock(AppendFiles.class);
        when(table.newAppend()).thenReturn(tableAppend);

        final Table result = tableManager.getOrCreateTable(tableIdentifier, avroSchema);

        assertThat(result).isEqualTo(table);
        verify(catalog).createTable(eq(tableIdentifier), any(org.apache.iceberg.Schema.class),
            any(PartitionSpec.class));
        verify(catalog).loadTable(tableIdentifier);
    }

    @Test
    void createAfterRetry() {
        when(catalog.createTable(eq(tableIdentifier), any(org.apache.iceberg.Schema.class),
            any(PartitionSpec.class)))
            .thenThrow(new RuntimeException("Temporary failure"))
            .thenReturn(table);

        final AppendFiles tableAppend = mock(AppendFiles.class);
        when(table.newAppend()).thenReturn(tableAppend);

        final Table result = tableManager.getOrCreateTable(tableIdentifier, avroSchema);

        assertThat(result).isEqualTo(table);
        verify(catalog, times(2)).createTable(eq(tableIdentifier), any(org.apache.iceberg.Schema.class),
            any(PartitionSpec.class));
    }

    @Test
    void failWhenExceedsRetries() {
        when(catalog.createTable(eq(tableIdentifier), any(org.apache.iceberg.Schema.class),
            any(PartitionSpec.class)))
            .thenThrow(new RuntimeException("Permanent failure"));

        assertThatThrownBy(() -> tableManager.getOrCreateTable(tableIdentifier, avroSchema))
            .isInstanceOf(RuntimeException.class)
            .hasMessage("Permanent failure");

        verify(catalog, times(4)).createTable(eq(tableIdentifier), any(org.apache.iceberg.Schema.class),
            any(PartitionSpec.class));
    }

    @Test
    void createNamespace() {
        final Namespace namespace = Namespace.of("test");

        IcebergTableManager.createNamespaceIfNotExist(catalog, namespace);

        verify((SupportsNamespaces) catalog).createNamespace(namespace);
    }

    @Test
    void ignoresNamespaceAlreadyExistsException() {
        final Namespace namespace = Namespace.of("existing");
        doThrow(new AlreadyExistsException("Namespace already exists"))
            .when((SupportsNamespaces) catalog).createNamespace(namespace);

        IcebergTableManager.createNamespaceIfNotExist(catalog, namespace);

        verify((SupportsNamespaces) catalog).createNamespace(namespace);
    }

    @Test
    void ignoresForbiddenException() {
        final Namespace namespace = Namespace.of("forbidden");
        doThrow(new ForbiddenException("Access denied"))
            .when((SupportsNamespaces) catalog).createNamespace(namespace);

        IcebergTableManager.createNamespaceIfNotExist(catalog, namespace);

        verify((SupportsNamespaces) catalog).createNamespace(namespace);
    }

    @Test
    void createTableWithCustomPartitionSpec() {
        // Create a table manager with custom partition spec
        final IcebergTableManager customTableManager = new IcebergTableManager(
            catalog, java.util.List.of("id"));

        // Add timestamp field to schema for more complex partitioning test
        final Schema schemaWithTimestamp = Schema.createRecord("TestRecord", "Test record", "test.namespace", false);
        schemaWithTimestamp.setFields(java.util.List.of(
            new Schema.Field("id", Schema.create(Schema.Type.LONG), "Record ID", null),
            new Schema.Field("name", Schema.create(Schema.Type.STRING), "Record name", null),
            new Schema.Field("timestamp", Schema.create(Schema.Type.LONG), "Timestamp", null)
        ));

        when(catalog.createTable(eq(tableIdentifier), any(org.apache.iceberg.Schema.class),
            any(PartitionSpec.class)))
            .thenReturn(table);

        final AppendFiles tableAppend = mock(AppendFiles.class);
        when(table.newAppend()).thenReturn(tableAppend);

        final Table result = customTableManager.getOrCreateTable(tableIdentifier, schemaWithTimestamp);

        assertThat(result).isEqualTo(table);
        verify(catalog).createTable(eq(tableIdentifier), any(org.apache.iceberg.Schema.class),
            any(PartitionSpec.class));
    }
}
