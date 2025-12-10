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


import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.base.Splitter;
import org.apache.avro.Schema;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.DateType;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimeType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.aiven.kafka.tieredstorage.iceberg.data.SchemaUpdate.AddColumn;
import static io.aiven.kafka.tieredstorage.iceberg.data.SchemaUpdate.Consumer;
import static io.aiven.kafka.tieredstorage.iceberg.data.SchemaUpdate.MakeOptional;
import static io.aiven.kafka.tieredstorage.iceberg.data.SchemaUpdate.UpdateType;

/**
 * The structure, definitions and idea of these defines was taken from
 * <a href="https://github.com/apache/iceberg/blob/main/kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/data/SchemaUtils.java">SchemaUtils</a>
 */
public class SchemaUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaUtils.class);

    private static final Pattern TRANSFORM_REGEX = Pattern.compile("(\\w+)\\((.+)\\)");

    static PrimitiveType needsDataTypeUpdate(final Type currentIcebergType, final Schema valueSchema) {
        if (currentIcebergType.typeId() == TypeID.FLOAT && valueSchema.getType() == Schema.Type.DOUBLE) {
            return DoubleType.get();
        }
        if (currentIcebergType.typeId() == TypeID.INTEGER && valueSchema.getType() == Schema.Type.LONG) {
            return LongType.get();
        }
        return null;
    }

    static void applySchemaUpdates(final Table table, final Consumer updates) {
        if (updates == null || updates.empty()) {
            // no updates to apply
            return;
        }

        Tasks.range(1)
            .retry(5)
            .run(notUsed -> commitSchemaUpdates(table, updates));
    }

    private static void commitSchemaUpdates(final Table table, final Consumer updates) {
        // get the latest schema in case another process updated it
        table.refresh();

        // filter out columns that have already been added
        final List<AddColumn> addColumns =
            updates.addColumns().stream()
                .filter(addCol -> !columnExists(table.schema(), addCol))
                .collect(Collectors.toList());

        // filter out columns that have the updated type
        final List<UpdateType> updateTypes =
            updates.updateTypes().stream()
                .filter(updateType -> !typeMatches(table.schema(), updateType))
                .collect(Collectors.toList());

        // filter out columns that have already been made optional
        final List<MakeOptional> makeOptionals =
            updates.makeOptionals().stream()
                .filter(makeOptional -> !isOptional(table.schema(), makeOptional))
                .collect(Collectors.toList());

        if (addColumns.isEmpty() && updateTypes.isEmpty() && makeOptionals.isEmpty()) {
            // no updates to apply
            LOG.info("Schema for table {} already up-to-date", table.name());
            return;
        }

        // apply the updates
        final UpdateSchema updateSchema = table.updateSchema();
        addColumns.forEach(
            update -> updateSchema.addColumn(update.parentName(), update.name(), update.type()));
        updateTypes.forEach(update -> updateSchema.updateColumn(update.name(), update.type()));
        makeOptionals.forEach(update -> updateSchema.makeColumnOptional(update.name()));
        updateSchema.commit();
        LOG.info("Schema for table {} updated with new columns", table.name());
    }

    private static boolean columnExists(final org.apache.iceberg.Schema schema, final AddColumn update) {
        return schema.findType(update.key()) != null;
    }

    private static boolean typeMatches(final org.apache.iceberg.Schema schema, final UpdateType update) {
        final Type type = schema.findType(update.name());
        if (type == null) {
            throw new IllegalArgumentException("Invalid column: " + update.name());
        }
        return type.typeId() == update.type().typeId();
    }

    private static boolean isOptional(final org.apache.iceberg.Schema schema, final MakeOptional update) {
        final NestedField field = schema.findField(update.name());
        if (field == null) {
            throw new IllegalArgumentException("Invalid column: " + update.name());
        }
        return field.isOptional();
    }

    public static PartitionSpec createPartitionSpec(
        final org.apache.iceberg.Schema schema, final List<String> partitionBy) {
        if (partitionBy.isEmpty()) {
            return PartitionSpec.unpartitioned();
        }

        final PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);
        partitionBy.forEach(
            partitionField -> {
                final Matcher matcher = TRANSFORM_REGEX.matcher(partitionField);
                if (matcher.matches()) {
                    final String transform = matcher.group(1);
                    switch (transform) {
                        case "year":
                        case "years":
                            specBuilder.year(matcher.group(2));
                            break;
                        case "month":
                        case "months":
                            specBuilder.month(matcher.group(2));
                            break;
                        case "day":
                        case "days":
                            specBuilder.day(matcher.group(2));
                            break;
                        case "hour":
                        case "hours":
                            specBuilder.hour(matcher.group(2));
                            break;
                        case "bucket": {
                            final Pair<String, Integer> args = transformArgPair(matcher.group(2));
                            specBuilder.bucket(args.first(), args.second());
                            break;
                        }
                        case "truncate": {
                            final Pair<String, Integer> args = transformArgPair(matcher.group(2));
                            specBuilder.truncate(args.first(), args.second());
                            break;
                        }
                        default:
                            throw new UnsupportedOperationException("Unsupported transform: " + transform);
                    }
                } else {
                    specBuilder.identity(partitionField);
                }
            });
        return specBuilder.build();
    }

    private static Pair<String, Integer> transformArgPair(final String argsStr) {
        final List<String> parts = Splitter.on(',').splitToList(argsStr);
        if (parts.size() != 2) {
            throw new IllegalArgumentException("Invalid argument " + argsStr + ", should have 2 parts");
        }
        return Pair.of(parts.get(0).trim(), Integer.parseInt(parts.get(1).trim()));
    }

    static class SchemaGenerator {

        private int fieldId = 1;

        SchemaGenerator() {
        }

        @SuppressWarnings("checkstyle:CyclomaticComplexity")
        Type inferIcebergType(final Object value) {
            if (value == null) {
                return null;
            } else if (value instanceof String) {
                return StringType.get();
            } else if (value instanceof Boolean) {
                return BooleanType.get();
            } else if (value instanceof BigDecimal) {
                final BigDecimal bigDecimal = (BigDecimal) value;
                return DecimalType.of(bigDecimal.precision(), bigDecimal.scale());
            } else if (value instanceof Integer || value instanceof Long) {
                return LongType.get();
            } else if (value instanceof Float || value instanceof Double) {
                return DoubleType.get();
            } else if (value instanceof LocalDate) {
                return DateType.get();
            } else if (value instanceof LocalTime) {
                return TimeType.get();
            } else if (value instanceof java.util.Date || value instanceof OffsetDateTime) {
                return TimestampType.withZone();
            } else if (value instanceof LocalDateTime) {
                return TimestampType.withoutZone();
            } else if (value instanceof List) {
                final List<?> list = (List<?>) value;
                if (list.isEmpty()) {
                    return null;
                }
                final Type elementType = inferIcebergType(list.get(0));
                return elementType == null ? null : ListType.ofOptional(nextId(), elementType);
            } else if (value instanceof Map) {
                final Map<?, ?> map = (Map<?, ?>) value;
                final List<NestedField> structFields =
                    map.entrySet().stream()
                        .filter(entry -> entry.getKey() != null && entry.getValue() != null)
                        .map(
                            entry -> {
                                final Type valueType = inferIcebergType(entry.getValue());
                                return valueType == null
                                    ? null
                                    : NestedField.optional(nextId(), entry.getKey().toString(), valueType);
                            })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                if (structFields.isEmpty()) {
                    return null;
                }
                return StructType.of(structFields);
            } else {
                return null;
            }
        }

        private int nextId() {
            return fieldId++;
        }
    }

    private SchemaUtils() {
    }
}
