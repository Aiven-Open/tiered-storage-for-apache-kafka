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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.temporal.Temporal;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.DateTimeUtil;

/**
 * The structure, definitions and idea of these defines was taken from
 * <a href="https://github.com/apache/iceberg/blob/main/kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/data/RecordConverter.java">RecordConverter</a>
 */
public class RecordConverter {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final org.apache.iceberg.Schema tableSchema;
    private final Map<Integer, Map<String, NestedField>> structNameMap = new HashMap<>();
    private final Conversions.UUIDConversion uuidConversion = new Conversions.UUIDConversion();

    public RecordConverter(final org.apache.iceberg.Schema schema) {
        this.tableSchema = schema;
    }

    public Record convert(final Object data) {
        return convert(data, null);
    }

    Record convert(final Object data, final SchemaUpdate.Consumer schemaUpdateConsumer) {
        return convertStructValue(data, tableSchema.asStruct(), -1, schemaUpdateConsumer);
    }

    private Object convertValue(
        final Object value, final Schema schema, final Type type, final int fieldId,
        final SchemaUpdate.Consumer schemaUpdateConsumer) {
        if (value == null) {
            return null;
        }
        switch (type.typeId()) {
            case STRUCT:
                return convertStructValue(value, type.asStructType(), fieldId, schemaUpdateConsumer);
            case LIST:
                return convertListValue(value, type.asListType(), schema.getElementType(), schemaUpdateConsumer);
            case MAP:
                return convertMapValue(value, type.asMapType(), schema, schemaUpdateConsumer);
            case INTEGER:
                return convertInt(value);
            case LONG:
                return convertLong(value);
            case FLOAT:
                return convertFloat(value);
            case DOUBLE:
                return convertDouble(value);
            case DECIMAL:
                return convertDecimal(value, schema.getLogicalType(), (DecimalType) type);
            case BOOLEAN:
                return convertBoolean(value);
            case STRING:
                return convertString(value);
            case UUID:
                return convertUuid(value);
            case BINARY:
                return convertBase64Binary(value);
            case FIXED:
                return ByteBuffers.toByteArray(convertBase64Binary(value));
            case DATE:
                return convertDateValue(value);
            case TIME:
                return convertTimeValue(value, schema.getLogicalType());
            case TIMESTAMP:
                return convertTimestampValue(value, schema.getLogicalType(), (TimestampType) type);
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type.typeId());
        }
    }

    protected GenericRecord convertStructValue(
        final Object value,
        final StructType schema,
        final int parentFieldId,
        final SchemaUpdate.Consumer schemaUpdateConsumer) {
        if (value instanceof org.apache.avro.generic.GenericRecord) {
            return convertToStruct((org.apache.avro.generic.GenericRecord) value, schema, parentFieldId,
                schemaUpdateConsumer);
        }
        throw new IllegalArgumentException("Cannot convert to struct: " + value.getClass().getName());
    }

    /** This method will be called for records and struct values when there is a record schema. */
    private GenericRecord convertToStruct(
        final org.apache.avro.generic.GenericRecord struct,
        final StructType schema,
        final int structFieldId,
        final SchemaUpdate.Consumer schemaUpdateConsumer) {
        final GenericRecord result = GenericRecord.create(schema);
        struct
            .getSchema()
            .getFields()
            .forEach(
                recordField -> {
                    final NestedField tableField = lookupStructField(recordField.name(), schema, structFieldId);
                    if (tableField == null) {
                        // TODO the schema evolution is not implemented yet. Keeping for future use.
                        // add the column if schema evolution is on, otherwise skip the value
                        if (schemaUpdateConsumer != null) {
                            final String parentFieldName =
                                structFieldId < 0 ? null : tableSchema.findColumnName(structFieldId);
                            final Type type =
                                AvroSchemaUtil.toIceberg(recordField.schema()).findType(recordField.name());
                            schemaUpdateConsumer.addColumn(parentFieldName, recordField.name(), type);
                        }
                    } else {
                        boolean hasSchemaUpdates = false;
                        if (schemaUpdateConsumer != null) {
                            // update the type if needed and schema evolution is on
                            final PrimitiveType evolveDataType =
                                SchemaUtils.needsDataTypeUpdate(tableField.type(), recordField.schema());
                            if (evolveDataType != null) {
                                final String fieldName = tableSchema.findColumnName(tableField.fieldId());
                                schemaUpdateConsumer.updateType(fieldName, evolveDataType);
                                hasSchemaUpdates = true;
                            }
                            // make optional if needed and schema evolution is on
                            if (tableField.isRequired() && recordField.schema().isNullable()) {
                                final String fieldName = tableSchema.findColumnName(tableField.fieldId());
                                schemaUpdateConsumer.makeOptional(fieldName);
                                hasSchemaUpdates = true;
                            }
                        }
                        if (!hasSchemaUpdates) {
                            if (tableField.isOptional() && recordField.schema().isNullable()) {
                                Schema recordSchema = null;
                                for (final Schema field : recordField.schema().getTypes()) {
                                    if (field.getType() != Schema.Type.NULL) {
                                        recordSchema = field;
                                    }
                                }
                                result.setField(
                                    tableField.name(),
                                    convertValue(
                                        struct.get(recordField.name()),
                                        recordSchema,
                                        tableField.type(),
                                        tableField.fieldId(),
                                        schemaUpdateConsumer));
                            } else {
                                result.setField(
                                    tableField.name(),
                                    convertValue(
                                        struct.get(recordField.name()),
                                        recordField.schema(),
                                        tableField.type(),
                                        tableField.fieldId(),
                                        schemaUpdateConsumer));
                            }
                        }
                    }
                });
        return result;
    }

    private NestedField lookupStructField(final String fieldName, final StructType schema, final int structFieldId) {
        return /*config.schemaCaseInsensitive()//TODO add config
                ? schema.caseInsensitiveField(fieldName);
                :*/ schema.field(fieldName);
    }

    protected List<Object> convertListValue(
        final Object value, final ListType type, final Schema elementType,
        final SchemaUpdate.Consumer schemaUpdateConsumer) {
        Preconditions.checkArgument(value instanceof List);
        final List<?> list = (List<?>) value;

        return list.stream()
            .map(
                element -> {
                    final int fieldId = type.fields().get(0).fieldId();
                    return convertValue(element, elementType, type.elementType(), fieldId, schemaUpdateConsumer);
                })
            .collect(Collectors.toList());
    }

    /**
     * Converts value for Iceberg's MAP type.
     * Since Avro does not support maps with non-string keys but Iceberg does,
     * it is a common practice to describe such maps as an array of records in Avro,
     * so corresponding type in Avro schema can be either MAP or ARRAY.
     */
    protected Map<Object, Object> convertMapValue(
        final Object value, final MapType type, final Schema schema,
        final SchemaUpdate.Consumer schemaUpdateConsumer) {
        final Map<Object, Object> result = new HashMap<>();
        if (schema.getType() == Schema.Type.MAP) {
            Preconditions.checkArgument(value instanceof Map);
            final Map<?, ?> map = (Map<?, ?>) value;
            map.forEach(
                (k, v) -> {
                    final int keyFieldId = type.fields().get(0).fieldId();
                    final int valueFieldId = type.fields().get(1).fieldId();
                    result.put(
                        convertValue(k, Schema.create(Schema.Type.STRING),
                            type.keyType(),
                            keyFieldId,
                            schemaUpdateConsumer),
                        convertValue(v, schema.getValueType(), type.valueType(), valueFieldId, schemaUpdateConsumer));
                });
        } else if (schema.getType() == Schema.Type.ARRAY) {
            final GenericData.Array<?> arrayValue = (GenericData.Array<?>) value;
            final Schema elementSchema = schema.getElementType();
            final List<Schema.Field> fields = elementSchema.getFields();
            for (final Object element : arrayValue) {
                final org.apache.avro.generic.GenericRecord record = (org.apache.avro.generic.GenericRecord) element;
                final int keyFieldId = type.fields().get(0).fieldId();
                final int valueFieldId = type.fields().get(1).fieldId();
                final Object keyField = record.get(0);
                final Object valueField = record.get(1);

                result.put(
                    convertValue(keyField, fields.get(0).schema(),
                        type.keyType(),
                        keyFieldId,
                        schemaUpdateConsumer),
                    convertValue(valueField, fields.get(1).schema(), type.valueType(), valueFieldId,
                        schemaUpdateConsumer));
            }
        }
        return result;
    }

    protected int convertInt(final Object value) {
        if (value instanceof Number) {
            return ((Number) value).intValue();
        } else if (value instanceof String) {
            return Integer.parseInt((String) value);
        }
        throw new IllegalArgumentException("Cannot convert to int: " + value.getClass().getName());
    }

    protected long convertLong(final Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        } else if (value instanceof String) {
            return Long.parseLong((String) value);
        }
        throw new IllegalArgumentException("Cannot convert to long: " + value.getClass().getName());
    }

    protected float convertFloat(final Object value) {
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        } else if (value instanceof String) {
            return Float.parseFloat((String) value);
        }
        throw new IllegalArgumentException("Cannot convert to float: " + value.getClass().getName());
    }

    protected double convertDouble(final Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else if (value instanceof String) {
            return Double.parseDouble((String) value);
        }
        throw new IllegalArgumentException("Cannot convert to double: " + value.getClass().getName());
    }

    protected BigDecimal convertDecimal(final Object value, final LogicalType logicalType, final DecimalType type) {
        final BigDecimal bigDecimal;
        final Conversions.DecimalConversion conversion = new Conversions.DecimalConversion();
        if (value instanceof ByteBuffer) {
            bigDecimal = conversion.fromBytes((ByteBuffer) value, null, logicalType);
        } else if (value instanceof GenericFixed) {
            bigDecimal = conversion.fromFixed((GenericFixed) value, null, logicalType);
        } else {
            throw new IllegalArgumentException(
                "Cannot convert to BigDecimal: " + value.getClass().getName());
        }
        return bigDecimal.setScale(type.scale(), RoundingMode.HALF_UP);
    }

    protected boolean convertBoolean(final Object value) {
        if (value instanceof Boolean) {
            return (boolean) value;
        } else if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        }
        throw new IllegalArgumentException("Cannot convert to boolean: " + value.getClass().getName());
    }

    protected String convertString(final Object value) {
        try {
            if (value instanceof String) {
                return (String) value;
            } else if (value instanceof Utf8 || value instanceof GenericData.EnumSymbol) {
                return value.toString();
            } else if (value instanceof Number || value instanceof Boolean) {
                return value.toString();
            } else if (value instanceof Map || value instanceof List) {
                return MAPPER.writeValueAsString(value);
            } else if (value instanceof org.apache.avro.generic.GenericRecord) {
                final Schema schema = ((org.apache.avro.generic.GenericRecord) value).getSchema();
                final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                final DatumWriter<org.apache.avro.generic.GenericRecord> writer = new GenericDatumWriter<>(schema);
                final JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, outputStream, true);

                writer.write((org.apache.avro.generic.GenericRecord) value, encoder);
                encoder.flush();
                return outputStream.toString(StandardCharsets.UTF_8);
            }
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        throw new IllegalArgumentException("Cannot convert to string: " + value.getClass().getName());
    }

    protected Object convertUuid(final Object value) {
        final UUID uuid;
        if (value instanceof String) {
            uuid = UUID.fromString((String) value);
        } else if (value instanceof UUID) {
            uuid = (UUID) value;
        } else if (value instanceof Utf8) {
            uuid = UUID.fromString(((Utf8) value).toString());
        } else if (value instanceof GenericData.Fixed) {
            return uuidConversion.fromFixed((GenericFixed) value, null, LogicalTypes.uuid());
        } else {
            throw new IllegalArgumentException("Cannot convert to UUID: " + value.getClass().getName());
        }
        return uuid;
    }

    protected ByteBuffer convertBase64Binary(final Object value) {
        if (value instanceof String) {
            return ByteBuffer.wrap(Base64.getDecoder().decode((String) value));
        } else if (value instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) value);
        } else if (value instanceof ByteBuffer) {
            return (ByteBuffer) value;
        } else if (value instanceof GenericData.Fixed) {
            return ByteBuffer.wrap(((GenericData.Fixed) value).bytes());
        }
        throw new IllegalArgumentException("Cannot convert to binary: " + value.getClass().getName());
    }

    @SuppressWarnings("JavaUtilDate")
    protected LocalDate convertDateValue(final Object value) {
        if (value instanceof Number) {
            final int days = ((Number) value).intValue();
            return DateTimeUtil.dateFromDays(days);
        } else if (value instanceof String) {
            return LocalDate.parse((String) value);
        } else if (value instanceof LocalDate) {
            return (LocalDate) value;
        } else if (value instanceof Date) {
            final int days = (int) (((Date) value).getTime() / 1000 / 60 / 60 / 24);
            return DateTimeUtil.dateFromDays(days);
        }
        throw new RuntimeException("Cannot convert date: " + value);
    }

    @SuppressWarnings("JavaUtilDate")
    protected LocalTime convertTimeValue(final Object value, final LogicalType logicalType) {
        if (value instanceof Number) {
            if (logicalType instanceof LogicalTypes.TimeMillis) {
                final long timeMillis = ((Number) value).longValue();
                return DateTimeUtil.timeFromMicros(timeMillis * 1000);
            } else if (logicalType instanceof LogicalTypes.TimeMicros) {
                final long timeMicros = ((Number) value).longValue();
                return DateTimeUtil.timeFromMicros(timeMicros);
            }
        }
        throw new RuntimeException("Cannot convert time: " + value);
    }

    protected Temporal convertTimestampValue(final Object value, final LogicalType logicalType,
                                             final TimestampType type) {
        if (type.shouldAdjustToUTC()) {
            return convertOffsetDateTime(value, logicalType);
        }
        return convertLocalDateTime(value, logicalType);
    }

    @SuppressWarnings("JavaUtilDate")
    private OffsetDateTime convertOffsetDateTime(final Object value, final LogicalType logicalType) {
        if (value instanceof Number) {
            if (logicalType instanceof LogicalTypes.TimestampMillis
                || logicalType instanceof LogicalTypes.LocalTimestampMillis) {
                final long timestampMillis = ((Number) value).longValue();
                return DateTimeUtil.timestamptzFromMicros(timestampMillis * 1000);
            } else if (logicalType instanceof LogicalTypes.TimestampMicros
                || logicalType instanceof LogicalTypes.LocalTimestampMicros) {
                final long timestampMicros = ((Number) value).longValue();
                return DateTimeUtil.timestamptzFromMicros(timestampMicros);
            }
        }
        throw new RuntimeException(
            "Cannot convert timestamptz: " + value + ", type: " + value.getClass());
    }

    @SuppressWarnings("JavaUtilDate")
    private LocalDateTime convertLocalDateTime(final Object value, final LogicalType logicalType) {
        if (value instanceof Number) {
            if (logicalType instanceof LogicalTypes.TimestampMillis) {
                final long timestampMillis = ((Number) value).longValue();
                return DateTimeUtil.timestampFromMicros(timestampMillis * 1000);
            } else if (logicalType instanceof LogicalTypes.TimestampMicros) {
                final long timestampMicros = ((Number) value).longValue();
                return DateTimeUtil.timestampFromMicros(timestampMicros);
            }
        }
        throw new RuntimeException(
            "Cannot convert timestamp: " + value + ", type: " + value.getClass());
    }
}
