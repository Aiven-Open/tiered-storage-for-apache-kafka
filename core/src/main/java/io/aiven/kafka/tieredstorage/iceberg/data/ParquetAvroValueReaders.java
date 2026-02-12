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

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericData.Record;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.parquet.ParquetValueReaders;
import org.apache.iceberg.parquet.ParquetValueReaders.StructReader;
import org.apache.iceberg.parquet.ParquetValueReaders.UnboxedReader;
import org.apache.iceberg.parquet.TypeWithSchemaVisitor;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.UUIDUtil;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * The structure, definitions and idea of these defines was taken from
 * <a href="https://github.com/apache/iceberg/blob/8e456aeeabd0a40b23864edadd622b45cb44572c/parquet/src/main/java/org/apache/iceberg/parquet/ParquetAvroValueReaders.java">ParquetAvroValueReaders</a>
 */
public class ParquetAvroValueReaders {
    private ParquetAvroValueReaders() {
    }

    @SuppressWarnings("unchecked")
    public static ParquetValueReader<Record> buildReader(final org.apache.iceberg.Schema expectedSchema,
                                                         final MessageType fileSchema, final Schema avroSchema) {
        return (ParquetValueReader<Record>)
            TypeWithSchemaVisitor.visit(
                expectedSchema.asStruct(), fileSchema,
                new ReadBuilderWithSchema(expectedSchema, fileSchema, avroSchema));
    }

    private static class ReadBuilderWithSchema extends TypeWithSchemaVisitor<ParquetValueReader<?>> {
        private final org.apache.iceberg.Schema schema;
        private final MessageType type;
        private final Schema providedAvroSchema;
        private final Map<Integer, Schema> avroSchemasByFieldId;

        ReadBuilderWithSchema(
            final org.apache.iceberg.Schema schema,
            final MessageType type,
            final Schema avroSchema) {
            this.schema = schema;
            this.type = type;
            this.providedAvroSchema = avroSchema;
            this.avroSchemasByFieldId = buildFieldSchemaMap(schema, avroSchema);
        }

        private static Map<Integer, Schema> buildFieldSchemaMap(
            final org.apache.iceberg.Schema schema,
            final Schema avroSchema) {
            final Map<Integer, Schema> result = Maps.newHashMap();
            process(schema.asStruct(), avroSchema, result);
            return result;
        }

        private static void process(
            final org.apache.iceberg.types.Type type,
            final Schema avroSchema,
            final Map<Integer, Schema> schemaMap) {

            if (avroSchema.getType() == Schema.Type.RECORD) {

                for (final Types.NestedField field : ((Types.StructType) type).fields()) {
                    final String fieldName = field.name();
                    final Schema.Field avroField = avroSchema.getField(fieldName);
                    final Schema fieldSchema = avroField.schema();
                    final Schema actualSchema = unwrapUnionIfNeeded(fieldSchema);
                    schemaMap.put(field.fieldId(), actualSchema);
                    process(field.type(), actualSchema, schemaMap);
                }
            } else if (avroSchema.getType() == Schema.Type.MAP) {
                processMapType(schemaMap, type.asMapType(), avroSchema);
            } else if (avroSchema.getType() == Schema.Type.ARRAY) {
                processListType(schemaMap, type.asListType(), avroSchema);
            }
        }

        /**
         * The only supported union type is a union of null and another type.
         * In parquet it becomes optional so there is no nested structure in schema, so Avro union needs unwrapping
         * for proper type mapping.
         */
        private static Schema unwrapUnionIfNeeded(final Schema fieldSchema) {
            Schema actualSchema = fieldSchema;
            if (fieldSchema.getType() == Schema.Type.UNION) {
                for (final Schema filedType : fieldSchema.getTypes()) {
                    if (filedType.getType() != Schema.Type.NULL) {
                        actualSchema = filedType;
                        break;
                    }
                }
            }
            return actualSchema;
        }

        private static void processListType(final Map<Integer, Schema> schemaMap, final Types.ListType listType,
                                            final Schema fieldSchema) {
            final Types.NestedField elementField = listType.fields().get(0);
            final Schema elementSchema = unwrapUnionIfNeeded(fieldSchema.getElementType());
            schemaMap.put(elementField.fieldId(), elementSchema);
            process(elementField.type(), elementSchema, schemaMap);
        }

        private static void processMapType(final Map<Integer, Schema> schemaMap, final Types.MapType mapType,
                                           final Schema fieldSchema) {
            final Types.NestedField keyField = mapType.fields().get(0);
            final Types.NestedField valueField = mapType.fields().get(1);
            final Schema valueSchema = unwrapUnionIfNeeded(fieldSchema.getValueType());


            schemaMap.put(keyField.fieldId(), Schema.create(Schema.Type.STRING));
            schemaMap.put(valueField.fieldId(), valueSchema);

            process(valueField.type(), valueSchema, schemaMap);
        }

        @Override
        public ParquetValueReader<?> message(
            final Types.StructType expected,
            final MessageType message,
            final List<ParquetValueReader<?>> fieldReaders) {
            return struct(expected, message.asGroupType(), fieldReaders);
        }

        @Override
        public ParquetValueReader<?> struct(
            final Types.StructType expected,
            final GroupType struct,
            final List<ParquetValueReader<?>> fieldReaders) {

            final Schema avroSchema;
            if (expected == schema.asStruct() && providedAvroSchema != null) {
                avroSchema = providedAvroSchema;
            } else {
                final int fieldId = struct.getId().intValue();
                avroSchema = avroSchemasByFieldId.getOrDefault(fieldId, null);
            }

            final Map<Integer, ParquetValueReader<?>> readersById = Maps.newHashMap();
            final List<Type> fields = struct.getFields();

            for (int i = 0; i < fields.size(); i += 1) {
                final Type fieldType = fields.get(i);
                final int fieldD = type.getMaxDefinitionLevel(path(fieldType.getName())) - 1;
                final int id = fieldType.getId().intValue();
                readersById.put(id, ParquetValueReaders.option(fieldType, fieldD, fieldReaders.get(i)));
            }

            final List<Types.NestedField> expectedFields =
                expected != null ? expected.fields() : ImmutableList.of();
            final List<ParquetValueReader<?>> reorderedFields =
                Lists.newArrayListWithExpectedSize(expectedFields.size());

            for (final Types.NestedField field : expectedFields) {
                final int id = field.fieldId();
                final ParquetValueReader<?> reader = readersById.get(id);
                if (reader != null) {
                    reorderedFields.add(reader);
                } else {
                    reorderedFields.add(ParquetValueReaders.nulls());
                }
            }

            return new RecordReader(reorderedFields, avroSchema);
        }

        @Override
        public ParquetValueReader<?> list(
            final Types.ListType expectedList,
            final GroupType array,
            final ParquetValueReader<?> elementReader) {

            final String[] repeatedPath = currentPath();

            final int repeatedD = type.getMaxDefinitionLevel(repeatedPath) - 1;
            final int repeatedR = type.getMaxRepetitionLevel(repeatedPath) - 1;

            final Type elementType = ParquetSchemaUtil.determineListElementType(array);
            final int elementD = type.getMaxDefinitionLevel(path(elementType.getName())) - 1;

            return new ListReader<>(
                repeatedD, 
                repeatedR, 
                ParquetValueReaders.option(elementType, elementD, elementReader),
                avroSchemasByFieldId.getOrDefault(array.getId().intValue(), null));
        }

        static class ListReader<E> extends ParquetValueReaders.ListReader<E> {
            private final Schema schema;

            ListReader(final int repeatedD, final int repeatedR, final ParquetValueReader<E> elementReader,
                       final Schema schema) {
                super(repeatedD, repeatedR, elementReader);
                this.schema = schema;
            }

            @Override
            protected List<E> buildList(final List<E> list) {
                final GenericData.Array<E> avroArray = new GenericData.Array<>(list.size(), schema);
                avroArray.addAll(list);
                return avroArray;
            }
        }

        @Override
        public ParquetValueReader<?> map(
            final Types.MapType expectedMap,
            final GroupType map,
            final ParquetValueReader<?> keyReader,
            final ParquetValueReader<?> valueReader) {

            final GroupType repeatedKeyValue = map.getFields().get(0).asGroupType();
            final String[] repeatedPath = currentPath();

            final int repeatedD = type.getMaxDefinitionLevel(repeatedPath) - 1;
            final int repeatedR = type.getMaxRepetitionLevel(repeatedPath) - 1;

            final Type keyType = repeatedKeyValue.getType(0);
            final int keyD = type.getMaxDefinitionLevel(path(keyType.getName())) - 1;
            final Type valueType = repeatedKeyValue.getType(1);
            final int valueD = type.getMaxDefinitionLevel(path(valueType.getName())) - 1;

            return new ParquetValueReaders.MapReader<>(
                repeatedD,
                repeatedR,
                ParquetValueReaders.option(keyType, keyD, keyReader),
                ParquetValueReaders.option(valueType, valueD, valueReader));
        }

        @Override
        public ParquetValueReader<?> primitive(
            final org.apache.iceberg.types.Type.PrimitiveType expected,
            final PrimitiveType primitive) {

            final ColumnDescriptor desc = type.getColumnDescription(currentPath());

            if (!avroSchemasByFieldId.containsKey(primitive.getId().intValue())) {
                return ParquetValueReaders.nulls();
            }
            if (primitive.getOriginalType() != null) {
                switch (primitive.getOriginalType()) {
                    case ENUM:
                    case JSON:
                    case UTF8:
                        final Schema utf8Schema = avroSchemasByFieldId.get(primitive.getId().intValue());
                        if (utf8Schema.getType() == Schema.Type.ENUM) {
                            return new EnumReader(desc, utf8Schema);
                        }
                        return new ParquetValueReaders.StringReader(desc);
                    case DATE:
                    case INT_8:
                    case INT_16:
                    case INT_32:
                    case INT_64:
                    case TIME_MICROS:
                    case TIMESTAMP_MICROS:
                        final Schema schema = avroSchemasByFieldId.get(primitive.getId().intValue());
                        if (schema != null && schema.getLogicalType() != null) {
                            if (schema.getLogicalType() instanceof LogicalTypes.TimestampMillis) {
                                return new TimestampMillisReader(desc);
                            } else if (schema.getLogicalType() instanceof LogicalTypes.TimeMillis) {
                                return new TimeMillisReader(desc);
                            }
                        }
                        return new UnboxedReader<>(desc);
                    case TIME_MILLIS:
                        return new TimeMillisReader(desc);
                    case TIMESTAMP_MILLIS:
                        return new TimestampMillisReader(desc);
                    case DECIMAL:
                        switch (primitive.getPrimitiveTypeName()) {
                            case BINARY:
                            case FIXED_LEN_BYTE_ARRAY:
                                final Schema fixedBytesSchema = avroSchemasByFieldId.get(primitive.getId().intValue());
                                if (fixedBytesSchema.getType() == Schema.Type.BYTES) {
                                    return new DecimalBytesReader(desc);
                                }
                                return new DecimalFixedReader(desc, fixedBytesSchema);
                            case INT64:
                                final Schema fixedSchema = avroSchemasByFieldId.get(primitive.getId().intValue());
                                if (fixedSchema != null && fixedSchema.getLogicalType() != null) {
                                    if (fixedSchema.getLogicalType() instanceof LogicalTypes.Decimal) {
                                        if (fixedSchema.getType() == Schema.Type.FIXED) {
                                            return new DecimalLongAsFixedReader(desc, fixedSchema);
                                        }
                                    }
                                }
                                return new DecimalLongAsBytesReader(desc);
                            case INT32:
                                return new DecimalIntegerAsBytesReader(desc);
                            default:
                                throw new UnsupportedOperationException(
                                    "Unsupported base type for decimal: " + primitive.getPrimitiveTypeName());
                        }
                    case BSON:
                        return new ParquetValueReaders.BytesReader(desc);
                    default:
                        throw new UnsupportedOperationException(
                            "Unsupported logical type: " + primitive.getOriginalType());
                }
            }

            switch (primitive.getPrimitiveTypeName()) {
                case FIXED_LEN_BYTE_ARRAY:
                    final Schema avroSchema = avroSchemasByFieldId.get(primitive.getId().intValue());
                    final LogicalTypeAnnotation logicalTypeAnnotation = primitive.getLogicalTypeAnnotation();
                    if (logicalTypeAnnotation != null
                        && logicalTypeAnnotation.toString().equals("UUID")
                        && avroSchema.getType() == Schema.Type.STRING) {
                        return new UuidReader(desc);
                    }
                    return new FixedReader(desc, avroSchema);
                case BINARY:
                    return new ParquetValueReaders.BytesReader(desc);
                case INT32:
                    if (expected != null && expected.typeId() == TypeID.LONG) {
                        return new ParquetValueReaders.IntAsLongReader(desc);
                    } else {
                        return new UnboxedReader<>(desc);
                    }
                case FLOAT:
                    if (expected != null && expected.typeId() == TypeID.DOUBLE) {
                        return new ParquetValueReaders.FloatAsDoubleReader(desc);
                    } else {
                        return new UnboxedReader<>(desc);
                    }
                case BOOLEAN:
                case INT64:
                case DOUBLE:
                    return new UnboxedReader<>(desc);
                default:
                    throw new UnsupportedOperationException("Unsupported type: " + primitive);
            }
        }
    }

    private static class TimestampMillisReader extends UnboxedReader<Long> {
        private TimestampMillisReader(final ColumnDescriptor desc) {
            super(desc);
        }

        @Override
        public Long read(final Long ignored) {
            return readLong();
        }

        @Override
        public long readLong() {
            return column.nextLong() / 1000L;
        }
    }

    static class EnumReader extends ParquetValueReaders.PrimitiveReader<GenericData.EnumSymbol> {
        private final Schema schema;

        EnumReader(final ColumnDescriptor desc, final Schema schema) {
            super(desc);
            this.schema = schema;
        }

        @Override
        public GenericData.EnumSymbol read(final GenericData.EnumSymbol ignored) {
            return new GenericData.EnumSymbol(schema, column.nextBinary().toStringUsingUTF8());
        }
    }

    static class UuidReader extends ParquetValueReaders.PrimitiveReader<String> {
        UuidReader(final ColumnDescriptor desc) {
            super(desc);
        }

        @Override
        public String read(final String ignored) {
            return UUIDUtil.convert(column.nextBinary().toByteBuffer()).toString();
        }
    }

    static class FixedReader extends ParquetValueReaders.PrimitiveReader<Fixed> {
        private final Schema schema;

        FixedReader(final ColumnDescriptor desc, final Schema schema) {
            super(desc);
            this.schema = schema;
        }

        @Override
        public Fixed read(final Fixed reuse) {
            final Fixed fixed = Objects.requireNonNullElseGet(reuse, () -> new Fixed(schema));
            column.nextBinary().toByteBuffer().get(fixed.bytes());
            return fixed;
        }
    }

    public static class TimeMillisReader extends UnboxedReader<Integer> {
        TimeMillisReader(final ColumnDescriptor desc) {
            super(desc);
        }

        @Override
        public Integer read(final Integer ignored) {
            return readInteger();
        }

        @Override
        public int readInteger() {
            return Long.valueOf(column.nextLong() / 1000).intValue();
        }
    }

    static class RecordReader extends StructReader<Record, Record> {
        private final Schema schema;

        RecordReader(final List<ParquetValueReader<?>> readers, final Schema schema) {
            super(readers);
            this.schema = schema;
        }

        @Override
        protected Record newStructData(final Record reuse) {
            return Objects.requireNonNullElseGet(reuse, () -> new Record(schema));
        }

        @Override
        protected Object getField(final Record intermediate, final int pos) {
            return intermediate.get(pos);
        }

        @Override
        protected Record buildStruct(final Record struct) {
            return struct;
        }

        @Override
        protected void set(final Record struct, final int pos, final Object value) {
            struct.put(pos, value);
        }
    }

    static class DecimalFixedReader extends ParquetValueReaders.PrimitiveReader<Fixed> {

        private final Schema schema;

        public DecimalFixedReader(final ColumnDescriptor desc, final Schema schema) {
            super(desc);
            this.schema = schema;
        }

        @Override
        public Fixed read(final Fixed decoder) {
            byte[] bytes = column.nextBinary().getBytesUnsafe();
            final int size = schema.getFixedSize();
            if (bytes.length < size) {
                final byte[] padded = new byte[size];
                System.arraycopy(bytes, 0, padded, size - bytes.length, bytes.length);
                bytes = padded;
            }
            return new Fixed(schema, bytes);
        }
    }

    static class DecimalBytesReader extends ParquetValueReaders.PrimitiveReader<ByteBuffer> {
        public DecimalBytesReader(final ColumnDescriptor desc) {
            super(desc);
        }

        @Override
        public ByteBuffer read(final ByteBuffer decoder) {
            return ByteBuffer.wrap(column.nextBinary().getBytesUnsafe());
        }
    }

    static class DecimalLongAsBytesReader extends ParquetValueReaders.PrimitiveReader<ByteBuffer> {

        public DecimalLongAsBytesReader(final ColumnDescriptor desc) {
            super(desc);
        }

        @Override
        public ByteBuffer read(final ByteBuffer reuse) {
            return ByteBuffer.wrap(BigInteger.valueOf(column.nextLong()).toByteArray());
        }
    }

    static class DecimalLongAsFixedReader extends ParquetValueReaders.PrimitiveReader<Fixed> {
        private final Schema schema;

        public DecimalLongAsFixedReader(final ColumnDescriptor desc, final Schema schema) {
            super(desc);
            this.schema = schema;
        }

        @Override
        public Fixed read(final Fixed reuse) {
            byte[] bytes = BigInteger.valueOf(column.nextLong()).toByteArray();
            final int size = schema.getFixedSize();
            if (bytes.length < size) {
                final byte[] padded = new byte[size];
                System.arraycopy(bytes, 0, padded, size - bytes.length, bytes.length);
                bytes = padded;
            }
            return new Fixed(schema, bytes);
        }
    }

    static class DecimalIntegerAsBytesReader extends ParquetValueReaders.PrimitiveReader<ByteBuffer> {

        public DecimalIntegerAsBytesReader(final ColumnDescriptor desc) {
            super(desc);
        }

        @Override
        public ByteBuffer read(final ByteBuffer reuse) {
            return ByteBuffer.wrap(BigInteger.valueOf(column.nextInteger()).toByteArray());
        }
    }
}
