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

package io.aiven.kafka.tieredstorage.iceberg.data;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.Files;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.apache.avro.LogicalTypes.date;
import static org.apache.avro.LogicalTypes.decimal;
import static org.apache.avro.LogicalTypes.localTimestampMicros;
import static org.apache.avro.LogicalTypes.localTimestampMillis;
import static org.apache.avro.LogicalTypes.timeMicros;
import static org.apache.avro.LogicalTypes.timeMillis;
import static org.apache.avro.LogicalTypes.timestampMicros;
import static org.apache.avro.LogicalTypes.timestampMillis;
import static org.apache.avro.LogicalTypes.uuid;
import static org.apache.avro.SchemaBuilder.builder;
import static org.apache.avro.SchemaBuilder.fixed;
import static org.apache.avro.SchemaBuilder.record;
import static org.assertj.core.api.Assertions.assertThat;

public class ConversionTest {

    private static final Conversions.DecimalConversion DECIMAL_CONVERSION = new Conversions.DecimalConversion();

    private GenericRecord writeAndReadRecord(final Schema avroSchema,
                                             final Object avroObject,
                                             final Path tempDir) throws Exception {
        final org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);

        final RecordConverter converter = new RecordConverter(icebergSchema);
        final Record icebergRecord = converter.convert(avroObject);

        final Path parquetPath = tempDir.resolve("iceberg-test.parquet");
        final OutputFile out = Files.localOutput(parquetPath.toFile());
        try (final FileAppender<Record> writer = Parquet.write(out)
            .schema(icebergSchema)
            .createWriterFunc(GenericParquetWriter::create)
            .build()) {
            writer.add(icebergRecord);
        }

        final InputFile in = Files.localInput(parquetPath.toFile());
        try (final CloseableIterable<GenericRecord> records = Parquet.read(in)
            .project(icebergSchema)
            .createReaderFunc(
                (fileSchema, messageType) -> ParquetAvroValueReaders.buildReader(icebergSchema, messageType,
                    avroSchema))
            .build()) {
            return records.iterator().next();
        }
    }

    private ByteBuffer decimalToBytes(final String decimalStr) {
        return ByteBuffer.wrap(new BigDecimal(decimalStr).unscaledValue().toByteArray());
    }

    private GenericFixed decimalToFixed(final String decimalStr, final Schema schema) {
        return DECIMAL_CONVERSION.toFixed(new BigDecimal(decimalStr), schema, schema.getLogicalType());
    }

    /**
     * According to default Avro conversions, decimal types can be encoded as
     * <ul>
     *   <li>bytes - for decimal logical type</li>
     *   <li>fixed - for decimal logical type with fixed size</li>
     * </ul>
     * {@link org.apache.avro.Conversions.DecimalConversion}
     *
     * <p>This also takes into account Parquet base type optimization for decimal:
     * <ul>
     *   <li>precision <= 9: int32</li>
     *   <li>precision <= 18: int64</li>
     *   <li>precision > 18: fixed-length byte array</li>
     * </ul>
     * See: <a href="https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal">Parquet Decimal Logical Type</a>
     *
     * <p>Oversized fixed size in Avro schema (fixed size larger than required for the precision)
     * is supported and and covered here since it requires 0-padding when reading from Parquet.
     */
    @Test
    public void testDecimalTypes(@TempDir final java.nio.file.Path tempDir) throws Exception {
        final Schema avroSchema = record("DecimalRecord").fields()
            .name("decimal_bytes").type(decimal(10, 2).addToSchema(builder().bytesType())).noDefault()
            .name("decimal_int32").type(decimal(9, 2).addToSchema(builder().bytesType())).noDefault()
            .name("decimal_int64").type(decimal(18, 2).addToSchema(builder().bytesType())).noDefault()
            .name("decimal_fixed").type(decimal(10, 2).addToSchema(fixed("decimal_fixed_type").size(5))).noDefault()
            .name("decimal_fixed_oversized").type(
                decimal(30, 2).addToSchema(fixed("decimal_fixed_oversized_type").size(16))).noDefault()
            .name("decimal_high_precision").type(
                decimal(38, 10).addToSchema(fixed("decimal_high_precision_type").size(16))).noDefault()
            .name("decimal_high_precision_bytes").type(decimal(30, 8).addToSchema(builder().bytesType())).noDefault()
            .endRecord();

        final GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("decimal_bytes", decimalToBytes("12345.67"));
        avroRecord.put("decimal_int32", decimalToBytes("12345.67"));
        avroRecord.put("decimal_int64", decimalToBytes("1234567890123.45"));
        avroRecord.put("decimal_fixed", decimalToFixed("12345.67", avroSchema.getField("decimal_fixed").schema()));
        avroRecord.put("decimal_fixed_oversized",
            decimalToFixed("123.45", avroSchema.getField("decimal_fixed_oversized").schema()));
        avroRecord.put("decimal_high_precision",
            decimalToFixed("12345678901234567890.1234567890", avroSchema.getField("decimal_high_precision").schema()));
        avroRecord.put("decimal_high_precision_bytes", decimalToBytes("9999999999999999999999.99999999"));

        final GenericRecord readRecord = writeAndReadRecord(avroSchema, avroRecord, tempDir);

        assertThat(readRecord).usingRecursiveComparison().isEqualTo(avroRecord);
    }

    /**
     * Covering nested logical types with Decimal as a representative.
     */
    @Test
    public void testNestedDecimalRecord(@TempDir final java.nio.file.Path tempDir) throws Exception {
        final Schema nestedSchema = record("NestedDecimal").fields()
            .name("nested_decimal_int32").type(decimal(9, 3).addToSchema(builder().bytesType())).noDefault()
            .name("nested_decimal_fixed")
                .type(decimal(16, 6).addToSchema(fixed("nested_fixed_type").size(8))).noDefault()
            .endRecord();

        final Schema avroSchema = record("RecordWithNestedDecimal").fields()
            .name("record_with_decimals").type(nestedSchema).noDefault()
            .endRecord();

        final GenericRecord nestedDecimal = new GenericData.Record(nestedSchema);
        nestedDecimal.put("nested_decimal_int32", decimalToBytes("987.654"));
        nestedDecimal.put("nested_decimal_fixed",
            decimalToFixed("1234567.891234", nestedSchema.getField("nested_decimal_fixed").schema()));

        final GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("record_with_decimals", nestedDecimal);

        final GenericRecord readRecord = writeAndReadRecord(avroSchema, avroRecord, tempDir);

        assertThat(readRecord).usingRecursiveComparison().isEqualTo(avroRecord);
    }

    /**
     * According to default Avro conversions, timestamp types can be encoded as
     * <ul>
     *   <li>timestamp-millis - long</li>
     *   <li>timestamp-micros - long</li>
     *   <li>local-timestamp-millis - long</li>
     *   <li>local-timestamp-micros - long</li>
     * </ul>
     * {@link org.apache.avro.data.TimeConversions.TimestampMillisConversion}
     * {@link org.apache.avro.data.TimeConversions.TimestampMicrosConversion}
     * {@link org.apache.avro.data.TimeConversions.LocalTimestampMillisConversion}
     * {@link org.apache.avro.data.TimeConversions.LocalTimestampMicrosConversion}
     * Also covering "adjust-to-utc" property for UTC timestamps recognized properly by Iceberg.
     * There is no proper support for local timestamps in Iceberg yet.
     */
    @Test
    public void testTimestampTypes(@TempDir final java.nio.file.Path tempDir) throws Exception {
        final Schema avroSchema = record("TimestampRecord").fields()
            .name("timestamp_millis").type(timestampMillis().addToSchema(builder().longType())).noDefault()
            .name("timestamp_micros").type(timestampMicros().addToSchema(builder().longType())).noDefault()
            .name("timestamp_millis_local").type(localTimestampMillis().addToSchema(builder().longType())).noDefault()
            .name("timestamp_micros_local").type(localTimestampMicros().addToSchema(builder().longType())).noDefault()
            // Add UTC timestamps
            .name("timestamp_millis_utc").type(timestampMillis().addToSchema(builder().longType())).noDefault()
            .name("timestamp_micros_utc").type(timestampMicros().addToSchema(builder().longType())).noDefault()
            .endRecord();

        avroSchema.getField("timestamp_millis_utc").schema().addProp("adjust-to-utc", true);
        avroSchema.getField("timestamp_micros_utc").schema().addProp("adjust-to-utc", true);

        final GenericRecord avroRecord = new GenericData.Record(avroSchema);
        final long millisTimestamp = 1625077800000L;
        final long microsTimestamp = millisTimestamp * 1000L;

        avroRecord.put("timestamp_millis", millisTimestamp);
        avroRecord.put("timestamp_micros", microsTimestamp);
        avroRecord.put("timestamp_millis_local", millisTimestamp);
        avroRecord.put("timestamp_micros_local", microsTimestamp);
        avroRecord.put("timestamp_millis_utc", millisTimestamp);
        avroRecord.put("timestamp_micros_utc", microsTimestamp);

        final GenericRecord readRecord = writeAndReadRecord(avroSchema, avroRecord, tempDir);

        assertThat(readRecord).usingRecursiveComparison().isEqualTo(avroRecord);
    }

    /**
     * According to default Avro conversions, date and time types can be encoded as
     * <ul>
     *   <li>date - int</li>
     *   <li>time-millis - int</li>
     *   <li>time-micros - long</li>
     * </ul>
     * {@link org.apache.avro.data.TimeConversions.DateConversion}
     * {@link org.apache.avro.data.TimeConversions.TimeMillisConversion}
     * {@link org.apache.avro.data.TimeConversions.TimeMicrosConversion}
     */
    @Test
    public void testDateTimeTypes(@TempDir final java.nio.file.Path tempDir) throws Exception {
        final Schema avroSchema = record("DateTimeRecord").fields()
            .name("date").type(date().addToSchema(builder().intType())).noDefault()
            .name("time_millis").type(timeMillis().addToSchema(builder().intType())).noDefault()
            .name("time_micros").type(timeMicros().addToSchema(builder().longType())).noDefault()
            .endRecord();

        final GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("date", 19000);
        avroRecord.put("time_millis", 12345678);
        avroRecord.put("time_micros", 86399999999L);

        final GenericRecord readRecord = writeAndReadRecord(avroSchema, avroRecord, tempDir);

        assertThat(readRecord).usingRecursiveComparison().isEqualTo(avroRecord);
    }

    /**
     * According to {@link org.apache.avro.Conversions.UUIDConversion} default Avro UUID conversion supports
     * only string and fixed types.
     */
    @Test
    public void testUuidType(@TempDir final java.nio.file.Path tempDir) throws Exception {
        final Schema avroSchema = record("UuidRecord").fields()
            .name("uuid_string").type(uuid().addToSchema(builder().stringType())).noDefault()
            .name("uuid_fixed").type(uuid()
                .addToSchema(uuid().addToSchema(builder().fixed("uuid_fixed").size(16)))).noDefault()
            .endRecord();

        final GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("uuid_string", UUID.randomUUID().toString());

        final Conversions.UUIDConversion uuidConversion = new Conversions.UUIDConversion();
        final UUID uuid = UUID.randomUUID();
        final Schema uuidFixedSchema = avroSchema.getField("uuid_fixed").schema();
        final GenericFixed uuidFixed = uuidConversion.toFixed(uuid, uuidFixedSchema, uuidFixedSchema.getLogicalType());
        avroRecord.put("uuid_fixed", uuidFixed);


        final GenericRecord readRecord = writeAndReadRecord(avroSchema, avroRecord, tempDir);

        assertThat(readRecord).usingRecursiveComparison().isEqualTo(avroRecord);
    }

    @Test
    public void testPrimitiveTypes(@TempDir final java.nio.file.Path tempDir) throws Exception {
        final Schema avroSchema = record("SimpleTypesRecord").fields()
            .requiredInt("int_field")
            .requiredLong("long_field")
            .requiredFloat("float_field")
            .requiredDouble("double_field")
            .requiredBoolean("boolean_field")
            .requiredString("string_field")
            .name("bytes_field").type().bytesType().noDefault()
            .name("fixed_field").type().fixed("fixed_16").size(16).noDefault()
            .name("enum_field").type().enumeration("Suit").symbols("SPADES", "HEARTS", "DIAMONDS", "CLUBS").noDefault()
            .endRecord();

        final GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("int_field", 42);
        avroRecord.put("long_field", 123456789012345L);
        avroRecord.put("float_field", 3.14f);
        avroRecord.put("double_field", 2.71828d);
        avroRecord.put("boolean_field", true);
        avroRecord.put("string_field", "hello world");
        avroRecord.put("bytes_field", ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5}));
        avroRecord.put("fixed_field", new GenericData.Fixed(avroSchema.getField("fixed_field").schema(), new byte[16]));
        avroRecord.put("enum_field", new GenericData.EnumSymbol(avroSchema.getField("enum_field").schema(), "HEARTS"));

        final GenericRecord readRecord = writeAndReadRecord(avroSchema, avroRecord, tempDir);

        assertThat(readRecord).usingRecursiveComparison().isEqualTo(avroRecord);
    }

    @Test
    public void testArrays(@TempDir final java.nio.file.Path tempDir) throws Exception {
        final Schema recordInArraySchema = record("RecordInArray").fields()
            .requiredString("name")
            .requiredInt("value")
            .endRecord();

        final Schema avroSchema = record("ArrayRecord").fields()
            .name("array_of_ints").type().array().items().intType().noDefault()
            .name("array_of_strings").type().array().items().stringType().noDefault()
            .name("array_of_bytes").type().array().items().bytesType().noDefault()
            .name("array_of_timestamps").type().array()
                .items(timestampMillis().addToSchema(builder().longType())).noDefault()
            .name("array_of_decimals").type().array()
                .items(decimal(10, 2).addToSchema(builder().bytesType())).noDefault()
            .name("array_of_arrays").type().array().items().array().items().intType().noDefault()
            .name("array_of_records").type().array().items(recordInArraySchema).noDefault()
            .name("array_of_unions").type().array()
                .items().unionOf().nullType().and().stringType().endUnion().noDefault()
            .endRecord();

        final GenericRecord avroRecord = new GenericData.Record(avroSchema);

        avroRecord.put("array_of_ints", Arrays.asList(1, 2, 3));
        avroRecord.put("array_of_strings", Arrays.asList("a", "b", "c"));

        final List<ByteBuffer> bytesList = new ArrayList<>();
        bytesList.add(ByteBuffer.wrap(new byte[] {1, 2, 3}));
        bytesList.add(ByteBuffer.wrap(new byte[] {4, 5, 6}));
        avroRecord.put("array_of_bytes", bytesList);

        avroRecord.put("array_of_timestamps", Arrays.asList(1625077800000L, 1625077900000L));

        final List<ByteBuffer> decimalList = new ArrayList<>();
        decimalList.add(decimalToBytes("123.45"));
        decimalList.add(decimalToBytes("678.90"));
        avroRecord.put("array_of_decimals", decimalList);

        final List<List<Integer>> arrays2D = new ArrayList<>();
        arrays2D.add(Arrays.asList(1, 2, 3));
        arrays2D.add(Arrays.asList(4, 5, 6));
        avroRecord.put("array_of_arrays", arrays2D);

        final List<GenericRecord> recordList = new ArrayList<>();
        final GenericRecord rec1 = new GenericData.Record(recordInArraySchema);
        rec1.put("name", "record1");
        rec1.put("value", 101);
        final GenericRecord rec2 = new GenericData.Record(recordInArraySchema);
        rec2.put("name", "record2");
        rec2.put("value", 102);
        recordList.add(rec1);
        recordList.add(rec2);
        avroRecord.put("array_of_records", recordList);

        final List<Object> unionList = new ArrayList<>();
        unionList.add("string value");
        unionList.add(null);
        avroRecord.put("array_of_unions", unionList);

        final GenericRecord readRecord = writeAndReadRecord(avroSchema, avroRecord, tempDir);

        assertThat(readRecord).usingRecursiveComparison().isEqualTo(avroRecord);
    }

    @Test
    public void testMapTypes(@TempDir final java.nio.file.Path tempDir) throws Exception {
        final Schema recordInMapSchema = record("RecordInMap").fields()
            .requiredString("name")
            .requiredInt("value")
            .endRecord();

        final Schema intKeyMapEntrySchema = record("IntKeyMapEntry").fields()
            .requiredInt("key")
            .requiredString("value")
            .endRecord();

        final Schema avroSchema = record("MapRecord").fields()
            .name("map_of_ints").type().map().values().intType().noDefault()
            .name("map_of_strings").type().map().values().stringType().noDefault()
            .name("map_of_bytes").type().map().values().bytesType().noDefault()
            .name("map_of_timestamps").type().map()
                .values(timestampMillis().addToSchema(builder().longType())).noDefault()
            .name("map_of_decimals").type().map()
                .values(decimal(10, 2).addToSchema(builder().bytesType())).noDefault()
            .name("empty_map").type().map().values().stringType().noDefault()
            .name("map_of_maps").type().map().values().map().values().intType().noDefault()
            .name("map_of_records").type().map().values(recordInMapSchema).noDefault()
            .name("map_with_int_keys").type().array().items(intKeyMapEntrySchema).noDefault()
            .endRecord();

        final GenericRecord avroRecord = new GenericData.Record(avroSchema);

        final Map<String, Integer> intMap = new HashMap<>();
        intMap.put("one", 1);
        intMap.put("two", 2);
        avroRecord.put("map_of_ints", intMap);

        final Map<String, String> stringMap = new HashMap<>();
        stringMap.put("greeting", "hello");
        stringMap.put("farewell", "goodbye");
        avroRecord.put("map_of_strings", stringMap);

        final Map<String, ByteBuffer> bytesMap = new HashMap<>();
        bytesMap.put("bytes1", ByteBuffer.wrap(new byte[] {1, 2, 3}));
        bytesMap.put("bytes2", ByteBuffer.wrap(new byte[] {4, 5, 6}));
        avroRecord.put("map_of_bytes", bytesMap);

        final Map<String, Long> timestampMap = new HashMap<>();
        timestampMap.put("now", 1625077800000L);
        timestampMap.put("later", 1625077900000L);
        avroRecord.put("map_of_timestamps", timestampMap);

        final Map<String, ByteBuffer> decimalMap = new HashMap<>();
        decimalMap.put("small", decimalToBytes("123.45"));
        decimalMap.put("large", decimalToBytes("9876.54"));
        avroRecord.put("map_of_decimals", decimalMap);

        avroRecord.put("empty_map", Collections.emptyMap());

        final Map<String, Map<String, Integer>> mapOfMaps = new HashMap<>();
        final Map<String, Integer> innerMap = new HashMap<>();
        innerMap.put("a", 1);
        innerMap.put("b", 2);
        mapOfMaps.put("inner", innerMap);
        avroRecord.put("map_of_maps", mapOfMaps);

        final Map<String, GenericRecord> mapOfRecords = new HashMap<>();
        final GenericRecord rec = new GenericData.Record(recordInMapSchema);
        rec.put("name", "record1");
        rec.put("value", 101);
        mapOfRecords.put("first", rec);
        avroRecord.put("map_of_records", mapOfRecords);


        final List<GenericRecord> intKeyMap = new ArrayList<>();
        final GenericRecord entry = new GenericData.Record(intKeyMapEntrySchema);
        entry.put("key", 1);
        entry.put("value", "one");
        intKeyMap.add(entry);
        avroRecord.put("map_with_int_keys", intKeyMap);

        final GenericRecord readRecord = writeAndReadRecord(avroSchema, avroRecord, tempDir);

        assertThat(readRecord).usingRecursiveComparison().isEqualTo(avroRecord);
    }

    private static Stream<Arguments> nullUnionTestCases() {
        return Stream.of(
            Arguments.of(Schema.Type.INT, 42),
            Arguments.of(Schema.Type.STRING, "hello world"),
            Arguments.of(Schema.Type.BYTES, ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5}))
        );
    }

    /**
     * Iceberg supports only unions with NULL that are becoming optional in Parquet.
     */
    @ParameterizedTest
    @MethodSource("nullUnionTestCases")
    public void testBasicUnionTypes(final Schema.Type type, final Object value,
                                    @TempDir final java.nio.file.Path tempDir) throws Exception {
        final Schema valueSchema = Schema.create(type);
        final Schema unionSchema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), valueSchema));

        final Schema avroSchema = record("UnionRecord").fields()
            .name("union_field").type(unionSchema).noDefault()
            .endRecord();

        final GenericRecord avroRecord = new GenericData.Record(avroSchema);
        avroRecord.put("union_field", value);

        final GenericRecord readRecord = writeAndReadRecord(avroSchema, avroRecord, tempDir);

        assertThat(readRecord).usingRecursiveComparison().isEqualTo(avroRecord);
    }

    /**
     * Test for complex union types including nested records, arrays, maps, fixed, enums, and unions.
     */
    @Test
    public void testComplexUnionTypes(@TempDir final java.nio.file.Path tempDir) throws Exception {
        final Schema recordInUnionSchema = record("RecordInUnion").fields()
            .requiredString("name")
            .requiredInt("value")
            .endRecord();

        final Schema nestedRecordSchema = record("NestedRecordWithUnion").fields()
            .name("nested_union_field").type().unionOf().nullType().and().stringType().endUnion().nullDefault()
            .endRecord();

        final Schema recordUnionSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), recordInUnionSchema);
        final Schema arraySchema = Schema.createArray(Schema.create(Schema.Type.INT));
        final Schema arrayUnionSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), arraySchema);
        final Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.STRING));
        final Schema mapUnionSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), mapSchema);

        final Schema fixedSchema = Schema.createFixed("fixed_8", null, null, 8);
        final Schema fixedUnionSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), fixedSchema);

        final Schema enumSchema = Schema.createEnum("Color", null, null, Arrays.asList("RED", "GREEN", "BLUE"));
        final Schema enumUnionSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), enumSchema);

        final Schema nestedRecordUnionSchema = Schema.createUnion(Schema.create(Schema.Type.NULL), nestedRecordSchema);

        final Schema avroSchema = record("ComplexUnionRecord").fields()
            .name("union_null_record").type(recordUnionSchema).noDefault()
            .name("union_null_array").type(arrayUnionSchema).noDefault()
            .name("union_null_map").type(mapUnionSchema).noDefault()
            .name("union_null_fixed").type(fixedUnionSchema).noDefault()
            .name("union_null_enum").type(enumUnionSchema).noDefault()
            .name("union_null_nested_record").type(nestedRecordUnionSchema).noDefault()
            .endRecord();

        final GenericRecord avroRecord = new GenericData.Record(avroSchema);

        final GenericRecord recordInUnion = new GenericData.Record(recordInUnionSchema);
        recordInUnion.put("name", "test record");
        recordInUnion.put("value", 100);
        avroRecord.put("union_null_record", recordInUnion);

        avroRecord.put("union_null_array", Arrays.asList(1, 2, 3));

        final Map<String, String> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", "value2");
        avroRecord.put("union_null_map", map);

        avroRecord.put("union_null_fixed", new GenericData.Fixed(fixedSchema, new byte[] {1, 2, 3, 4, 5, 6, 7, 8}));
        avroRecord.put("union_null_enum", new GenericData.EnumSymbol(enumSchema, "GREEN"));

        final GenericRecord nestedRecord = new GenericData.Record(nestedRecordSchema);
        nestedRecord.put("nested_union_field", "nested union value");
        avroRecord.put("union_null_nested_record", nestedRecord);

        final GenericRecord readRecord = writeAndReadRecord(avroSchema, avroRecord, tempDir);

        assertThat(readRecord).usingRecursiveComparison().isEqualTo(avroRecord);
    }
}
