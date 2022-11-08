/*
 * Copyright (C) 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.bigtable;

import static org.junit.Assert.assertEquals;

import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Mutation.SetCell;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Rule;
import org.junit.Test;

/** Tests {@link BeamRowToBigtableFn}. */
public class BeamRowToBigtableFnTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void processElementWithPrimaryKey() {

    String columnFamily = "default";
    String rowKeyValue = "this-is-a-value-in-column-rowkey";
    String rowKeyColumnName = "rowkey";

    String stringValue = "hello this is a random string";
    String stringColumnName = "stringColumn";

    Schema schema =
        Schema.builder()
            .addField(
                Schema.Field.of(
                    rowKeyColumnName,
                    FieldType.STRING.withMetadata(
                        CassandraRowMapperFn.KEY_ORDER_METADATA_KEY, "0")))
            .addStringField(stringColumnName)
            .build();

    Row input = Row.withSchema(schema).addValue(rowKeyValue).addValue(stringValue).build();

    final List<Row> rows = Collections.singletonList(input);
    List<Mutation> mutations = new ArrayList<>();

    mutations.add(
        createMutation(
            columnFamily, stringColumnName, ByteString.copyFrom(Bytes.toBytes(stringValue))));

    final List<KV<ByteString, Iterable<Mutation>>> expectedBigtableRows =
        ImmutableList.of(KV.of(ByteString.copyFrom(Bytes.toBytes(rowKeyValue)), mutations));

    PCollection<KV<ByteString, Iterable<Mutation>>> bigtableRows =
        pipeline
            .apply("Create", Create.of(rows))
            .apply(
                "Transform to Bigtable",
                ParDo.of(
                    BeamRowToBigtableFn.create(
                        ValueProvider.StaticValueProvider.of("#"),
                        ValueProvider.StaticValueProvider.of("default"))));

    PAssert.that(bigtableRows).containsInAnyOrder(expectedBigtableRows);
    pipeline.run();
  }

  @Test
  public void processElementWithPrimitives() {

    String rowKeyValue = "thisistherowkeyvalue";
    String rowKeyColumnName = "rowkey";
    String columnFamily = "default";

    // Byte
    Byte byteValue = 20;
    String byteColumnName = "byteColumn";
    // ByteArray
    byte[] byteArrayValue = new byte[20];
    new Random().nextBytes(byteArrayValue);
    String byteArrayColumnName = "byteArrayColumn";
    // Int16
    short int16Value = Short.MAX_VALUE;
    String int16ColumnName = "int16Column";
    // Int32
    int int32Value = Integer.MAX_VALUE;
    String int32ColumnName = "int32Column";
    // Int64
    long int64Value = Long.MAX_VALUE;
    String int64ColumnName = "int64Column";
    // Decimal
    BigDecimal decimalValue = BigDecimal.valueOf(1000000);
    String decimalColumnName = "decimalColumn";
    // Float
    Float floatValue = Float.MAX_VALUE;
    String floatColumnName = "floatColumn";
    // Double
    double doubleValue = Double.MAX_VALUE;
    String doubleColumnName = "doubleColumn";
    // String
    String stringValue = "hello this is a random string";
    String stringColumnName = "stringColumn";
    // DateTime
    DateTime dateTimeValue = new DateTime(0);
    String dateTimeValueAsString = "1970-01-01T00:00:00.000Z";
    String dateTimeColumnName = "dateTimeColumn";
    // Boolean
    boolean booleanValue = true;
    String booleanColumnName = "booleanColumn";

    Schema schema =
        Schema.builder()
            .addField(
                Schema.Field.of(
                    rowKeyColumnName,
                    FieldType.STRING.withMetadata(
                        CassandraRowMapperFn.KEY_ORDER_METADATA_KEY, "0")))
            .addByteField(byteColumnName)
            .addByteArrayField(byteArrayColumnName)
            .addInt16Field(int16ColumnName)
            .addInt32Field(int32ColumnName)
            .addInt64Field(int64ColumnName)
            .addDecimalField(decimalColumnName)
            .addFloatField(floatColumnName)
            .addDoubleField(doubleColumnName)
            .addStringField(stringColumnName)
            .addDateTimeField(dateTimeColumnName)
            .addBooleanField(booleanColumnName)
            .build();

    Row input =
        Row.withSchema(schema)
            .addValue(rowKeyValue)
            .addValue(byteValue)
            .addValue(byteArrayValue)
            .addValue(int16Value)
            .addValue(int32Value)
            .addValue(int64Value)
            .addValue(decimalValue)
            .addValue(floatValue)
            .addValue(doubleValue)
            .addValue(stringValue)
            .addValue(dateTimeValue)
            .addValue(booleanValue)
            .build();

    final List<Row> rows = Collections.singletonList(input);
    List<Mutation> mutations = new ArrayList<>();

    byte[] byteWrapper = new byte[1];
    byteWrapper[0] = byteValue;

    mutations.add(createMutation(columnFamily, byteColumnName, ByteString.copyFrom(byteWrapper)));
    mutations.add(
        createMutation(columnFamily, byteArrayColumnName, ByteString.copyFrom(byteArrayValue)));
    mutations.add(
        createMutation(
            columnFamily, int16ColumnName, ByteString.copyFrom(Bytes.toBytes(int16Value))));
    mutations.add(
        createMutation(
            columnFamily, int32ColumnName, ByteString.copyFrom(Bytes.toBytes(int32Value))));
    mutations.add(
        createMutation(
            columnFamily, int64ColumnName, ByteString.copyFrom(Bytes.toBytes(int64Value))));
    mutations.add(
        createMutation(
            columnFamily, decimalColumnName, ByteString.copyFrom(Bytes.toBytes(decimalValue))));
    mutations.add(
        createMutation(
            columnFamily, floatColumnName, ByteString.copyFrom(Bytes.toBytes(floatValue))));
    mutations.add(
        createMutation(
            columnFamily, doubleColumnName, ByteString.copyFrom(Bytes.toBytes(doubleValue))));
    mutations.add(
        createMutation(
            columnFamily, stringColumnName, ByteString.copyFrom(Bytes.toBytes(stringValue))));
    mutations.add(
        createMutation(
            columnFamily,
            dateTimeColumnName,
            ByteString.copyFrom(Bytes.toBytes(dateTimeValueAsString))));
    mutations.add(
        createMutation(
            columnFamily, booleanColumnName, ByteString.copyFrom(Bytes.toBytes(booleanValue))));

    final List<KV<ByteString, Iterable<Mutation>>> expectedBigtableRows =
        ImmutableList.of(KV.of(ByteString.copyFrom(Bytes.toBytes(rowKeyValue)), mutations));
    PCollection<KV<ByteString, Iterable<Mutation>>> bigtableRows =
        pipeline
            .apply("Create", Create.of(rows))
            .apply(
                "Transform to Bigtable",
                ParDo.of(
                    BeamRowToBigtableFn.create(
                        ValueProvider.StaticValueProvider.of("#"),
                        ValueProvider.StaticValueProvider.of("default"))));

    PAssert.that(bigtableRows).containsInAnyOrder(expectedBigtableRows);
    pipeline.run();
  }

  private Mutation createMutation(String columnFamily, String columnName, ByteString value) {
    SetCell setcell =
        SetCell.newBuilder()
            .setFamilyName(columnFamily)
            .setColumnQualifier(ByteString.copyFrom(Bytes.toBytes(columnName)))
            .setValue(value)
            .build();

    return Mutation.newBuilder().setSetCell(setcell).build();
  }

  @Test
  public void processElementWithCompoundKey() {

    String columnFamily = "default";
    boolean rowKeyValue1 = false;
    String rowKeyColumnName1 = "rowkey1";
    Long rowKeyValue2 = Long.MAX_VALUE;
    String rowKeyColumnName2 = "rowkey2";

    String stringValue = "hello this is a random string";
    String stringColumnName = "stringColumn";

    Schema schema =
        Schema.builder()
            .addField(
                Schema.Field.of(
                    rowKeyColumnName1,
                    FieldType.BOOLEAN.withMetadata(
                        CassandraRowMapperFn.KEY_ORDER_METADATA_KEY, "0")))
            .addField(
                Schema.Field.of(
                    rowKeyColumnName2,
                    FieldType.INT64.withMetadata(CassandraRowMapperFn.KEY_ORDER_METADATA_KEY, "1")))
            .addStringField(stringColumnName)
            .build();

    Row input =
        Row.withSchema(schema)
            .addValue(rowKeyValue1)
            .addValue(rowKeyValue2)
            .addValue(stringValue)
            .build();

    final List<Row> rows = Collections.singletonList(input);
    List<Mutation> mutations = new ArrayList<>();

    mutations.add(
        createMutation(
            columnFamily, stringColumnName, ByteString.copyFrom(Bytes.toBytes(stringValue))));

    final List<KV<ByteString, Iterable<Mutation>>> expectedBigtableRows =
        ImmutableList.of(
            KV.of(ByteString.copyFrom(Bytes.toBytes("false#9223372036854775807")), mutations));
    PCollection<KV<ByteString, Iterable<Mutation>>> bigtableRows =
        pipeline
            .apply("Create", Create.of(rows))
            .apply(
                "Transform to Bigtable",
                ParDo.of(
                    BeamRowToBigtableFn.create(
                        ValueProvider.StaticValueProvider.of("#"),
                        ValueProvider.StaticValueProvider.of("default"))));

    PAssert.that(bigtableRows).containsInAnyOrder(expectedBigtableRows);
    pipeline.run();
  }

  @Test
  public void processElementWithListColumn() {
    String columnFamily = "default";
    String rowKeyValue = "rowkeyvalue";
    String rowKeyColumnName = "rowkey";

    String listColumnName = "listColumnName";
    List<String> listValue = new ArrayList<>();
    listValue.add("first");
    listValue.add("second");
    listValue.add("third");

    Schema schema =
        Schema.builder()
            .addField(
                Schema.Field.of(
                    rowKeyColumnName,
                    FieldType.STRING.withMetadata(
                        CassandraRowMapperFn.KEY_ORDER_METADATA_KEY, "0")))
            .addField(Schema.Field.of(listColumnName, FieldType.array(FieldType.STRING)))
            .build();

    Row input = Row.withSchema(schema).addValue(rowKeyValue).addValue(listValue).build();

    final List<Row> rows = Collections.singletonList(input);
    List<Mutation> mutations = new ArrayList<>();

    mutations.add(
        createMutation(
            columnFamily,
            "listColumnName[0]",
            ByteString.copyFrom(Bytes.toBytes(listValue.get(0)))));
    mutations.add(
        createMutation(
            columnFamily,
            "listColumnName[1]",
            ByteString.copyFrom(Bytes.toBytes(listValue.get(1)))));
    mutations.add(
        createMutation(
            columnFamily,
            "listColumnName[2]",
            ByteString.copyFrom(Bytes.toBytes(listValue.get(2)))));

    final List<KV<ByteString, Iterable<Mutation>>> expectedBigtableRows =
        ImmutableList.of(KV.of(ByteString.copyFrom(Bytes.toBytes("rowkeyvalue")), mutations));
    PCollection<KV<ByteString, Iterable<Mutation>>> bigtableRows =
        pipeline
            .apply("Create", Create.of(rows))
            .apply(
                "Transform to Bigtable",
                ParDo.of(
                    BeamRowToBigtableFn.create(
                        ValueProvider.StaticValueProvider.of("#"),
                        ValueProvider.StaticValueProvider.of("default"))));

    PAssert.that(bigtableRows).containsInAnyOrder(expectedBigtableRows);
    pipeline.run();
  }

  @Test
  public void processElementWithMapColumn() {
    String columnFamily = "default";
    String rowKeyValue = "rowkeyvalue";
    String rowKeyColumnName = "rowkey";

    String listColumnName = "mapColumnName";
    Map<Integer, String> mapValue = new HashMap<>();
    mapValue.put(0, "first");
    mapValue.put(1, "second");
    mapValue.put(2, "third");

    Schema schema =
        Schema.builder()
            .addField(
                Schema.Field.of(
                    rowKeyColumnName,
                    FieldType.STRING.withMetadata(
                        CassandraRowMapperFn.KEY_ORDER_METADATA_KEY, "0")))
            .addField(
                Schema.Field.of(listColumnName, FieldType.map(FieldType.INT32, FieldType.STRING)))
            .build();

    Row input = Row.withSchema(schema).addValue(rowKeyValue).addValue(mapValue).build();

    final List<Row> rows = Collections.singletonList(input);
    List<Mutation> mutations = new ArrayList<>();

    mutations.add(
        createMutation(
            columnFamily, "mapColumnName[0].key", ByteString.copyFrom(Bytes.toBytes(0))));
    mutations.add(
        createMutation(
            columnFamily,
            "mapColumnName[0].value",
            ByteString.copyFrom(Bytes.toBytes(mapValue.get(0)))));
    mutations.add(
        createMutation(
            columnFamily, "mapColumnName[1].key", ByteString.copyFrom(Bytes.toBytes(1))));
    mutations.add(
        createMutation(
            columnFamily,
            "mapColumnName[1].value",
            ByteString.copyFrom(Bytes.toBytes(mapValue.get(1)))));
    mutations.add(
        createMutation(
            columnFamily, "mapColumnName[2].key", ByteString.copyFrom(Bytes.toBytes(2))));
    mutations.add(
        createMutation(
            columnFamily,
            "mapColumnName[2].value",
            ByteString.copyFrom(Bytes.toBytes(mapValue.get(2)))));

    final List<KV<ByteString, Iterable<Mutation>>> expectedBigtableRows =
        ImmutableList.of(KV.of(ByteString.copyFrom(Bytes.toBytes("rowkeyvalue")), mutations));
    PCollection<KV<ByteString, Iterable<Mutation>>> bigtableRows =
        pipeline
            .apply("Create", Create.of(rows))
            .apply(
                "Transform to Bigtable",
                ParDo.of(
                    BeamRowToBigtableFn.create(
                        ValueProvider.StaticValueProvider.of("#"),
                        ValueProvider.StaticValueProvider.of("default"))));

    PAssert.that(bigtableRows).containsInAnyOrder(expectedBigtableRows);
    pipeline.run();
  }

  @Test
  public void processElementWithSplitLargeRows() {
    String columnFamily = "default";
    String rowKeyValue = "rowkeyvalue";
    String rowKeyColumnName = "rowkey";

    // Int32
    int int32Value = Integer.MAX_VALUE;
    String int32ColumnName = "int32Column";

    String listColumnName = "listColumnName";
    List<String> listValue = new ArrayList<>();
    listValue.add("first");
    listValue.add("second");
    listValue.add("third");

    String mapColumnName = "mapColumnName";
    Map<Integer, String> mapValue = new HashMap<>();
    mapValue.put(0, "first");
    mapValue.put(1, "second");
    mapValue.put(2, "third");

    Schema schema =
        Schema.builder()
            .addField(
                Schema.Field.of(
                    rowKeyColumnName,
                    FieldType.STRING.withMetadata(
                        CassandraRowMapperFn.KEY_ORDER_METADATA_KEY, "0")))
            .addInt32Field(int32ColumnName)
            .addField(
                Schema.Field.of(mapColumnName, FieldType.map(FieldType.INT32, FieldType.STRING)))
            .addField(Schema.Field.of(listColumnName, FieldType.array(FieldType.STRING)))
            .build();
    Row input =
        Row.withSchema(schema)
            .addValue(rowKeyValue)
            .addValue(int32Value)
            .addValue(mapValue)
            .addValues(listValue)
            .build();

    final List<Row> rows = Collections.singletonList(input);

    // Setup the pipeline
    PCollection<KV<ByteString, Iterable<Mutation>>> bigtableRows =
        pipeline
            .apply("Create", Create.of(rows))
            .apply(
                "Transform to Bigtable",
                ParDo.of(
                    BeamRowToBigtableFn.createWithSplitLargeRows(
                        ValueProvider.StaticValueProvider.of("#"),
                        ValueProvider.StaticValueProvider.of("default"),
                        StaticValueProvider.of(true),
                        4)));

    // Setup the expected values and match with returned values.
    List<Mutation> mutations1 = new ArrayList<>();

    mutations1.add(
        createMutation(
            columnFamily, int32ColumnName, ByteString.copyFrom(Bytes.toBytes(int32Value))));
    mutations1.add(
        createMutation(
            columnFamily, "mapColumnName[0].key", ByteString.copyFrom(Bytes.toBytes(0))));
    mutations1.add(
        createMutation(
            columnFamily,
            "mapColumnName[0].value",
            ByteString.copyFrom(Bytes.toBytes(mapValue.get(0)))));
    mutations1.add(
        createMutation(
            columnFamily, "mapColumnName[1].key", ByteString.copyFrom(Bytes.toBytes(1))));
    List<Mutation> mutations2 = new ArrayList<>();
    mutations2.add(
        createMutation(
            columnFamily,
            "mapColumnName[1].value",
            ByteString.copyFrom(Bytes.toBytes(mapValue.get(1)))));
    mutations2.add(
        createMutation(
            columnFamily, "mapColumnName[2].key", ByteString.copyFrom(Bytes.toBytes(2))));
    mutations2.add(
        createMutation(
            columnFamily,
            "mapColumnName[2].value",
            ByteString.copyFrom(Bytes.toBytes(mapValue.get(2)))));
    mutations2.add(
        createMutation(
            columnFamily,
            "listColumnName[0]",
            ByteString.copyFrom(Bytes.toBytes(listValue.get(0)))));
    List<Mutation> mutations3 = new ArrayList<>();
    mutations3.add(
        createMutation(
            columnFamily,
            "listColumnName[1]",
            ByteString.copyFrom(Bytes.toBytes(listValue.get(1)))));
    mutations3.add(
        createMutation(
            columnFamily,
            "listColumnName[2]",
            ByteString.copyFrom(Bytes.toBytes(listValue.get(2)))));

    final List<KV<ByteString, Iterable<Mutation>>> expectedBigtableRows =
        ImmutableList.of(
            KV.of(ByteString.copyFrom(Bytes.toBytes("rowkeyvalue")), mutations1),
            KV.of(ByteString.copyFrom(Bytes.toBytes("rowkeyvalue")), mutations2),
            KV.of(ByteString.copyFrom(Bytes.toBytes("rowkeyvalue")), mutations3));

    PAssert.that(bigtableRows).containsInAnyOrder(expectedBigtableRows);

    // Run the pipeline
    pipeline.run();
  }

  @Test
  public void processElementNullColumn() {
    String columnFamily = "default";
    String rowKeyValue = "rowkeyvalue";
    String rowKeyColumnName = "rowkey";

    String nullColumnName = "nullColumnName";
    String nullValue = null;

    Schema schema =
        Schema.builder()
            .addField(
                Schema.Field.of(
                    rowKeyColumnName,
                    FieldType.STRING.withMetadata(
                        CassandraRowMapperFn.KEY_ORDER_METADATA_KEY, "0")))
            .addNullableField(nullColumnName, FieldType.STRING)
            .build();

    Row input = Row.withSchema(schema).addValue(rowKeyValue).addValue(nullValue).build();

    final List<Row> rows = Collections.singletonList(input);
    List<Mutation> mutations = new ArrayList<>();

    mutations.add(createMutation(columnFamily, nullColumnName, ByteString.EMPTY));

    final List<KV<ByteString, Iterable<Mutation>>> expectedBigtableRows =
        ImmutableList.of(KV.of(ByteString.copyFrom(Bytes.toBytes("rowkeyvalue")), mutations));
    PCollection<KV<ByteString, Iterable<Mutation>>> bigtableRows =
        pipeline
            .apply("Create", Create.of(rows))
            .apply(
                "Transform to Bigtable",
                ParDo.of(
                    BeamRowToBigtableFn.create(
                        ValueProvider.StaticValueProvider.of("#"),
                        ValueProvider.StaticValueProvider.of("default"))));

    PAssert.that(bigtableRows).containsInAnyOrder(expectedBigtableRows);
    pipeline.run();
  }

  @Test
  public void primitiveBYTEFieldToString() {
    byte b = 20;
    assertEquals("FA==", BeamRowToBigtableFn.primitiveFieldToString(TypeName.BYTE, b));
  }

  @Test
  public void primitiveINT16FieldToString() {
    assertEquals("200", BeamRowToBigtableFn.primitiveFieldToString(TypeName.INT16, 200));
  }

  @Test
  public void primitiveINT32FieldToString() {
    assertEquals("200", BeamRowToBigtableFn.primitiveFieldToString(TypeName.INT32, 200));
  }

  @Test
  public void primitiveINT64FieldToString() {
    assertEquals("200", BeamRowToBigtableFn.primitiveFieldToString(TypeName.INT64, 200));
  }

  @Test
  public void primitiveDECIMALFieldToString() {
    assertEquals("1337.37", BeamRowToBigtableFn.primitiveFieldToString(TypeName.DECIMAL, 1337.37));
  }

  @Test
  public void primitiveFLOATFieldToString() {
    assertEquals("1337.37", BeamRowToBigtableFn.primitiveFieldToString(TypeName.FLOAT, 1337.37));
  }

  @Test
  public void primitiveDOUBLEFieldToString() {
    assertEquals("1337.37", BeamRowToBigtableFn.primitiveFieldToString(TypeName.DOUBLE, 1337.37));
  }

  @Test
  public void primitiveSTRINGFieldToString() {
    assertEquals(
        "Hello world!",
        BeamRowToBigtableFn.primitiveFieldToString(TypeName.STRING, "Hello world!"));
  }

  @Test
  public void primitiveDATETIMEFieldToString() {
    assertEquals(
        "1970-01-01T00:00:00.000Z",
        BeamRowToBigtableFn.primitiveFieldToString(
            TypeName.DATETIME, new DateTime(0).withZone(DateTimeZone.UTC)));
  }

  @Test
  public void primitiveBOOLEANFieldToString() {
    assertEquals("true", BeamRowToBigtableFn.primitiveFieldToString(TypeName.BOOLEAN, true));
  }

  @Test
  public void primitiveBYTESFieldToString() {
    byte[] bytes = "Man".getBytes();
    assertEquals("TWFu", BeamRowToBigtableFn.primitiveFieldToString(TypeName.BYTES, bytes));
  }
}
