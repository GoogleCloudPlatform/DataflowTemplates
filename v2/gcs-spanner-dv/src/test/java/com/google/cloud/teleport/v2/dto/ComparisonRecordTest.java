package com.google.cloud.teleport.v2.dto;

import static org.junit.Assert.assertEquals;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ComparisonRecordTest {

  //Spanner struct tests
  @Test
  public void testFromSpannerStruct_String() {
    Struct struct = Struct.newBuilder()
        .set("col1").to(Value.string("test_value"))
        .set("__tableName__").to(Value.string("test_table"))
        .build();

    ComparisonRecord record = ComparisonRecord.fromSpannerStruct(struct, Collections.singletonList("col1"));
    assertEquals("4fbb051e701e31e4fde2e4007742ccbf", record.getHash());
    assertEquals("test_table", record.getTableName());
    assertEquals(1, record.getPrimaryKeyColumns().size());
    assertEquals("col1", record.getPrimaryKeyColumns().get(0).getColName());
    assertEquals("test_value", record.getPrimaryKeyColumns().get(0).getColValue());
  }

  @Test
  public void testFromSpannerStruct_AllTypes() {
    Struct struct = Struct.newBuilder()
        .set("col_string").to("string_val")
        .set("col_int64").to(12345L)
        .set("col_float64").to(12.345)
        .set("col_bool").to(true)
        .set("col_numeric").to(BigDecimal.valueOf(100.50))
        .set("col_date").to(Date.fromYearMonthDay(2023, 10, 1))
        .set("col_bytes").to(ByteArray.copyFrom("bytes".getBytes()))
        .set("__tableName__").to(Value.string("test_table"))
        .build();

    ComparisonRecord record = ComparisonRecord.fromSpannerStruct(struct, Collections.emptyList());
    assertEquals("e891aa28102bc896303fc5773e7e6457", record.getHash());
    assertEquals("test_table", record.getTableName());
    Assert.assertTrue(record.getPrimaryKeyColumns().isEmpty());
  }

  @Test
  public void testFromSpannerStruct_Nulls() {
    Struct struct = Struct.newBuilder()
        .set("col_string").to(Value.string(null))
        .set("col_int64").to(Value.int64(null))
        .set("__tableName__").to(Value.string("test_table"))
        .build();

    ComparisonRecord record = ComparisonRecord.fromSpannerStruct(struct, Collections.emptyList());
    assertEquals("b9ba3d31be9a5bfec628ffea0f2dacc2", record.getHash());
    assertEquals("test_table", record.getTableName());
    Assert.assertTrue(record.getPrimaryKeyColumns().isEmpty());
  }

  @Test
  public void testFromSpannerStruct_MultiplePKs() {
    Struct struct = Struct.newBuilder()
        .set("pk1").to("val1")
        .set("pk2").to(123)
        .set("col_other").to("other")
        .set("__tableName__").to(Value.string("table_multi_pk"))
        .build();

    // Pass PKs in specific order
    ComparisonRecord record = ComparisonRecord.fromSpannerStruct(struct, java.util.Arrays.asList("pk1", "pk2"));

    assertEquals("table_multi_pk", record.getTableName());
    assertEquals(2, record.getPrimaryKeyColumns().size());

    // Check first PK
    assertEquals("pk1", record.getPrimaryKeyColumns().get(0).getColName());
    assertEquals("val1", record.getPrimaryKeyColumns().get(0).getColValue());

    // Check second PK
    assertEquals("pk2", record.getPrimaryKeyColumns().get(1).getColName());
    assertEquals("123", record.getPrimaryKeyColumns().get(1).getColValue());
  }

  @Test
  public void testFromSpannerStruct_StringBoundaries() {
    // "ab", "c" vs "a", "bc"
    // In naive concatenation, these produce "abc" and collide.
    // In our hasher, they should be distinct due to length prefixing.
    Struct struct1 = Struct.newBuilder()
        .set("col1").to("ab")
        .set("col2").to("c")
        .set("__tableName__").to("t")
        .build();
    ComparisonRecord record1 = ComparisonRecord.fromSpannerStruct(struct1, Collections.emptyList());

    Struct struct2 = Struct.newBuilder()
        .set("col1").to("a")
        .set("col2").to("bc")
        .set("__tableName__").to("t")
        .build();
    ComparisonRecord record2 = ComparisonRecord.fromSpannerStruct(struct2, Collections.emptyList());

    Assert.assertNotEquals(record1.getHash(), record2.getHash());
  }

  @Test
  public void testFromSpannerStruct_TypeDifference() {
    // String "123" vs Int64 123
    Struct structString = Struct.newBuilder()
        .set("col1").to("123")
        .set("__tableName__").to("t")
        .build();
    ComparisonRecord recordString = ComparisonRecord.fromSpannerStruct(structString, Collections.emptyList());

    Struct structInt = Struct.newBuilder()
        .set("col1").to(123L)
        .set("__tableName__").to("t")
        .build();
    ComparisonRecord recordInt = ComparisonRecord.fromSpannerStruct(structInt, Collections.emptyList());

    Assert.assertNotEquals(recordString.getHash(), recordInt.getHash());
  }

  @Test
  public void testFromSpannerStruct_NullVsEmptyString() {
    // Null vs Empty String
    // We handle null with a 0 byte, and non-null with a 1 byte.
    Struct structNull = Struct.newBuilder()
        .set("col1").to(Value.string(null))
        .set("__tableName__").to("t")
        .build();
    ComparisonRecord recordNull = ComparisonRecord.fromSpannerStruct(structNull, Collections.emptyList());

    Struct structEmpty = Struct.newBuilder()
        .set("col1").to("")
        .set("__tableName__").to("t")
        .build();
    ComparisonRecord recordEmpty = ComparisonRecord.fromSpannerStruct(structEmpty, Collections.emptyList());

    Assert.assertNotEquals(recordNull.getHash(), recordEmpty.getHash());
  }

  @Test
  public void testFromSpannerStruct_DateFormatting() {
    // Verify ISO-8601 formatting consistency
    Struct struct1 = Struct.newBuilder()
        .set("col1").to(Date.fromYearMonthDay(2023, 1, 9))
        .set("__tableName__").to("t")
        .build();
    ComparisonRecord record1 = ComparisonRecord.fromSpannerStruct(struct1, Collections.emptyList());

    Struct struct2 = Struct.newBuilder()
        .set("col1").to(Date.fromYearMonthDay(2023, 1, 9))
        .set("__tableName__").to("t")
        .build();
    ComparisonRecord record2 = ComparisonRecord.fromSpannerStruct(struct2, Collections.emptyList());

    Assert.assertEquals(record1.getHash(), record2.getHash());

    // Different date
    Struct struct3 = Struct.newBuilder()
        .set("col1").to(Date.fromYearMonthDay(2023, 1, 10))
        .set("__tableName__").to("t")
        .build();
    ComparisonRecord record3 = ComparisonRecord.fromSpannerStruct(struct3, Collections.emptyList());
    Assert.assertNotEquals(record1.getHash(), record3.getHash());
    Assert.assertNotEquals(record1.getHash(), record3.getHash());
  }

  @Test
  public void testFromSpannerStruct_PrimaryKeyPresence() {
    Struct struct = Struct.newBuilder()
        .set("pk1").to("val1")
        .set("col1").to("data")
        .set("__tableName__").to("t")
        .build();

    ComparisonRecord record = ComparisonRecord.fromSpannerStruct(struct, Collections.singletonList("pk1"));

    Assert.assertEquals(1, record.getPrimaryKeyColumns().size());
    Assert.assertEquals("pk1", record.getPrimaryKeyColumns().get(0).getColName());
    Assert.assertEquals("val1", record.getPrimaryKeyColumns().get(0).getColValue());
  }

  //Avro record tests

  @Test
  public void testFromAvroRecord_String() {
    Schema payloadSchema = SchemaBuilder.record("payload").fields()
        .requiredString("col1")
        .endRecord();
    GenericRecord payload = new GenericRecordBuilder(payloadSchema)
        .set("col1", "test_value")
        .build();

    ComparisonRecord record = createAvroComparisonRecord("test_table", payload, Collections.singletonList("col1"));
    assertEquals("4fbb051e701e31e4fde2e4007742ccbf", record.getHash());
    assertEquals("test_table", record.getTableName());
    assertEquals(1, record.getPrimaryKeyColumns().size());
    assertEquals("col1", record.getPrimaryKeyColumns().get(0).getColName());
    assertEquals("test_value", record.getPrimaryKeyColumns().get(0).getColValue());
  }

  @Test
  public void testFromAvroRecord_Nulls() {
    Schema payloadSchema = SchemaBuilder.record("payload").fields()
        .optionalString("col_string")
        .optionalLong("col_int64")
        .endRecord();

    GenericRecord payload = new GenericRecordBuilder(payloadSchema)
        .set("col_string", null)
        .set("col_int64", null)
        .build();

    ComparisonRecord record = createAvroComparisonRecord("test_table", payload, Collections.emptyList());
    assertEquals("b9ba3d31be9a5bfec628ffea0f2dacc2", record.getHash());
    assertEquals("test_table", record.getTableName());
    Assert.assertTrue(record.getPrimaryKeyColumns().isEmpty());
  }

  @Test
  public void testFromAvroRecord_MultiplePKs() {
    Schema payloadSchema = SchemaBuilder.record("payload").fields()
        .requiredString("pk1")
        .requiredLong("pk2")
        .requiredString("col_other")
        .endRecord();

    GenericRecord payload = new GenericRecordBuilder(payloadSchema)
        .set("pk1", "val1")
        .set("pk2", 123L)
        .set("col_other", "other")
        .build();

    ComparisonRecord record = createAvroComparisonRecord("table_multi_pk", payload,
        java.util.Arrays.asList("pk1", "pk2"));

    assertEquals("table_multi_pk", record.getTableName());
    assertEquals(2, record.getPrimaryKeyColumns().size());

    assertEquals("pk1", record.getPrimaryKeyColumns().get(0).getColName());
    assertEquals("val1", record.getPrimaryKeyColumns().get(0).getColValue());

    assertEquals("pk2", record.getPrimaryKeyColumns().get(1).getColName());
    assertEquals("123", record.getPrimaryKeyColumns().get(1).getColValue());
  }

  @Test
  public void testFromAvroRecord_HashCollision_StringBoundaries() {
    Schema payloadSchema = SchemaBuilder.record("payload").fields()
        .requiredString("col1")
        .requiredString("col2")
        .endRecord();

    GenericRecord payload1 = new GenericRecordBuilder(payloadSchema)
        .set("col1", "ab")
        .set("col2", "c")
        .build();
    ComparisonRecord record1 = createAvroComparisonRecord("t", payload1, Collections.emptyList());

    GenericRecord payload2 = new GenericRecordBuilder(payloadSchema)
        .set("col1", "a")
        .set("col2", "bc")
        .build();
    ComparisonRecord record2 = createAvroComparisonRecord("t", payload2, Collections.emptyList());

    Assert.assertNotEquals(record1.getHash(), record2.getHash());
  }

  @Test
  public void testFromAvroRecord_HashCollision_TypeDifference() {
    // String "123" vs Long 123
    Schema stringSchema = SchemaBuilder.record("payload").fields().requiredString("col1").endRecord();
    GenericRecord payloadString = new GenericRecordBuilder(stringSchema).set("col1", "123").build();
    ComparisonRecord recordString = createAvroComparisonRecord("t", payloadString, Collections.emptyList());

    Schema longSchema = SchemaBuilder.record("payload").fields().requiredLong("col1").endRecord();
    GenericRecord payloadInt = new GenericRecordBuilder(longSchema).set("col1", 123L).build();
    ComparisonRecord recordInt = createAvroComparisonRecord("t", payloadInt, Collections.emptyList());

    Assert.assertNotEquals(recordString.getHash(), recordInt.getHash());
  }

  @Test
  public void testFromAvroRecord_FieldOrderDoesNotMatter() {
    // Schema 1: col1, col2
    Schema schema1 = SchemaBuilder.record("payload").fields()
        .requiredString("col1")
        .requiredString("col2")
        .endRecord();
    GenericRecord payload1 = new GenericRecordBuilder(schema1)
        .set("col1", "val1")
        .set("col2", "val2")
        .build();
    ComparisonRecord record1 = createAvroComparisonRecord("t", payload1, Collections.emptyList());

    // Schema 2: col2, col1 (Same data, different order)
    Schema schema2 = SchemaBuilder.record("payload").fields()
        .requiredString("col2")
        .requiredString("col1")
        .endRecord();
    GenericRecord payload2 = new GenericRecordBuilder(schema2)
        .set("col2", "val2")
        .set("col1", "val1")
        .build();
    ComparisonRecord record2 = createAvroComparisonRecord("t", payload2, Collections.emptyList());
    Assert.assertEquals(record1.getHash(), record2.getHash());
  }

  @Test
  public void testFromAvroRecord_PrimaryKeyPresence() {
    Schema payloadSchema = SchemaBuilder.record("payload").fields()
        .requiredString("pk1")
        .requiredString("col1")
        .endRecord();
    GenericRecord payload = new GenericRecordBuilder(payloadSchema)
        .set("pk1", "val1")
        .set("col1", "data")
        .build();

    ComparisonRecord record = createAvroComparisonRecord("t", payload, Collections.singletonList("pk1"));

    Assert.assertEquals(1, record.getPrimaryKeyColumns().size());
    Assert.assertEquals("pk1", record.getPrimaryKeyColumns().get(0).getColName());
    Assert.assertEquals("val1", record.getPrimaryKeyColumns().get(0).getColValue());
  }

  //Equivalence tests
  @Test
  public void testEquivalenceOfSpannerAndAvroRecord_AllPrimitiveTypes() {
    // Tests primitive types that map cleanly between Avro and Spanner
    Schema payloadSchema = SchemaBuilder.record("payload").fields()
        .requiredString("col_string")
        .requiredLong("col_int64")
        .requiredDouble("col_float64")
        .requiredBoolean("col_bool")
        .requiredBytes("col_bytes")
        .endRecord();

    GenericRecord payload = new GenericRecordBuilder(payloadSchema)
        .set("col_string", "string_val")
        .set("col_int64", 12345L)
        .set("col_float64", 12.345)
        .set("col_bool", true)
        .set("col_bytes", "bytes".getBytes())
        .build();

    ComparisonRecord sourceRecord = createAvroComparisonRecord("test_table", payload, Collections.emptyList());

    // We expect this to match Spanner hash if Spanner had ONLY these fields.
    // Let's verify by constructing Spanner struct with ONLY these fields.
    Struct spannerStruct = Struct.newBuilder()
        .set("col_string").to("string_val")
        .set("col_int64").to(12345L)
        .set("col_float64").to(12.345)
        .set("col_bool").to(true)
        .set("col_bytes").to(ByteArray.copyFrom("bytes".getBytes()))
        .set("__tableName__").to("test_table")
        .build();

    ComparisonRecord spannerRecord = ComparisonRecord.fromSpannerStruct(spannerStruct, Collections.emptyList());
    assertEquals(spannerRecord.getHash(), sourceRecord.getHash());
  }

  @Test
  public void testEquivalenceOfSpannerAndAvroRecord_DifferentColumnOrder() {
      // Spanner: {col1="a", col2="b"}
      Struct spannerStruct = Struct.newBuilder()
              .set("col1").to("a")
              .set("col2").to("b")
              .set("__tableName__").to("test_table")
              .build();

      // Avro: {col2="b", col1="a"}
      Schema payloadSchema = SchemaBuilder.record("payload").fields()
              .requiredString("col2")
              .requiredString("col1")
              .endRecord();
      GenericRecord payload = new GenericRecordBuilder(payloadSchema)
              .set("col2", "b")
              .set("col1", "a")
              .build();

      ComparisonRecord spannerRecord = ComparisonRecord.fromSpannerStruct(spannerStruct, Collections.emptyList());
      ComparisonRecord avroRecord = createAvroComparisonRecord("test_table", payload, Collections.emptyList());

      assertEquals(spannerRecord.getHash(), avroRecord.getHash());
  }

  @Test
  public void testEquivalenceOfSpannerAndAvroRecord_NullValues() {
      // Spanner with nulls
      Struct spannerStruct = Struct.newBuilder()
              .set("col_string").to(Value.string(null))
              .set("col_int64").to(Value.int64(null))
              .set("__tableName__").to("test_table")
              .build();

      // Avro with nulls
      Schema payloadSchema = SchemaBuilder.record("payload").fields()
              .optionalString("col_string")
              .optionalLong("col_int64")
              .endRecord();
      GenericRecord payload = new GenericRecordBuilder(payloadSchema)
              .set("col_string", null)
              .set("col_int64", null)
              .build();

      ComparisonRecord spannerRecord = ComparisonRecord.fromSpannerStruct(spannerStruct, Collections.emptyList());
      ComparisonRecord avroRecord = createAvroComparisonRecord("test_table", payload, Collections.emptyList());

      assertEquals(spannerRecord.getHash(), avroRecord.getHash());
  }

  @Test
  public void testEquivalenceOfSpannerAndAvroRecord_Bytes() {
      byte[] byteVal = "some-bytes".getBytes();
      Struct spannerStruct = Struct.newBuilder()
              .set("col_bytes").to(ByteArray.copyFrom(byteVal))
              .set("__tableName__").to("test_table")
              .build();

      Schema payloadSchema = SchemaBuilder.record("payload").fields()
              .requiredBytes("col_bytes")
              .endRecord();
      GenericRecord payload = new GenericRecordBuilder(payloadSchema)
              .set("col_bytes", java.nio.ByteBuffer.wrap(byteVal))
              .build();

      ComparisonRecord spannerRecord = ComparisonRecord.fromSpannerStruct(spannerStruct, Collections.emptyList());
      ComparisonRecord avroRecord = createAvroComparisonRecord("test_table", payload, Collections.emptyList());

      assertEquals(spannerRecord.getHash(), avroRecord.getHash());
  }

  @Test
  public void testEquivalenceOfSpannerAndAvroRecord_WithPKs() {
      // Avro
      Schema payloadSchema = SchemaBuilder.record("payload").fields()
              .requiredString("pk1")
              .requiredString("col1")
              .endRecord();
      GenericRecord payload = new GenericRecordBuilder(payloadSchema)
              .set("pk1", "val1")
              .set("col1", "data")
              .build();
      ComparisonRecord avroRecord = createAvroComparisonRecord("t", payload, Collections.singletonList("pk1"));

      // Spanner
      Struct struct = Struct.newBuilder()
              .set("pk1").to("val1")
              .set("col1").to("data")
              .set("__tableName__").to("t")
              .build();
      ComparisonRecord spannerRecord = ComparisonRecord.fromSpannerStruct(struct, Collections.singletonList("pk1"));

      // Check individual fields first for debugging clarity
      Assert.assertEquals(avroRecord.getHash(), spannerRecord.getHash());
      Assert.assertEquals(avroRecord.getTableName(), spannerRecord.getTableName());
      Assert.assertEquals(avroRecord.getPrimaryKeyColumns(), spannerRecord.getPrimaryKeyColumns());

      // Check full equality
      Assert.assertEquals(avroRecord, spannerRecord);
  }

  //Serialization test
  @Test
  public void testSerialization() throws Exception {
    ComparisonRecord record = ComparisonRecord.builder()
        .setTableName("test_table")
        .setPrimaryKeyColumns(Collections.emptyList())
        .setHash("some_hash_123")
        .build();

    ComparisonRecord cloned = CoderUtils.clone(
        SchemaCoder.of(
            Objects.requireNonNull(new AutoValueSchema()
                .schemaFor(TypeDescriptor.of(ComparisonRecord.class))),
            TypeDescriptor.of(ComparisonRecord.class),
            new AutoValueSchema()
                .toRowFunction(TypeDescriptor.of(ComparisonRecord.class)),
            new AutoValueSchema()
                .fromRowFunction(TypeDescriptor.of(ComparisonRecord.class))),
        record);

    assertEquals(record, cloned);
    Assert.assertNotNull(cloned);
    assertEquals(record.getHash(), cloned.getHash());
  }

  // Helper
  private ComparisonRecord createAvroComparisonRecord(String tableName, GenericRecord payload,
      java.util.List<String> primaryKeys) {
    Schema schema = SchemaBuilder.record("SourceRowWithMetadata").fields()
        .requiredString("tableName")
        .optionalString("shardId")
        .name("primaryKeys").type().array().items().stringType().noDefault()
        .name("payload").type(payload.getSchema()).noDefault()
        .endRecord();

    return ComparisonRecord.fromAvroRecord(new GenericRecordBuilder(schema)
        .set("tableName", tableName)
        .set("primaryKeys", primaryKeys)
        .set("payload", payload)
        .build());
  }
}
