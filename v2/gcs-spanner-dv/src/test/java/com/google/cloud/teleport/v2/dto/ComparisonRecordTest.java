package com.google.cloud.teleport.v2.dto;

import static org.junit.Assert.assertEquals;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Objects;
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
    assertEquals("dc907235a1cefae97f1bc87d84cc5175", record.getHash());
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

  @Test
  public void testHashCollision_StringBoundaries() {
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
  public void testHashCollision_TypeDifference() {
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
  public void testHash_NullVsEmptyString() {
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
  public void testHash_DateFormatting() {
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
  }
}
