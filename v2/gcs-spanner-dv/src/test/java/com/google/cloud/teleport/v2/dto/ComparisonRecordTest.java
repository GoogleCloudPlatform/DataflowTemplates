package com.google.cloud.teleport.v2.dto;

import static org.junit.Assert.assertEquals;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import java.math.BigDecimal;
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
        .build();

    ComparisonRecord record = ComparisonRecord.fromSpannerStruct(struct);
    assertEquals("8fc7948a4710187bda119d5f18e378d9", record.getHash());
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
        .build();

    ComparisonRecord record = ComparisonRecord.fromSpannerStruct(struct);
    assertEquals("dee12649f61da69a8ae3d6319cb35af0", record.getHash());
  }

  @Test
  public void testFromSpannerStruct_Nulls() {
    Struct struct = Struct.newBuilder()
        .set("col_string").to(Value.string(null))
        .set("col_int64").to(Value.int64(null))
        .build();

    ComparisonRecord record = ComparisonRecord.fromSpannerStruct(struct);
    //This hash is returned for an empty string. This is never expected since Spanner requires
    //a non-null PK for every row.
    assertEquals("00000000000000000000000000000000", record.getHash());
  }

  @Test
  public void testSerialization() throws Exception {
    ComparisonRecord record = ComparisonRecord.builder()
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
}
