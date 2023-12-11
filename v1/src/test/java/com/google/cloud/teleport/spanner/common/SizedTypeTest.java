package com.google.cloud.teleport.spanner.common;

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.spanner.common.Type.StructField;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

public class SizedTypeTest {

  @Test
  public void testStruct() {
    SizedType type = SizedType.parseSpannerType(
        "STRUCT<a BOOL, b ARRAY<STRUCT<c STRING, d ARRAY<FLOAT64>>, e STRUCT<f STRUCT<g INT64>>>",
        Dialect.GOOGLE_STANDARD_SQL);
    assertEquals(type.type, Type.struct(ImmutableList.of(StructField.of("a", Type.bool()),
        StructField.of("b", Type.array(Type.struct(
            ImmutableList.of(StructField.of("c", Type.string()),
                StructField.of("d", Type.array(Type.float64())))))), StructField.of("e",
            Type.struct(ImmutableList.of(StructField.of("f",
                Type.struct(ImmutableList.of(StructField.of("g", Type.int64()))))))))));
git 
    assertEquals(SizedType.typeString(type.type, null),
        "STRUCT<a BOOL, b ARRAY<STRUCT<c STRING, d ARRAY<FLOAT64>>, e STRUCT<f STRUCT<g INT64>>>");
  }
}
