/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.spanner.type;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TypeTest {
  @Test
  public void testToString() {
    // Google Standard SQL primitive types
    assertEquals("BOOL", Type.bool().toString());
    assertEquals("INT64", Type.int64().toString());
    assertEquals("NUMERIC", Type.numeric().toString());
    assertEquals("FLOAT32", Type.float32().toString());
    assertEquals("FLOAT64", Type.float64().toString());
    assertEquals("STRING", Type.string().toString());
    assertEquals("JSON", Type.json().toString());
    assertEquals("TOKENLIST", Type.tokenlist().toString());
    assertEquals("BYTES", Type.bytes().toString());
    assertEquals("TIMESTAMP", Type.timestamp().toString());
    assertEquals("DATE", Type.date().toString());

    // Array types
    assertEquals("ARRAY<INT64>", Type.array(Type.int64()).toString());
    assertEquals("ARRAY<STRING>", Type.array(Type.string()).toString());
    assertEquals("ARRAY<ARRAY<STRING>>", Type.array(Type.array(Type.string())).toString());

    // Struct type
    Type structType =
        Type.struct(
            Type.StructField.of("field1", Type.int64()),
            Type.StructField.of("field2", Type.string()));
    assertEquals("STRUCT<field1 INT64, field2 STRING>", structType.toString());

    // PG types
    assertEquals("PG_BOOL", Type.pgBool().toString());
    assertEquals("PG_INT8", Type.pgInt8().toString());
    assertEquals("PG_FLOAT4", Type.pgFloat4().toString());
    assertEquals("PG_FLOAT8", Type.pgFloat8().toString());
    assertEquals("PG_TEXT", Type.pgText().toString());
    assertEquals("PG_VARCHAR", Type.pgVarchar().toString());
    assertEquals("PG_NUMERIC", Type.pgNumeric().toString());
    assertEquals("PG_JSONB", Type.pgJsonb().toString());
    assertEquals("PG_BYTEA", Type.pgBytea().toString());
    assertEquals("PG_TIMESTAMPTZ", Type.pgTimestamptz().toString());
    assertEquals("PG_DATE", Type.pgDate().toString());
    assertEquals("PG_TEXT[]", Type.pgArray(Type.pgText()).toString());
    assertEquals("PG_COMMIT_TIMESTAMP", Type.pgCommitTimestamp().toString());
  }

  @Test
  public void testEqualsAndHashCode() {
    Type t1 = Type.int64();
    Type t2 = Type.int64();
    Type t3 = Type.string();

    assertTrue(t1.equals(t1));
    assertTrue(t1.equals(t2));
    assertFalse(t1.equals(t3));
    assertFalse(t1.equals(null));
    assertFalse(t1.equals("string"));

    assertEquals(t1.hashCode(), t2.hashCode());

    // Test with array types
    Type a1 = Type.array(Type.int64());
    Type a2 = Type.array(Type.int64());
    Type a3 = Type.array(Type.string());
    assertTrue(a1.equals(a2));
    assertFalse(a1.equals(a3));

    // Test with struct types
    Type s1 = Type.struct(Type.StructField.of("f1", Type.int64()));
    Type s2 = Type.struct(Type.StructField.of("f1", Type.int64()));
    Type s3 = Type.struct(Type.StructField.of("f2", Type.int64()));
    assertTrue(s1.equals(s2));
    assertFalse(s1.equals(s3));
  }

  @Test
  public void testStructFieldEqualsAndHashCode() {
    Type.StructField f1 = Type.StructField.of("f1", Type.int64());
    Type.StructField f2 = Type.StructField.of("f1", Type.int64());
    Type.StructField f3 = Type.StructField.of("f2", Type.int64());
    Type.StructField f4 = Type.StructField.of("f1", Type.string());

    assertTrue(f1.equals(f1));
    assertTrue(f1.equals(f2));
    assertFalse(f1.equals(f3));
    assertFalse(f1.equals(f4));
    assertFalse(f1.equals(null));
    assertFalse(f1.equals("string"));

    assertEquals(f1.hashCode(), f2.hashCode());
  }

  @Test
  public void testGetArrayElementType() {
    Type a1 = Type.array(Type.int64());
    assertEquals(Type.int64(), a1.getArrayElementType());

    Type a2 = Type.pgArray(Type.pgInt8());
    assertEquals(Type.pgInt8(), a2.getArrayElementType());
  }

  @Test(expected = IllegalStateException.class)
  public void testGetArrayElementType_Exception() {
    Type.int64().getArrayElementType();
  }

  @Test
  public void testPgArray_AllTypes() {
    assertEquals(Type.Code.PG_ARRAY, Type.pgArray(Type.pgBool()).getCode());
    assertEquals(Type.Code.PG_ARRAY, Type.pgArray(Type.pgInt8()).getCode());
    assertEquals(Type.Code.PG_ARRAY, Type.pgArray(Type.pgFloat4()).getCode());
    assertEquals(Type.Code.PG_ARRAY, Type.pgArray(Type.pgFloat8()).getCode());
    assertEquals(Type.Code.PG_ARRAY, Type.pgArray(Type.pgNumeric()).getCode());
    assertEquals(Type.Code.PG_ARRAY, Type.pgArray(Type.pgJsonb()).getCode());
    assertEquals(Type.Code.PG_ARRAY, Type.pgArray(Type.pgVarchar()).getCode());
    assertEquals(Type.Code.PG_ARRAY, Type.pgArray(Type.pgText()).getCode());
    assertEquals(Type.Code.PG_ARRAY, Type.pgArray(Type.pgBytea()).getCode());
    assertEquals(Type.Code.PG_ARRAY, Type.pgArray(Type.pgTimestamptz()).getCode());
    assertEquals(Type.Code.PG_ARRAY, Type.pgArray(Type.pgDate()).getCode());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPgArray_UnknownType() {
    Type.pgArray(Type.int64()); // int64 is not a PG type in this context!
  }
}
