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
}
