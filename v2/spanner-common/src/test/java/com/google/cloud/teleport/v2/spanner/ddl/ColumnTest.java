/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.spanner.ddl;

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.spanner.type.Type;
import org.junit.Test;

public class ColumnTest {

  @Test
  public void testTypeString_GoogleStandardSQL() {
    assertEquals("BOOL", Column.builder().name("col").type(Type.bool()).autoBuild().typeString());
    assertEquals("INT64", Column.builder().name("col").type(Type.int64()).autoBuild().typeString());
    assertEquals(
        "FLOAT64", Column.builder().name("col").type(Type.float64()).autoBuild().typeString());
    assertEquals(
        "FLOAT32", Column.builder().name("col").type(Type.float32()).autoBuild().typeString());
    assertEquals(
        "STRING(MAX)",
        Column.builder().name("col").type(Type.string()).size(-1).autoBuild().typeString());
    assertEquals(
        "STRING(10)",
        Column.builder().name("col").type(Type.string()).size(10).autoBuild().typeString());
    assertEquals(
        "BYTES(MAX)",
        Column.builder().name("col").type(Type.bytes()).size(-1).autoBuild().typeString());
    assertEquals(
        "BYTES(20)",
        Column.builder().name("col").type(Type.bytes()).size(20).autoBuild().typeString());
    assertEquals("DATE", Column.builder().name("col").type(Type.date()).autoBuild().typeString());
    assertEquals(
        "TIMESTAMP", Column.builder().name("col").type(Type.timestamp()).autoBuild().typeString());
    assertEquals(
        "NUMERIC", Column.builder().name("col").type(Type.numeric()).autoBuild().typeString());
    assertEquals("JSON", Column.builder().name("col").type(Type.json()).autoBuild().typeString());
    assertEquals(
        "TOKENLIST", Column.builder().name("col").type(Type.tokenlist()).autoBuild().typeString());
    assertEquals("UUID", Column.builder().name("col").type(Type.uuid()).autoBuild().typeString());
    assertEquals(
        "ARRAY<INT64>",
        Column.builder().name("col").type(Type.array(Type.int64())).autoBuild().typeString());
  }

  @Test
  public void testTypeString_PostgreSQL() {
    assertEquals(
        "boolean",
        Column.builder(Dialect.POSTGRESQL)
            .name("col")
            .type(Type.pgBool())
            .autoBuild()
            .typeString());
    assertEquals(
        "bigint",
        Column.builder(Dialect.POSTGRESQL)
            .name("col")
            .type(Type.pgInt8())
            .autoBuild()
            .typeString());
    assertEquals(
        "double precision",
        Column.builder(Dialect.POSTGRESQL)
            .name("col")
            .type(Type.pgFloat8())
            .autoBuild()
            .typeString());
    assertEquals(
        "real",
        Column.builder(Dialect.POSTGRESQL)
            .name("col")
            .type(Type.pgFloat4())
            .autoBuild()
            .typeString());
    assertEquals(
        "character varying",
        Column.builder(Dialect.POSTGRESQL)
            .name("col")
            .type(Type.pgVarchar())
            .size(-1)
            .autoBuild()
            .typeString());
    assertEquals(
        "character varying(10)",
        Column.builder(Dialect.POSTGRESQL)
            .name("col")
            .type(Type.pgVarchar())
            .size(10)
            .autoBuild()
            .typeString());
    assertEquals(
        "text",
        Column.builder(Dialect.POSTGRESQL)
            .name("col")
            .type(Type.pgText())
            .autoBuild()
            .typeString());
    assertEquals(
        "bytea",
        Column.builder(Dialect.POSTGRESQL)
            .name("col")
            .type(Type.pgBytea())
            .autoBuild()
            .typeString());
    assertEquals(
        "date",
        Column.builder(Dialect.POSTGRESQL)
            .name("col")
            .type(Type.pgDate())
            .autoBuild()
            .typeString());
    assertEquals(
        "timestamp with time zone",
        Column.builder(Dialect.POSTGRESQL)
            .name("col")
            .type(Type.pgTimestamptz())
            .autoBuild()
            .typeString());
    assertEquals(
        "numeric",
        Column.builder(Dialect.POSTGRESQL)
            .name("col")
            .type(Type.pgNumeric())
            .autoBuild()
            .typeString());
    assertEquals(
        "jsonb",
        Column.builder(Dialect.POSTGRESQL)
            .name("col")
            .type(Type.pgJsonb())
            .autoBuild()
            .typeString());
    assertEquals(
        "spanner.commit_timestamp",
        Column.builder(Dialect.POSTGRESQL)
            .name("col")
            .type(Type.pgCommitTimestamp())
            .autoBuild()
            .typeString());
    assertEquals(
        "uuid",
        Column.builder(Dialect.POSTGRESQL)
            .name("col")
            .type(Type.pgUuid())
            .autoBuild()
            .typeString());
    assertEquals(
        "bigint[]",
        Column.builder(Dialect.POSTGRESQL)
            .name("col")
            .type(Type.pgArray(Type.pgInt8()))
            .autoBuild()
            .typeString());
  }

  @Test
  public void testParseSpannerType_GoogleStandardSQL() {
    assertEquals(Type.bool(), Column.builder().name("col").parseType("BOOL").autoBuild().type());
    assertEquals(Type.int64(), Column.builder().name("col").parseType("INT64").autoBuild().type());
    assertEquals(
        Type.float64(), Column.builder().name("col").parseType("FLOAT64").autoBuild().type());
    assertEquals(
        Type.float32(), Column.builder().name("col").parseType("FLOAT32").autoBuild().type());
    assertEquals(
        Type.string(), Column.builder().name("col").parseType("STRING(MAX)").autoBuild().type());
    assertEquals(
        Type.bytes(), Column.builder().name("col").parseType("BYTES(MAX)").autoBuild().type());
    assertEquals(Type.date(), Column.builder().name("col").parseType("DATE").autoBuild().type());
    assertEquals(
        Type.timestamp(), Column.builder().name("col").parseType("TIMESTAMP").autoBuild().type());
    assertEquals(
        Type.numeric(), Column.builder().name("col").parseType("NUMERIC").autoBuild().type());
    assertEquals(Type.json(), Column.builder().name("col").parseType("JSON").autoBuild().type());
    assertEquals(
        Type.tokenlist(), Column.builder().name("col").parseType("TOKENLIST").autoBuild().type());
    assertEquals(Type.uuid(), Column.builder().name("col").parseType("UUID").autoBuild().type());
    assertEquals(
        Type.array(Type.int64()),
        Column.builder().name("col").parseType("ARRAY<INT64>").autoBuild().type());
  }

  @Test
  public void testParseSpannerType_PostgreSQL() {
    assertEquals(
        Type.pgBool(),
        Column.builder(Dialect.POSTGRESQL).name("col").parseType("boolean").autoBuild().type());
    assertEquals(
        Type.pgInt8(),
        Column.builder(Dialect.POSTGRESQL).name("col").parseType("bigint").autoBuild().type());
    assertEquals(
        Type.pgFloat8(),
        Column.builder(Dialect.POSTGRESQL)
            .name("col")
            .parseType("double precision")
            .autoBuild()
            .type());
    assertEquals(
        Type.pgFloat4(),
        Column.builder(Dialect.POSTGRESQL).name("col").parseType("real").autoBuild().type());
    assertEquals(
        Type.pgText(),
        Column.builder(Dialect.POSTGRESQL).name("col").parseType("text").autoBuild().type());
    assertEquals(
        Type.pgVarchar(),
        Column.builder(Dialect.POSTGRESQL)
            .name("col")
            .parseType("character varying")
            .autoBuild()
            .type());
    assertEquals(
        Type.pgBytea(),
        Column.builder(Dialect.POSTGRESQL).name("col").parseType("bytea").autoBuild().type());
    assertEquals(
        Type.pgTimestamptz(),
        Column.builder(Dialect.POSTGRESQL)
            .name("col")
            .parseType("timestamp with time zone")
            .autoBuild()
            .type());
    assertEquals(
        Type.pgNumeric(),
        Column.builder(Dialect.POSTGRESQL).name("col").parseType("numeric").autoBuild().type());
    assertEquals(
        Type.pgJsonb(),
        Column.builder(Dialect.POSTGRESQL).name("col").parseType("jsonb").autoBuild().type());
    assertEquals(
        Type.pgDate(),
        Column.builder(Dialect.POSTGRESQL).name("col").parseType("date").autoBuild().type());
    assertEquals(
        Type.pgCommitTimestamp(),
        Column.builder(Dialect.POSTGRESQL)
            .name("col")
            .parseType("spanner.commit_timestamp")
            .autoBuild()
            .type());
    assertEquals(
        Type.pgUuid(),
        Column.builder(Dialect.POSTGRESQL).name("col").parseType("uuid").autoBuild().type());
    assertEquals(
        Type.pgArray(Type.pgInt8()),
        Column.builder(Dialect.POSTGRESQL).name("col").parseType("bigint[]").autoBuild().type());
  }

  @Test
  public void testPrettyPrint() {
    Column c = Column.builder().name("col1").type(Type.bool()).notNull(true).autoBuild();
    assertEquals("`col1`                                  BOOL NOT NULL", c.prettyPrint());

    Column c2 = Column.builder().name("col2").type(Type.string()).size(10).autoBuild();
    assertEquals("`col2`                                  STRING(10)", c2.prettyPrint());

    Column c3 =
        Column.builder().name("col3").type(Type.int64()).generatedAs("1+1").stored().autoBuild();
    assertEquals("`col3`                                  INT64 AS (1+1) STORED", c3.prettyPrint());

    Column c4 =
        Column.builder(Dialect.POSTGRESQL)
            .name("col4")
            .type(Type.pgInt8())
            .generatedAs("1+1")
            .stored()
            .autoBuild();
    assertEquals(
        "\"col4\"                                  bigint GENERATED ALWAYS AS (1+1) STORED",
        c4.prettyPrint());
  }
}
