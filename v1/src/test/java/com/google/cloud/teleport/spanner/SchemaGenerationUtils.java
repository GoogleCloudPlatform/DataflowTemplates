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
package com.google.cloud.teleport.spanner;

import com.google.cloud.teleport.spanner.common.Type;
import com.google.cloud.teleport.spanner.ddl.Ddl;

/**
 * Utility class for generating dynamic Data Definition Language (DDL) schemas for integration
 * tests.
 *
 * <p>Provides pre-configured Spanner DDL builders for both Google Standard SQL and PostgreSQL
 * dialects, including extensive testing structures such as all scalar and array data types,
 * interleaved tables, and foreign key references.
 */
public class SchemaGenerationUtils {

  /**
   * Generates comprehensive base tables using the Google Standard SQL Dialect.
   *
   * <p>Includes a root "Users" table and an interleaved "AllTYPES" table containing every supported
   * GSQL primitive and array type (e.g. string, float64, timestamp, arrays of bytes).
   *
   * @param builder The Spanner DDL builder to append the schema to.
   */
  public static void addBaseTables(Ddl.Builder builder) {
    builder
        .createTable("Users")
        .column("first_name")
        .string()
        .max()
        .endColumn()
        .column("last_name")
        .string()
        .size(5)
        .endColumn()
        .column("age")
        .int64()
        .endColumn()
        .primaryKey()
        .asc("first_name")
        .desc("last_name")
        .end()
        .endTable()
        .createTable("AllTYPES")
        .column("first_name")
        .string()
        .max()
        .endColumn()
        .column("last_name")
        .string()
        .size(5)
        .endColumn()
        .column("id")
        .int64()
        .notNull()
        .endColumn()
        .column("bool_field")
        .bool()
        .endColumn()
        .column("int64_field")
        .int64()
        .endColumn()
        .column("float32_field")
        .float32()
        .endColumn()
        .column("float64_field")
        .float64()
        .endColumn()
        .column("string_field")
        .string()
        .max()
        .endColumn()
        .column("bytes_field")
        .bytes()
        .max()
        .endColumn()
        .column("timestamp_field")
        .timestamp()
        .endColumn()
        .column("date_field")
        .date()
        .endColumn()
        .column("arr_bool_field")
        .type(Type.array(Type.bool()))
        .endColumn()
        .column("arr_int64_field")
        .type(Type.array(Type.int64()))
        .endColumn()
        .column("arr_float32_field")
        .type(Type.array(Type.float32()))
        .endColumn()
        .column("arr_float64_field")
        .type(Type.array(Type.float64()))
        .endColumn()
        .column("arr_string_field")
        .type(Type.array(Type.string()))
        .max()
        .endColumn()
        .column("arr_bytes_field")
        .type(Type.array(Type.bytes()))
        .max()
        .endColumn()
        .column("arr_timestamp_field")
        .type(Type.array(Type.timestamp()))
        .endColumn()
        .column("arr_date_field")
        .type(Type.array(Type.date()))
        .endColumn()
        .primaryKey()
        .asc("first_name")
        .desc("last_name")
        .asc("id")
        .end()
        .interleaveInParent("Users")
        .onDeleteCascade()
        .endTable();
  }

  /**
   * Generates comprehensive base tables using the PostgreSQL Dialect.
   *
   * <p>Includes a root "Users" table and an interleaved "AllTYPES" table containing every supported
   * PG primitive and array type (e.g. pgVarchar, pgFloat8, pgTimestamptz, pgBytea).
   *
   * @param builder The Spanner DDL builder to append the schema to.
   */
  public static void addPgBaseTables(Ddl.Builder builder) {
    builder
        .createTable("Users")
        .column("first_name")
        .pgVarchar()
        .max()
        .endColumn()
        .column("last_name")
        .pgVarchar()
        .size(5)
        .endColumn()
        .column("age")
        .pgInt8()
        .endColumn()
        .primaryKey()
        .asc("first_name")
        .asc("last_name")
        .end()
        .endTable()
        .createTable("AllTYPES")
        .column("id")
        .pgInt8()
        .notNull()
        .endColumn()
        .column("first_name")
        .pgVarchar()
        .max()
        .endColumn()
        .column("last_name")
        .pgVarchar()
        .size(5)
        .endColumn()
        .column("bool_field")
        .pgBool()
        .endColumn()
        .column("int_field")
        .pgInt8()
        .endColumn()
        .column("float32_field")
        .pgFloat4()
        .endColumn()
        .column("float64_field")
        .pgFloat8()
        .endColumn()
        .column("string_field")
        .pgText()
        .endColumn()
        .column("bytes_field")
        .pgBytea()
        .endColumn()
        .column("timestamp_field")
        .pgTimestamptz()
        .endColumn()
        .column("numeric_field")
        .pgNumeric()
        .endColumn()
        .column("date_field")
        .pgDate()
        .endColumn()
        .column("arr_bool_field")
        .type(Type.pgArray(Type.pgBool()))
        .endColumn()
        .column("arr_int_field")
        .type(Type.pgArray(Type.pgInt8()))
        .endColumn()
        .column("arr_float32_field")
        .type(Type.pgArray(Type.pgFloat4()))
        .endColumn()
        .column("arr_float64_field")
        .type(Type.pgArray(Type.pgFloat8()))
        .endColumn()
        .column("arr_string_field")
        .type(Type.pgArray(Type.pgVarchar()))
        .max()
        .endColumn()
        .column("arr_bytes_field")
        .type(Type.pgArray(Type.pgBytea()))
        .max()
        .endColumn()
        .column("arr_timestamp_field")
        .type(Type.pgArray(Type.pgTimestamptz()))
        .endColumn()
        .column("arr_date_field")
        .type(Type.pgArray(Type.pgDate()))
        .endColumn()
        .column("arr_numeric_field")
        .type(Type.pgArray(Type.pgNumeric()))
        .endColumn()
        .primaryKey()
        .asc("first_name")
        .asc("last_name")
        .asc("id")
        .asc("float64_field")
        .end()
        .interleaveInParent("Users")
        .onDeleteCascade()
        .endTable();
  }

  /**
   * Generates empty tables using the Google Standard SQL Dialect.
   *
   * <p>Useful for testing export/import behaviors when tables exist in the schema but contain
   * exactly zero rows.
   *
   * @param builder The Spanner DDL builder to append the schema to.
   */
  public static void addEmptyTables(Ddl.Builder builder) {
    builder
        .createTable("empty_one")
        .column("first")
        .string()
        .max()
        .endColumn()
        .column("second")
        .string()
        .size(5)
        .endColumn()
        .column("value")
        .int64()
        .endColumn()
        .primaryKey()
        .asc("first")
        .desc("second")
        .end()
        .endTable()
        .createTable("empty_two")
        .column("first")
        .string()
        .max()
        .endColumn()
        .column("second")
        .string()
        .size(5)
        .endColumn()
        .column("value")
        .int64()
        .endColumn()
        .column("another_value")
        .int64()
        .endColumn()
        .primaryKey()
        .asc("first")
        .end()
        .endTable();
  }

  /**
   * Generates empty tables using the PostgreSQL Dialect.
   *
   * <p>Useful for testing export/import behaviors when tables exist in the schema but contain
   * exactly zero rows.
   *
   * @param builder The Spanner DDL builder to append the schema to.
   */
  public static void addPgEmptyTables(Ddl.Builder builder) {
    builder
        .createTable("empty_one")
        .column("first")
        .pgVarchar()
        .max()
        .endColumn()
        .column("second")
        .pgVarchar()
        .size(5)
        .endColumn()
        .column("value")
        .pgInt8()
        .endColumn()
        .primaryKey()
        .asc("first")
        .asc("second")
        .end()
        .endTable()
        .createTable("empty_two")
        .column("first")
        .pgVarchar()
        .max()
        .endColumn()
        .column("second")
        .pgVarchar()
        .size(5)
        .endColumn()
        .column("value")
        .pgInt8()
        .endColumn()
        .column("another_value")
        .pgInt8()
        .endColumn()
        .primaryKey()
        .asc("first")
        .end()
        .endTable();
  }

  /**
   * Generates tables with complex Foreign Key constraints using the Google Standard SQL Dialect.
   *
   * <p>Includes a parent "Ref" table and an interleaved "Child" table that reference each other
   * using multiple foreign key definitions (e.g. single column, composite, non-enforced).
   *
   * @param builder The Spanner DDL builder to append the schema to.
   */
  public static void addForeignKeys(Ddl.Builder builder) {
    builder
        .createTable("Ref")
        .column("id1")
        .int64()
        .endColumn()
        .column("id2")
        .int64()
        .endColumn()
        .primaryKey()
        .asc("id1")
        .asc("id2")
        .end()
        .endTable()
        .createTable("Child")
        .column("id1")
        .int64()
        .endColumn()
        .column("id2")
        .int64()
        .endColumn()
        .column("id3")
        .int64()
        .endColumn()
        .primaryKey()
        .asc("id1")
        .asc("id2")
        .asc("id3")
        .end()
        .interleaveInParent("Ref")
        .foreignKeys(
            com.google.common.collect.ImmutableList.of(
                "ALTER TABLE `Child` ADD CONSTRAINT `fk1` FOREIGN KEY (`id1`) REFERENCES `Ref` (`id1`)",
                "ALTER TABLE `Child` ADD CONSTRAINT `fk2` FOREIGN KEY (`id2`) REFERENCES `Ref` (`id2`)",
                "ALTER TABLE `Child` ADD CONSTRAINT `fk3` FOREIGN KEY (`id2`) REFERENCES `Ref` (`id2`)",
                "ALTER TABLE `Child` ADD CONSTRAINT `fk4` FOREIGN KEY (`id2`, `id1`) REFERENCES `Ref` (`id2`, `id1`)",
                "ALTER TABLE `Child` ADD CONSTRAINT `fk5` FOREIGN KEY (`id2`) REFERENCES `Ref` (`id2`) NOT ENFORCED",
                "ALTER TABLE `Child` ADD CONSTRAINT `fk6` FOREIGN KEY (`id2`) REFERENCES `Ref` (`id2`) ENFORCED"))
        .endTable();
  }

  /**
   * Generates tables with complex Foreign Key constraints using the PostgreSQL Dialect.
   *
   * <p>Includes a parent "Ref" table and an interleaved "Child" table that reference each other
   * using multiple foreign key definitions natively formatted for PostgreSQL Spanner schemas.
   *
   * @param builder The Spanner DDL builder to append the schema to.
   */
  public static void addPgForeignKeys(Ddl.Builder builder) {
    builder
        .createTable("Ref")
        .column("id1")
        .pgInt8()
        .endColumn()
        .column("id2")
        .pgInt8()
        .endColumn()
        .primaryKey()
        .asc("id1")
        .asc("id2")
        .end()
        .endTable()
        .createTable("Child")
        .column("id1")
        .pgInt8()
        .endColumn()
        .column("id2")
        .pgInt8()
        .endColumn()
        .column("id3")
        .pgInt8()
        .endColumn()
        .primaryKey()
        .asc("id1")
        .asc("id2")
        .asc("id3")
        .end()
        .interleaveInParent("Ref")
        .foreignKeys(
            com.google.common.collect.ImmutableList.of(
                "ALTER TABLE \"Child\" ADD CONSTRAINT \"fk1\" FOREIGN KEY (\"id1\") REFERENCES"
                    + " \"Ref\" (\"id1\")",
                "ALTER TABLE \"Child\" ADD CONSTRAINT \"fk2\" FOREIGN KEY (\"id2\") REFERENCES"
                    + " \"Ref\" (\"id2\")",
                "ALTER TABLE \"Child\" ADD CONSTRAINT \"fk3\" FOREIGN KEY (\"id2\") REFERENCES"
                    + " \"Ref\" (\"id2\")",
                "ALTER TABLE \"Child\" ADD CONSTRAINT \"fk4\" FOREIGN KEY (\"id2\", \"id1\") "
                    + "REFERENCES \"Ref\" (\"id2\", \"id1\")"))
        .endTable();
  }
}
