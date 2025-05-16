/*
 * Copyright (C) 2025 Google LLC
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
package com.google.cloud.teleport.v2.spanner.migrations.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.annotations.cassandra.CassandraType.Kind;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.junit.Before;
import org.junit.Test;

public class SchemaStringOverridesBasedMapperTest {

  private SchemaStringOverridesBasedMapper mapper;
  private Ddl ddl;

  @Before
  public void setup() {
    // Define the Spanner DDL
    ddl =
        Ddl.builder(Dialect.GOOGLE_STANDARD_SQL)
            // Table 't_spanner_alpha' is renamed from 't_source_alpha'
            .createTable("t_spanner_alpha")
            .column("c_spanner_alpha_id")
            .int64()
            .notNull()
            .endColumn() // Renamed from c_alpha_id
            .column("c_spanner_alpha_val")
            .string()
            .max()
            .endColumn() // Renamed from c_alpha_val
            .column("c_alpha_data")
            .bytes()
            .max()
            .endColumn() // Not renamed
            .primaryKey()
            .asc("c_spanner_alpha_id")
            .end()
            .endTable()
            // Table 't_source_beta' is not renamed
            .createTable("t_source_beta")
            .column("c_beta_id")
            .string()
            .size(36)
            .notNull()
            .endColumn() // Not renamed
            .column("c_spanner_beta_val")
            .string()
            .max()
            .endColumn() // Renamed from c_beta_val
            .column("c_beta_notes")
            .string()
            .max()
            .columnOptions(ImmutableList.of("CASSANDRA_TYPE=\"text\""))
            .endColumn() // Not renamed, has Cassandra annotation
            .primaryKey()
            .asc("c_beta_id")
            .end()
            .endTable()
            // Table 't_spanner_gamma' is renamed from 't_source_gamma'
            // but has a column that is NOT renamed in the DDL (c_gamma_id)
            // and one that IS renamed (c_spanner_gamma_desc from c_gamma_desc)
            .createTable("t_spanner_gamma")
            .column("c_gamma_id")
            .int64()
            .notNull()
            .endColumn() // Not renamed in DDL, but table is.
            .column("c_spanner_gamma_desc")
            .string()
            .max()
            .endColumn() // Renamed from c_gamma_desc
            .primaryKey()
            .asc("c_gamma_id")
            .end()
            .endTable()
            .build();

    Map<String, String> userOptionOverrides = new HashMap<>();
    userOptionOverrides.put(
        "tableOverrides", "[{t_source_alpha, t_spanner_alpha}, {t_source_gamma, t_spanner_gamma}]");
    userOptionOverrides.put(
        "columnOverrides",
        "[{t_source_alpha.c_alpha_id, t_source_alpha.c_spanner_alpha_id},"
            + " {t_source_alpha.c_alpha_val, t_source_alpha.c_spanner_alpha_val},"
            + " {t_source_beta.c_beta_val, t_source_beta.c_spanner_beta_val},"
            + " {t_source_gamma.c_gamma_desc, t_source_gamma.c_spanner_gamma_desc}]");

    mapper = new SchemaStringOverridesBasedMapper(userOptionOverrides, ddl);
  }

  @Test
  public void testGetDialect() {
    assertEquals(Dialect.GOOGLE_STANDARD_SQL, mapper.getDialect());
  }

  @Test
  public void testGetSourceTablesToMigrate() {
    List<String> expected = Arrays.asList("t_source_alpha", "t_source_beta", "t_source_gamma");
    List<String> actual = mapper.getSourceTablesToMigrate("");
    Collections.sort(expected);
    Collections.sort(actual);
    assertEquals(expected, actual);
  }

  @Test
  public void testGetSourceTableName() {
    assertEquals("t_source_alpha", mapper.getSourceTableName("", "t_spanner_alpha"));
    assertEquals("t_source_beta", mapper.getSourceTableName("", "t_source_beta")); // No override
    assertEquals("t_source_gamma", mapper.getSourceTableName("", "t_spanner_gamma"));

    // Spanner table not in DDL
    assertThrows(
        NoSuchElementException.class, () -> mapper.getSourceTableName("", "non_existent_sp_table"));
  }

  @Test
  public void testGetSpannerTableName() {
    assertEquals("t_spanner_alpha", mapper.getSpannerTableName("", "t_source_alpha"));
    assertEquals("t_source_beta", mapper.getSpannerTableName("", "t_source_beta")); // No override
    assertEquals("t_spanner_gamma", mapper.getSpannerTableName("", "t_source_gamma"));

    // Source table with override, but target Spanner table not in DDL
    Map<String, String> faultyOverrides = new HashMap<>();
    faultyOverrides.put("tableOverrides", "[{t_faulty, t_non_existent_sp}]");
    SchemaStringOverridesBasedMapper faultyMapper =
        new SchemaStringOverridesBasedMapper(faultyOverrides, ddl);
    assertThrows(
        NoSuchElementException.class, () -> faultyMapper.getSpannerTableName("", "t_faulty"));

    // Source table with no override, and not in DDL
    assertThrows(
        NoSuchElementException.class,
        () -> mapper.getSpannerTableName("", "non_existent_src_table"));
  }

  @Test
  public void testGetSpannerColumnName() {
    // Renamed table, renamed column
    assertEquals(
        "c_spanner_alpha_id", mapper.getSpannerColumnName("", "t_source_alpha", "c_alpha_id"));
    // Renamed table, non-renamed column
    assertEquals("c_alpha_data", mapper.getSpannerColumnName("", "t_source_alpha", "c_alpha_data"));
    // Non-renamed table, renamed column
    assertEquals(
        "c_spanner_beta_val", mapper.getSpannerColumnName("", "t_source_beta", "c_beta_val"));
    // Non-renamed table, non-renamed column
    assertEquals("c_beta_id", mapper.getSpannerColumnName("", "t_source_beta", "c_beta_id"));

    // Source column whose override maps to a Spanner column not in DDL for that table
    Map<String, String> faultyColOverrides = new HashMap<>();
    faultyColOverrides.put(
        "columnOverrides", "[{t_source_beta.c_beta_id, t_source_beta.c_non_existent_sp_col}]");
    SchemaStringOverridesBasedMapper faultyMapper =
        new SchemaStringOverridesBasedMapper(faultyColOverrides, ddl);
    assertThrows(
        NoSuchElementException.class,
        () -> faultyMapper.getSpannerColumnName("", "t_source_beta", "c_beta_id"));

    // Source table not found (implicitly, as it won't map to a DDL table)
    assertThrows(
        NoSuchElementException.class,
        () -> mapper.getSpannerColumnName("", "non_existent_src_table", "any_col"));
    // Source column not found for existing table
    assertThrows(
        NoSuchElementException.class,
        () -> mapper.getSpannerColumnName("", "t_source_alpha", "non_existent_src_col"));
  }

  @Test
  public void testGetSourceColumnName() {
    // Renamed table, renamed column
    assertEquals(
        "c_alpha_id", mapper.getSourceColumnName("", "t_spanner_alpha", "c_spanner_alpha_id"));
    // Renamed table, non-renamed column (Spanner col name is same as source col name)
    assertEquals("c_alpha_data", mapper.getSourceColumnName("", "t_spanner_alpha", "c_alpha_data"));
    // Non-renamed table, renamed column
    assertEquals(
        "c_beta_val", mapper.getSourceColumnName("", "t_source_beta", "c_spanner_beta_val"));
    // Non-renamed table, non-renamed column
    assertEquals("c_beta_id", mapper.getSourceColumnName("", "t_source_beta", "c_beta_id"));

    // Spanner table not in DDL
    assertThrows(
        NoSuchElementException.class,
        () -> mapper.getSourceColumnName("", "non_existent_sp_table", "any_col"));
    // Spanner column not in DDL for existing table
    assertThrows(
        NoSuchElementException.class,
        () -> mapper.getSourceColumnName("", "t_spanner_alpha", "non_existent_sp_col"));
  }

  @Test
  public void testGetSpannerColumnType() {
    assertEquals(
        Type.int64(), mapper.getSpannerColumnType("", "t_spanner_alpha", "c_spanner_alpha_id"));
    assertEquals(
        Type.string(), mapper.getSpannerColumnType("", "t_source_beta", "c_spanner_beta_val"));
    assertEquals(Type.bytes(), mapper.getSpannerColumnType("", "t_spanner_alpha", "c_alpha_data"));

    assertThrows(
        NoSuchElementException.class,
        () -> mapper.getSpannerColumnType("", "t_spanner_alpha", "non_existent_col"));
    assertThrows(
        NoSuchElementException.class,
        () -> mapper.getSpannerColumnType("", "non_existent_table", "c_spanner_alpha_id"));
  }

  @Test
  public void testCassandraAnnotations() {
    assertEquals(
        mapper
            .getSpannerColumnCassandraAnnotations("", "t_source_beta", "c_spanner_beta_val")
            .cassandraType()
            .getKind(),
        Kind.NONE);
    assertEquals(
        mapper
            .getSpannerColumnCassandraAnnotations("", "t_source_beta", "c_beta_notes")
            .cassandraType()
            .getKind(),
        Kind.PRIMITIVE);
  }

  @Test
  public void testGetSpannerColumns() {
    List<String> expectedAlpha =
        Arrays.asList("c_spanner_alpha_id", "c_spanner_alpha_val", "c_alpha_data");
    List<String> actualAlpha = mapper.getSpannerColumns("", "t_spanner_alpha");
    Collections.sort(expectedAlpha);
    Collections.sort(actualAlpha);
    assertEquals(expectedAlpha, actualAlpha);

    List<String> expectedBeta = Arrays.asList("c_beta_id", "c_spanner_beta_val", "c_beta_notes");
    List<String> actualBeta = mapper.getSpannerColumns("", "t_source_beta");
    Collections.sort(expectedBeta);
    Collections.sort(actualBeta);
    assertEquals(expectedBeta, actualBeta);

    assertThrows(
        NoSuchElementException.class, () -> mapper.getSpannerColumns("", "non_existent_table"));
  }

  @Test
  public void testGetShardIdColumnName() {
    assertNull(mapper.getShardIdColumnName("", "t_spanner_alpha"));
    assertNull(mapper.getShardIdColumnName("", "t_source_beta"));
  }

  @Test
  public void testGetSyntheticPrimaryKeyColName() {
    assertNull(mapper.getSyntheticPrimaryKeyColName("", "t_spanner_alpha"));
    assertNull(mapper.getSyntheticPrimaryKeyColName("", "t_source_beta"));
  }

  @Test
  public void testColExistsAtSource() {
    // Renamed table, renamed column -> source exists
    assertTrue(mapper.colExistsAtSource("", "t_spanner_alpha", "c_spanner_alpha_id")); // c_alpha_id
    // Renamed table, non-renamed Spanner column -> source exists
    assertTrue(mapper.colExistsAtSource("", "t_spanner_alpha", "c_alpha_data")); // c_alpha_data
    // Non-renamed table, renamed Spanner column -> source exists
    assertTrue(mapper.colExistsAtSource("", "t_source_beta", "c_spanner_beta_val")); // c_beta_val
    // Non-renamed table, non-renamed Spanner column -> source exists
    assertTrue(mapper.colExistsAtSource("", "t_source_beta", "c_beta_id")); // c_beta_id

    // Spanner column exists in DDL, but no corresponding source override and name doesn't match a
    // conceptual source column
    // This case is tricky: if "c_spanner_only_col" is in DDL for "t_spanner_alpha",
    // getSourceColumnName would return "c_spanner_only_col".
    // colExistsAtSource depends on whether this "c_spanner_only_col" is considered a source column.
    // Given current logic (default to same name), it would be true if DDL has it.
    // To test false, the Spanner column must not exist in DDL or not be mappable.
    assertFalse(mapper.colExistsAtSource("", "t_spanner_alpha", "non_existent_sp_col"));
    assertFalse(mapper.colExistsAtSource("", "non_existent_sp_table", "any_col"));
  }
}
