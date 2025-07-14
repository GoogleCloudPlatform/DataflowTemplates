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
import com.google.cloud.teleport.v2.spanner.ddl.annotations.cassandra.CassandraType;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.common.collect.ImmutableList;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SchemaFileOverridesBasedMapperTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private SchemaFileOverridesBasedMapper mapper;
  private Ddl ddl;
  private Path schemaOverridesFile;

  @Before
  public void setup() throws IOException {
    // Define the Spanner DDL (same as StringOverrides test for consistency)
    ddl =
        Ddl.builder(Dialect.GOOGLE_STANDARD_SQL)
            .createTable("t_spanner_alpha")
            .column("c_spanner_alpha_id")
            .int64()
            .notNull()
            .endColumn()
            .column("c_spanner_alpha_val")
            .string()
            .max()
            .endColumn()
            .column("c_alpha_data")
            .bytes()
            .max()
            .endColumn()
            .primaryKey()
            .asc("c_spanner_alpha_id")
            .end()
            .endTable()
            .createTable("t_source_beta")
            .column("c_beta_id")
            .string()
            .size(36)
            .notNull()
            .endColumn()
            .column("c_spanner_beta_val")
            .string()
            .max()
            .endColumn()
            .column("c_beta_notes")
            .string()
            .max()
            .columnOptions(ImmutableList.of("CASSANDRA_TYPE=\"text\""))
            .endColumn()
            .primaryKey()
            .asc("c_beta_id")
            .end()
            .endTable()
            .createTable("t_spanner_gamma")
            .column("c_gamma_id")
            .int64()
            .notNull()
            .endColumn()
            .column("c_spanner_gamma_desc")
            .string()
            .max()
            .endColumn()
            .primaryKey()
            .asc("c_gamma_id")
            .end()
            .endTable()
            .build();

    // Create a temporary schema overrides file
    schemaOverridesFile = temporaryFolder.newFile("schema_overrides.json").toPath();
    String overridesJsonContent =
        "{\n"
            + "  \"renamedTables\": {\n"
            + "    \"t_source_alpha\": \"t_spanner_alpha\",\n"
            + "    \"t_source_gamma\": \"t_spanner_gamma\"\n"
            + "  },\n"
            + "  \"renamedColumns\": {\n"
            + "    \"t_source_alpha\": {\n"
            + "      \"c_alpha_id\": \"c_spanner_alpha_id\",\n"
            + "      \"c_alpha_val\": \"c_spanner_alpha_val\"\n"
            + "    },\n"
            + "    \"t_source_beta\": {\n"
            + "      \"c_beta_val\": \"c_spanner_beta_val\"\n"
            + "    },\n"
            + "    \"t_source_gamma\": {\n"
            + "      \"c_gamma_desc\": \"c_spanner_gamma_desc\"\n"
            + "    }\n"
            + "  }\n"
            + "}";

    try (BufferedWriter writer = Files.newBufferedWriter(schemaOverridesFile)) {
      writer.write(overridesJsonContent);
    }

    mapper = new SchemaFileOverridesBasedMapper(schemaOverridesFile.toString(), ddl);
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
    assertEquals("t_source_beta", mapper.getSourceTableName("", "t_source_beta"));
    assertEquals("t_source_gamma", mapper.getSourceTableName("", "t_spanner_gamma"));

    assertThrows(
        NoSuchElementException.class, () -> mapper.getSourceTableName("", "non_existent_sp_table"));
  }

  @Test
  public void testGetSpannerTableName() throws IOException {
    assertEquals("t_spanner_alpha", mapper.getSpannerTableName("", "t_source_alpha"));
    assertEquals("t_source_beta", mapper.getSpannerTableName("", "t_source_beta"));
    assertEquals("t_spanner_gamma", mapper.getSpannerTableName("", "t_source_gamma"));

    // Create a faulty mapper with an override to a non-existent Spanner table
    String faultyOverridesContent =
        "{\n"
            + "  \"renamedTables\": {\n"
            + "    \"t_faulty_source\": \"t_non_existent_sp\"\n"
            + "  }\n"
            + "}";
    Path faultyFile = temporaryFolder.newFile("faulty_overrides.json").toPath();
    try (BufferedWriter writer = Files.newBufferedWriter(faultyFile)) {
      writer.write(faultyOverridesContent);
    }
    SchemaFileOverridesBasedMapper faultyMapper =
        new SchemaFileOverridesBasedMapper(faultyFile.toString(), ddl);
    assertThrows(
        NoSuchElementException.class,
        () -> faultyMapper.getSpannerTableName("", "t_faulty_source"));

    assertThrows(
        NoSuchElementException.class,
        () -> mapper.getSpannerTableName("", "non_existent_src_table"));
  }

  @Test
  public void testGetSpannerColumnName() throws IOException {
    assertEquals(
        "c_spanner_alpha_id",
        mapper.getSpannerColumnName("", "t_source_alpha", "c_alpha_id")); // Renamed
    assertEquals(
        "c_alpha_data",
        mapper.getSpannerColumnName("", "t_source_alpha", "c_alpha_data")); // Not renamed
    assertEquals(
        "c_spanner_beta_val",
        mapper.getSpannerColumnName("", "t_source_beta", "c_beta_val")); // Renamed
    assertEquals(
        "c_beta_id", mapper.getSpannerColumnName("", "t_source_beta", "c_beta_id")); // Not renamed

    // Faulty override: column override maps to a Spanner column not in DDL
    String faultyColOverridesContent =
        "{\n"
            + "  \"renamedColumns\": {\n"
            + "    \"t_source_beta\": {\n"
            + "      \"c_beta_id\": \"c_non_existent_sp_col\"\n"
            + "    }\n"
            + "  }\n"
            + "}";
    Path faultyColFile = temporaryFolder.newFile("faulty_col_overrides.json").toPath();
    try (BufferedWriter writer = Files.newBufferedWriter(faultyColFile)) {
      writer.write(faultyColOverridesContent);
    }
    SchemaFileOverridesBasedMapper faultyMapper =
        new SchemaFileOverridesBasedMapper(faultyColFile.toString(), ddl);
    assertThrows(
        NoSuchElementException.class,
        () -> faultyMapper.getSpannerColumnName("", "t_source_beta", "c_beta_id"));

    assertThrows(
        NoSuchElementException.class,
        () -> mapper.getSpannerColumnName("", "non_existent_src_table", "any_col"));
    assertThrows(
        NoSuchElementException.class,
        () -> mapper.getSpannerColumnName("", "t_source_alpha", "non_existent_src_col"));
  }

  @Test
  public void testGetSourceColumnName() {
    assertEquals(
        "c_alpha_id", mapper.getSourceColumnName("", "t_spanner_alpha", "c_spanner_alpha_id"));
    assertEquals("c_alpha_data", mapper.getSourceColumnName("", "t_spanner_alpha", "c_alpha_data"));
    assertEquals(
        "c_beta_val", mapper.getSourceColumnName("", "t_source_beta", "c_spanner_beta_val"));
    assertEquals("c_beta_id", mapper.getSourceColumnName("", "t_source_beta", "c_beta_id"));

    assertThrows(
        NoSuchElementException.class,
        () -> mapper.getSourceColumnName("", "non_existent_sp_table", "any_col"));
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
  }

  @Test
  public void testCassandraAnnotations() {
    assertEquals(
        mapper
            .getSpannerColumnCassandraAnnotations("", "t_source_beta", "c_spanner_beta_val")
            .cassandraType()
            .getKind(),
        CassandraType.Kind.NONE);
    assertEquals(
        mapper
            .getSpannerColumnCassandraAnnotations("", "t_source_beta", "c_beta_notes")
            .cassandraType()
            .getKind(),
        CassandraType.Kind.PRIMITIVE);
  }

  @Test
  public void testGetSpannerColumns() {
    List<String> expectedAlpha =
        Arrays.asList("c_spanner_alpha_id", "c_spanner_alpha_val", "c_alpha_data");
    List<String> actualAlpha = mapper.getSpannerColumns("", "t_spanner_alpha");
    Collections.sort(expectedAlpha);
    Collections.sort(actualAlpha);
    assertEquals(expectedAlpha, actualAlpha);

    assertThrows(
        NoSuchElementException.class, () -> mapper.getSpannerColumns("", "non_existent_table"));
  }

  @Test
  public void testGetShardIdColumnName() {
    assertNull(mapper.getShardIdColumnName("", "t_spanner_alpha"));
  }

  @Test
  public void testGetSyntheticPrimaryKeyColName() {
    assertNull(mapper.getSyntheticPrimaryKeyColName("", "t_spanner_alpha"));
  }

  @Test
  public void testColExistsAtSource() {
    assertTrue(mapper.colExistsAtSource("", "t_spanner_alpha", "c_spanner_alpha_id"));
    assertTrue(mapper.colExistsAtSource("", "t_spanner_alpha", "c_alpha_data"));
    assertFalse(mapper.colExistsAtSource("", "t_spanner_alpha", "non_existent_sp_col"));
    assertFalse(mapper.colExistsAtSource("", "non_existent_sp_table", "any_col"));
  }

  @Test
  public void testEmptyOverridesFile() throws IOException {
    Path emptyFile = temporaryFolder.newFile("empty_overrides.json").toPath();
    String emptyJsonContent = "{}"; // Valid JSON, but no overrides
    try (BufferedWriter writer = Files.newBufferedWriter(emptyFile)) {
      writer.write(emptyJsonContent);
    }
    SchemaFileOverridesBasedMapper emptyMapper =
        new SchemaFileOverridesBasedMapper(emptyFile.toString(), ddl);

    // Should default to same names
    assertEquals("t_spanner_alpha", emptyMapper.getSpannerTableName("", "t_spanner_alpha"));
    assertEquals(
        "c_spanner_alpha_id",
        emptyMapper.getSpannerColumnName("", "t_spanner_alpha", "c_spanner_alpha_id"));
    assertEquals("t_spanner_alpha", emptyMapper.getSourceTableName("", "t_spanner_alpha"));
    assertEquals(
        "c_spanner_alpha_id",
        emptyMapper.getSourceColumnName("", "t_spanner_alpha", "c_spanner_alpha_id"));
  }

  @Test
  public void testMalformedOverridesFile() throws IOException {
    Path malformedFile = temporaryFolder.newFile("malformed_overrides.json").toPath();
    String malformedJsonContent = "{malformed_json_content";
    try (BufferedWriter writer = Files.newBufferedWriter(malformedFile)) {
      writer.write(malformedJsonContent);
    }
    // SchemaFileOverridesParser constructor should throw IllegalArgumentException
    assertThrows(
        IllegalArgumentException.class,
        () -> new SchemaFileOverridesBasedMapper(malformedFile.toString(), ddl));
  }
}
