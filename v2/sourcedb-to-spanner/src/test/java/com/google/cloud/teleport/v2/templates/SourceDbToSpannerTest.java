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
package com.google.cloud.teleport.v2.templates;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.options.SourceDbToSpannerOptions;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchema;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceSchemaReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableReference;
import com.google.cloud.teleport.v2.source.reader.io.schema.SourceTableSchema;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.InvalidOptionsException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.IdentityMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SessionBasedMapper;
import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;
import com.google.common.io.Resources;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class SourceDbToSpannerTest {

  private Ddl spannerDdl;

  @Before
  public void setup() {
    spannerDdl =
        Ddl.builder()
            .createTable("new_cart")
            .column("new_quantity")
            .int64()
            .notNull()
            .endColumn()
            .column("new_user_id")
            .string()
            .size(10)
            .endColumn()
            .primaryKey()
            .asc("new_user_id")
            .asc("new_quantity")
            .end()
            .endTable()
            .createTable("new_people")
            .column("synth_id")
            .int64()
            .notNull()
            .endColumn()
            .column("new_name")
            .string()
            .size(10)
            .endColumn()
            .primaryKey()
            .asc("synth_id")
            .end()
            .endTable()
            .build();
  }

  @Test
  public void testCreateSpannerConfig() {
    SourceDbToSpannerOptions mockOptions =
        mock(SourceDbToSpannerOptions.class, Mockito.withSettings().serializable());
    when(mockOptions.getProjectId()).thenReturn("testProject");
    when(mockOptions.getSpannerHost()).thenReturn("testHost");
    when(mockOptions.getInstanceId()).thenReturn("testInstance");
    when(mockOptions.getDatabaseId()).thenReturn("testDatabaseId");

    SpannerConfig config = SourceDbToSpanner.createSpannerConfig(mockOptions);
    assertEquals(config.getProjectId().get(), "testProject");
    assertEquals(config.getInstanceId().get(), "testInstance");
    assertEquals(config.getDatabaseId().get(), "testDatabaseId");
  }

  @Test
  public void createIdentitySchemaMapper() {
    SourceDbToSpannerOptions mockOptions = createOptionsHelper("", "");
    ISchemaMapper schemaMapper = SourceDbToSpanner.getSchemaMapper(mockOptions, spannerDdl);
    assertTrue(schemaMapper instanceof IdentityMapper);
  }

  @Test
  public void createSessionSchemaMapper() {
    SourceDbToSpannerOptions mockOptions =
        createOptionsHelper(
            Paths.get(Resources.getResource("session-file-with-dropped-column.json").getPath())
                .toString(),
            null);
    ISchemaMapper schemaMapper = SourceDbToSpanner.getSchemaMapper(mockOptions, spannerDdl);
    assertTrue(schemaMapper instanceof SessionBasedMapper);
  }

  @Test(expected = Exception.class)
  public void createInvalidSchemaMapper_withException() {
    SourceDbToSpannerOptions mockOptions = createOptionsHelper("invalid-file", "");
    SourceDbToSpanner.getSchemaMapper(mockOptions, spannerDdl);
  }

  private SourceDbToSpannerOptions createOptionsHelper(String sessionFile, String tables) {
    SourceDbToSpannerOptions mockOptions =
        mock(SourceDbToSpannerOptions.class, Mockito.withSettings().serializable());
    when(mockOptions.getSessionFilePath()).thenReturn(sessionFile);
    when(mockOptions.getTables()).thenReturn(tables);
    return mockOptions;
  }

  @Test
  public void listTablesToMigrateIdentity() {
    SourceDbToSpannerOptions mockOptions = createOptionsHelper("", "");
    ISchemaMapper schemaMapper = SourceDbToSpanner.getSchemaMapper(mockOptions, spannerDdl);
    List<String> tables =
        SourceDbToSpanner.listTablesToMigrate(mockOptions, schemaMapper, spannerDdl);
    List<String> ddlTables =
        spannerDdl.allTables().stream().map(t -> t.name()).collect(Collectors.toList());
    assertEquals(2, tables.size());
    assertTrue(ddlTables.containsAll(tables));
  }

  @Test
  public void listTablesToMigrateIdentityOverride() {
    SourceDbToSpannerOptions mockOptions = createOptionsHelper("", "new_cart");
    ISchemaMapper schemaMapper = SourceDbToSpanner.getSchemaMapper(mockOptions, spannerDdl);
    List<String> tables =
        SourceDbToSpanner.listTablesToMigrate(mockOptions, schemaMapper, spannerDdl);
    List<String> ddlTables =
        spannerDdl.allTables().stream().map(t -> t.name()).collect(Collectors.toList());
    assertEquals(1, tables.size());
    assertTrue(ddlTables.containsAll(tables));
  }

  @Test
  public void listTablesToMigrateSession() {
    SourceDbToSpannerOptions mockOptions =
        createOptionsHelper(
            Paths.get(Resources.getResource("session-file-with-dropped-column.json").getPath())
                .toString(),
            "");
    ISchemaMapper schemaMapper = SourceDbToSpanner.getSchemaMapper(mockOptions, spannerDdl);
    List<String> tables =
        SourceDbToSpanner.listTablesToMigrate(mockOptions, schemaMapper, spannerDdl);

    assertEquals(2, tables.size());
    assertTrue(tables.contains("cart"));
    assertTrue(tables.contains("people"));
  }

  @Test
  public void listTablesToMigrateSessionOverride() {
    SourceDbToSpannerOptions mockOptions =
        createOptionsHelper(
            Paths.get(Resources.getResource("session-file-with-dropped-column.json").getPath())
                .toString(),
            "cart");
    ISchemaMapper schemaMapper = SourceDbToSpanner.getSchemaMapper(mockOptions, spannerDdl);
    List<String> tables =
        SourceDbToSpanner.listTablesToMigrate(mockOptions, schemaMapper, spannerDdl);

    assertEquals(1, tables.size());
    assertTrue(tables.contains("cart"));
  }

  @Test(expected = InvalidOptionsException.class)
  public void listTablesToMigrateSessionOverrideInvalid() {
    SourceDbToSpannerOptions mockOptions =
        createOptionsHelper(
            Paths.get(Resources.getResource("session-file-with-dropped-column.json").getPath())
                .toString(),
            "asd");
    ISchemaMapper schemaMapper = SourceDbToSpanner.getSchemaMapper(mockOptions, spannerDdl);
    List<String> tables =
        SourceDbToSpanner.listTablesToMigrate(mockOptions, schemaMapper, spannerDdl);
  }

  @Test
  public void createTableIdToRefMap() {
    SourceSchema srcSchema =
        SourceSchema.builder()
            .setSchemaReference(SourceSchemaReference.builder().setDbName("test").build())
            .addTableSchema(
                SourceTableSchema.builder()
                    .setTableName("testTable")
                    .setTableSchemaUUID("uuid1")
                    .addSourceColumnNameToSourceColumnType(
                        "col1", new SourceColumnType("int", new Long[] {}, new Long[] {}))
                    .build())
            .build();

    Map<String, SourceTableReference> tableIDToRefMap =
        SourceDbToSpanner.getTableIDToRefMap(srcSchema);
    assertEquals(tableIDToRefMap.get("uuid1").getName(), "Db.test.Table.testTable");
    assertNull(tableIDToRefMap.get("uuid2"));
  }
}
