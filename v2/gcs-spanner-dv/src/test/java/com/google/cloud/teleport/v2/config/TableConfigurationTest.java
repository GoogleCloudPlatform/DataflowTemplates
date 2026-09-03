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
package com.google.cloud.teleport.v2.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.templates.GCSSpannerDV;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.NoSuchElementException;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TableConfigurationTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private GCSSpannerDV.Options options;
  private ISchemaMapper mockSchemaMapper;

  @Before
  public void setUp() {
    options = PipelineOptionsFactory.create().as(GCSSpannerDV.Options.class);
    mockSchemaMapper = mock(ISchemaMapper.class);
  }

  @Test
  public void testEmptyConfig() {
    TableConfiguration config = TableConfiguration.empty();
    assertFalse(config.hasFilters());
    assertTrue(config.getSourceTables().isEmpty());
    assertTrue(config.isSourceTableAllowed("any_table"));
    assertTrue(config.isSpannerTableAllowed("any_table", mockSchemaMapper));
  }

  @Test
  public void testParseFromOptionsWithTables() throws IOException {
    options.setTables("table1, table2,table3 ");

    File inputDir = tempFolder.newFolder("input");
    options.setGcsInputDirectory(inputDir.getAbsolutePath());
    new File(inputDir, "table1").mkdirs();
    new File(inputDir, "table1/data.avro").createNewFile();
    new File(inputDir, "table2").mkdirs();
    new File(inputDir, "table2/data.avro").createNewFile();
    new File(inputDir, "table3").mkdirs();
    new File(inputDir, "table3/data.avro").createNewFile();

    TableConfiguration config = TableConfiguration.parseFromOptions(options);

    assertTrue(config.hasFilters());
    assertEquals(3, config.getSourceTables().size());
    assertTrue(config.getSourceTables().contains("table1"));
    assertTrue(config.getSourceTables().contains("table2"));
    assertTrue(config.getSourceTables().contains("table3"));
    assertFalse(config.getSourceTables().contains("table4"));
  }

  @Test
  public void testParseFromOptionsWithTableListFile() throws IOException {
    File tableListFile = tempFolder.newFile("tables.json");
    try (FileWriter writer = new FileWriter(tableListFile)) {
      writer.write("{\"tableNames\": [\"tableA\", \" tableB \", \"\", \"tableC\"]}");
    }
    options.setTableListFilePath(tableListFile.getAbsolutePath());

    File inputDir = tempFolder.newFolder("input");
    options.setGcsInputDirectory(inputDir.getAbsolutePath());
    new File(inputDir, "tableA").mkdirs();
    new File(inputDir, "tableA/data.avro").createNewFile();
    new File(inputDir, "tableB").mkdirs();
    new File(inputDir, "tableB/data.avro").createNewFile();
    new File(inputDir, "tableC").mkdirs();
    new File(inputDir, "tableC/data.avro").createNewFile();

    TableConfiguration config = TableConfiguration.parseFromOptions(options);

    assertTrue(config.hasFilters());
    assertEquals(3, config.getSourceTables().size());
    assertTrue(config.getSourceTables().contains("tableA"));
    assertTrue(config.getSourceTables().contains("tableB"));
    assertTrue(config.getSourceTables().contains("tableC"));
  }

  @Test
  public void testParseFromOptionsThrowsWhenBothProvided() {
    options.setTables("table1");
    options.setTableListFilePath("gs://dummy/tables.txt");

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class, () -> TableConfiguration.parseFromOptions(options));
    assertTrue(
        thrown.getMessage().contains("Please configure only one of these parameters at a time."));
  }

  @Test
  public void testParseFromOptionsNoGcsInputDirectory() {
    options.setTables("table1,table2");
    options.setGcsInputDirectory(null);

    TableConfiguration config = TableConfiguration.parseFromOptions(options);
    assertTrue(config.hasFilters());
    assertEquals(2, config.getSourceTables().size());
  }

  @Test
  public void testIsSourceTableAllowed() {
    options.setTables("table1,table2");
    options.setGcsInputDirectory(null);
    TableConfiguration config = TableConfiguration.parseFromOptions(options);

    assertTrue(config.isSourceTableAllowed("table1"));
    assertTrue(config.isSourceTableAllowed("table2"));
    assertFalse(config.isSourceTableAllowed("table3"));
  }

  @Test
  public void testIsSpannerTableAllowed() {
    options.setTables("source_table1,source_table2");
    options.setGcsInputDirectory(null);
    TableConfiguration config = TableConfiguration.parseFromOptions(options);

    when(mockSchemaMapper.getSourceTableName("", "spanner_table1")).thenReturn("source_table1");
    when(mockSchemaMapper.getSourceTableName("", "spanner_table2")).thenReturn("source_table2");
    when(mockSchemaMapper.getSourceTableName("", "spanner_table3")).thenReturn("source_table3");

    assertTrue(config.isSpannerTableAllowed("spanner_table1", mockSchemaMapper));
    assertTrue(config.isSpannerTableAllowed("spanner_table2", mockSchemaMapper));
    assertFalse(config.isSpannerTableAllowed("spanner_table3", mockSchemaMapper));
  }

  @Test
  public void testIsSpannerTableAllowedThrowsNoSuchElementException() {
    options.setTables("source_table1");
    options.setGcsInputDirectory(null);
    TableConfiguration config = TableConfiguration.parseFromOptions(options);

    when(mockSchemaMapper.getSourceTableName(anyString(), anyString()))
        .thenThrow(new NoSuchElementException("Table not found"));

    assertFalse(config.isSpannerTableAllowed("unknown_table", mockSchemaMapper));
  }

  @Test
  public void testParseFromOptionsThrowsWhenTableListFileFailsToRead() {
    options.setTableListFilePath(tempFolder.getRoot().getAbsolutePath() + "/non_existent_file.json");

    RuntimeException thrown =
        assertThrows(RuntimeException.class, () -> TableConfiguration.parseFromOptions(options));
    assertTrue(thrown.getMessage().contains("Failed to read JSON tableListFilePath"));
  }
}
