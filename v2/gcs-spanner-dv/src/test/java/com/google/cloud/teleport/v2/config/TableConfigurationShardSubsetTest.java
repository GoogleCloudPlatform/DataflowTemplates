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

import com.google.cloud.teleport.v2.options.GCSSpannerDVOptions;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests for the shard-subsetting configuration: {@code --shardIds} and {@code spannerQuery}. */
public class TableConfigurationShardSubsetTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private GCSSpannerDVOptions options;

  @Before
  public void setUp() {
    options = PipelineOptionsFactory.create().as(GCSSpannerDVOptions.class);
    options.setGcsInputDirectory(null);
  }

  private String writeTableConfigFile(String json) throws IOException {
    File tableConfigFile = tempFolder.newFile();
    try (FileWriter writer = new FileWriter(tableConfigFile)) {
      writer.write(json);
    }
    return tableConfigFile.getAbsolutePath();
  }

  // P1 (also: TableConfiguration.empty() has no shards and no queries).
  @Test
  public void testShardIdsUnsetDisablesShardSubsetting() {
    TableConfiguration config = TableConfiguration.parseFromOptions(options);

    assertFalse(config.hasShardFilter());
    assertTrue(config.getShardIds().isEmpty());
    assertFalse(config.hasSpannerQueries());
    assertTrue(config.getSpannerQueries().isEmpty());

    TableConfiguration empty = TableConfiguration.empty();
    assertFalse(empty.hasShardFilter());
    assertTrue(empty.getShardIds().isEmpty());
    assertFalse(empty.hasSpannerQueries());
    assertTrue(empty.getSpannerQueries().isEmpty());
  }

  // P2
  @Test
  public void testShardIdsParsedTrimmedAndEmptyEntriesSkipped() {
    options.setShardIds(" a, b ,,c ");

    TableConfiguration config = TableConfiguration.parseFromOptions(options);

    assertTrue(config.hasShardFilter());
    assertEquals(Arrays.asList("a", "b", "c"), new ArrayList<>(config.getShardIds()));
  }

  // P2 (extra): de-duplicated, first-occurrence order, unmodifiable (D-010, PD-20).
  @Test
  public void testShardIdsDeduplicatedInFirstOccurrenceOrder() {
    options.setShardIds("b,a,b");

    TableConfiguration config = TableConfiguration.parseFromOptions(options);

    assertTrue(config.hasShardFilter());
    assertEquals(Arrays.asList("b", "a"), new ArrayList<>(config.getShardIds()));
    assertThrows(UnsupportedOperationException.class, () -> config.getShardIds().add("c"));
  }

  // P2 (extra): a value that parses to an empty list means "off" (Q-4 -> D-007).
  @Test
  public void testShardIdsOnlySeparatorsDisablesShardSubsetting() {
    options.setShardIds(" , ,");

    TableConfiguration config = TableConfiguration.parseFromOptions(options);

    assertFalse(config.hasShardFilter());
    assertTrue(config.getShardIds().isEmpty());
  }

  // P3
  @Test
  public void testTableConfigFileSpannerQueryAvailableForSourceTable() throws IOException {
    String query = "SELECT * FROM T WHERE id < 5";
    options.setTableConfigurationFilePath(
        writeTableConfigFile(
            "{\"tableNames\":[\"T\"],\"optionalConfigurations\":{\"T\":{\"spannerQuery\":\""
                + query
                + "\"}}}"));

    TableConfiguration config = TableConfiguration.parseFromOptions(options);

    assertTrue(config.hasSpannerQueries());
    assertEquals(Collections.singletonMap("T", query), config.getSpannerQueries());
    assertEquals(new HashSet<>(Arrays.asList("T")), config.getSourceTables());
  }

  // P4: every blank query is named in one IllegalArgumentException (PD-2).
  @Test
  public void testTableConfigFileBlankSpannerQueryFailsNamingTable() throws IOException {
    options.setTableConfigurationFilePath(
        writeTableConfigFile(
            "{\"tableNames\":[\"T1\",\"T2\"],\"optionalConfigurations\":{"
                + "\"T1\":{\"spannerQuery\":\"   \"},"
                + "\"T2\":{\"spannerQuery\":\"\"}}}"));

    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class, () -> TableConfiguration.parseFromOptions(options));
    assertTrue(thrown.getMessage(), thrown.getMessage().contains("T1"));
    assertTrue(thrown.getMessage(), thrown.getMessage().contains("T2"));
  }

  // P4 (extra): absent or null query means "not configured" (Q-3 -> D-002).
  @Test
  public void testTableConfigFileAbsentOrNullSpannerQueryIsNotConfigured() throws IOException {
    options.setTableConfigurationFilePath(
        writeTableConfigFile(
            "{\"tableNames\":[\"T\",\"U\"],\"optionalConfigurations\":{"
                + "\"T\":{},"
                + "\"U\":{\"spannerQuery\":null}}}"));

    TableConfiguration config = TableConfiguration.parseFromOptions(options);

    assertFalse(config.hasSpannerQueries());
    assertTrue(config.getSpannerQueries().isEmpty());
    assertEquals(new HashSet<>(Arrays.asList("T", "U")), config.getSourceTables());
  }

  // P5
  @Test
  public void testTableConfigFileWithoutOptionalConfigurationsParsesAsToday() throws IOException {
    options.setTableConfigurationFilePath(writeTableConfigFile("{\"tableNames\":[\"A\",\"B\"]}"));

    TableConfiguration config = TableConfiguration.parseFromOptions(options);

    assertTrue(config.hasTableFilters());
    assertEquals(new HashSet<>(Arrays.asList("A", "B")), config.getSourceTables());
    assertFalse(config.hasSpannerQueries());
    assertTrue(config.getSpannerQueries().isEmpty());
    assertFalse(config.hasShardFilter());
  }

  // P6
  @Test
  public void testTablesWithShardIdsAllowedAndNoSpannerQuery() {
    options.setTables("A");
    options.setShardIds("s1");

    TableConfiguration config = TableConfiguration.parseFromOptions(options);

    assertEquals(Collections.singleton("A"), config.getSourceTables());
    assertTrue(config.hasShardFilter());
    assertEquals(Collections.singletonList("s1"), new ArrayList<>(config.getShardIds()));
    assertFalse(config.hasSpannerQueries());
    assertTrue(config.getSpannerQueries().isEmpty());
  }

  // P3/P6 (extra): shard list and query map compose.
  @Test
  public void testShardIdsWithTableConfigFile() throws IOException {
    String query = "SELECT * FROM Orders WHERE shard IN ('s1', 's2')";
    options.setShardIds("s1,s2");
    options.setTableConfigurationFilePath(
        writeTableConfigFile(
            "{\"tableNames\":[\"Orders\",\"Customers\"],\"optionalConfigurations\":{"
                + "\"Orders\":{\"spannerQuery\":\""
                + query
                + "\"}}}"));

    TableConfiguration config = TableConfiguration.parseFromOptions(options);

    assertTrue(config.hasShardFilter());
    assertEquals(Arrays.asList("s1", "s2"), new ArrayList<>(config.getShardIds()));
    assertTrue(config.hasSpannerQueries());
    Map<String, String> queries = config.getSpannerQueries();
    assertEquals(Collections.singletonMap("Orders", query), queries);
    assertThrows(UnsupportedOperationException.class, () -> queries.put("Customers", "SELECT 1"));
    assertEquals(new HashSet<>(Arrays.asList("Orders", "Customers")), config.getSourceTables());
  }
}
