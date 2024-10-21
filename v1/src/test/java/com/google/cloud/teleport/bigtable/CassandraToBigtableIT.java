/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.bigtable;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.gcp.bigtable.matchers.BigtableAsserts.assertThatBigtableRecords;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.junit.Assert.assertTrue;

import com.datastax.oss.driver.api.core.servererrors.AlreadyExistsException;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.cassandra.CassandraResourceManager;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ExceptionUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigtable.BigtableResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link CassandraToBigtable}. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(CassandraToBigtable.class)
@RunWith(JUnit4.class)
public class CassandraToBigtableIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraToBigtableIT.class);

  private CassandraResourceManager cassandraResourceManager;
  private BigtableResourceManager bigtableResourceManager;
  private GcsResourceManager gcsResourceManager;

  private String createWritetimeSchema(Map... entries) {

    ArrayList<String> schema = new ArrayList<>();
    for (Map entry : entries) {
      ArrayList<String> schemaRow = new ArrayList<>();
      for (Object key : entry.keySet()) {
        schemaRow.add(String.format("%s: %s", doubleQuote(key), doubleQuote(entry.get(key))));
      }

      String rowString = String.format("{%s}", String.join(", ", schemaRow));
      schema.add(rowString);
    }
    return String.join("\n", schema);
  }

  private String doubleQuote(Object string) {
    return '"' + (String) string + '"';
  }

  // Convert Instant timestamp to epoch microsecond format.
  private long toEpochMicros(Instant time) {
    return time.toEpochMilli() * 1000;
  }

  @Before
  public void setup() throws IOException {
    cassandraResourceManager = CassandraResourceManager.builder(testName).build();
    bigtableResourceManager =
        BigtableResourceManager.builder(testName, PROJECT, credentialsProvider)
            .maybeUseStaticInstance()
            .build();
    gcsResourceManager =
        GcsResourceManager.builder(artifactBucketName, getClass().getSimpleName(), credentials)
            .build();
  }

  @After
  public void tearDownClass() {
    ResourceManagerUtils.cleanResources(
        cassandraResourceManager, bigtableResourceManager, gcsResourceManager);
  }

  @Test
  public void testCassandraToBigtable() throws IOException {
    // Arrange
    String sourceTableName =
        "source_table_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();
    String tableName = "test_table_" + RandomStringUtils.randomAlphanumeric(8);
    List<Map<String, Object>> records = new ArrayList<>();
    records.add(Map.of("id", 1, "company", "Google"));
    records.add(Map.of("id", 2, "company", "Alphabet"));
    records.add(Map.of("id", 3, "company", "Acme Inc"));

    try {
      cassandraResourceManager.executeStatement(
          "CREATE TABLE " + sourceTableName + " ( id int PRIMARY KEY, company text )");
    } catch (Exception e) {
      // This might happen because DriverTimeouts are retried on the ResourceManager, but the
      // might have gone through.
      if (ExceptionUtils.containsType(e, AlreadyExistsException.class)) {
        LOG.warn("Already exists creating table {}, ignoring", e);
      } else {
        throw e;
      }
    }

    cassandraResourceManager.insertDocuments(sourceTableName, records);

    String colFamily = "names";
    bigtableResourceManager.createTable(tableName, ImmutableList.of(colFamily));

    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("cassandraHosts", cassandraResourceManager.getHost())
            .addParameter("cassandraPort", String.valueOf(cassandraResourceManager.getPort()))
            .addParameter("cassandraKeyspace", cassandraResourceManager.getKeyspaceName())
            .addParameter("cassandraTable", sourceTableName)
            .addParameter("bigtableProjectId", PROJECT)
            .addParameter("bigtableInstanceId", bigtableResourceManager.getInstanceId())
            .addParameter("bigtableTableId", tableName)
            .addParameter("defaultColumnFamily", colFamily);

    // Act
    LaunchInfo info = launchTemplate(options);
    Instant prePipelineRun = Instant.now();
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));
    Instant postPipelineRun = Instant.now();

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Row> rows = bigtableResourceManager.readTable(tableName);
    assertThat(rows).hasSize(3);
    assertThatBigtableRecords(rows, colFamily)
        .hasRecordsUnordered(
            List.of(
                Map.of("company", "Google"),
                Map.of("company", "Alphabet"),
                Map.of("company", "Acme Inc")));

    // The default writetime behavior is to insert the pipeline runtime to Bigtable.
    for (Row row : rows) {
      // Only one cell per row should be present. This timestamp is of microsecond format.
      long timestamp = row.getCells().get(0).getTimestamp();
      assertTrue(timestamp > toEpochMicros(prePipelineRun));
      assertTrue(timestamp < toEpochMicros(postPipelineRun));
    }
  }

  @Test
  public void testWritetimeReplication() throws Exception {
    // Arrange
    String sourceTableName =
        "source_table_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();
    String tableName = "test_table_" + RandomStringUtils.randomAlphanumeric(8);
    List<Map<String, Object>> records = new ArrayList<>();
    records.add(Map.of("id", 1, "company", "Writetime_Google"));
    records.add(Map.of("id", 2, "company", "Writetime_Alphabet"));
    records.add(Map.of("id", 3, "company", "Writetime_Acme Inc"));

    // Write cells into Cassandra.
    Instant preCassandraWrite = Instant.now();
    try {
      cassandraResourceManager.executeStatement(
          "CREATE TABLE " + sourceTableName + " ( id int PRIMARY KEY, company text )");
    } catch (Exception e) {
      // This might happen because DriverTimeouts are retried on the ResourceManager, but the
      // might have gone through.
      if (ExceptionUtils.containsType(e, AlreadyExistsException.class)) {
        LOG.warn("Already exists creating table {}, ignoring", e);
      } else {
        throw e;
      }
    }
    cassandraResourceManager.insertDocuments(sourceTableName, records);
    Instant postCassandraWrite = Instant.now();

    // Create Bigtable table.
    String colFamily = "names";
    bigtableResourceManager.createTable(tableName, ImmutableList.of(colFamily));

    // Create writetime schema.
    String schemaString =
        createWritetimeSchema(
            Map.of(
                "keyspace_name",
                cassandraResourceManager.getKeyspaceName(),
                "table_name",
                sourceTableName,
                "column_name",
                "id",
                "position",
                "0"),
            Map.of(
                "keyspace_name",
                cassandraResourceManager.getKeyspaceName(),
                "table_name",
                sourceTableName,
                "column_name",
                "company",
                "position",
                "-1"));
    gcsResourceManager.createArtifact("input/schema.json", schemaString);

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("cassandraHosts", cassandraResourceManager.getHost())
            .addParameter("cassandraPort", String.valueOf(cassandraResourceManager.getPort()))
            .addParameter("cassandraKeyspace", cassandraResourceManager.getKeyspaceName())
            .addParameter("cassandraTable", sourceTableName)
            .addParameter("bigtableProjectId", PROJECT)
            .addParameter("bigtableInstanceId", bigtableResourceManager.getInstanceId())
            .addParameter("bigtableTableId", tableName)
            .addParameter("defaultColumnFamily", colFamily)
            .addParameter(
                "writetimeCassandraColumnSchema",
                getGcsPath("input/schema.json", gcsResourceManager));

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    // Assert that bigtable writes propagate correctly.
    List<Row> rows = bigtableResourceManager.readTable(tableName);
    assertThat(rows).hasSize(3);
    assertThatBigtableRecords(rows, colFamily)
        .hasRecordsUnordered(
            List.of(
                Map.of("company", "Writetime_Google"),
                Map.of("company", "Writetime_Alphabet"),
                Map.of("company", "Writetime_Acme Inc")));

    // Assert that Cassandra writetimes were propagated over. We don't know what exact instant the
    // Cassandra write happened, but we can assert that it is during that timeframe.
    for (Row row : rows) {
      // Only one cell per row should be present. This timestamp is of microsecond format.
      long timestamp = row.getCells().get(0).getTimestamp();
      assertTrue(timestamp > toEpochMicros(preCassandraWrite));
      assertTrue(timestamp < toEpochMicros(postCassandraWrite));
    }
  }

  // Test setZeroTimestamp option, that sets writetime to 0 (epoch time). This is a legacy behavior.
  @Test
  public void testZeroTimestamp() throws IOException {
    // Arrange
    String sourceTableName =
        "source_table_" + RandomStringUtils.randomAlphanumeric(8).toLowerCase();
    String tableName = "test_table_" + RandomStringUtils.randomAlphanumeric(8);
    List<Map<String, Object>> records = new ArrayList<>();
    records.add(Map.of("id", 1, "company", "ZeroTimestamp_Google"));
    records.add(Map.of("id", 2, "company", "ZeroTimestamp_Alphabet"));
    records.add(Map.of("id", 3, "company", "ZeroTimestamp_Acme Inc"));

    try {
      cassandraResourceManager.executeStatement(
          "CREATE TABLE " + sourceTableName + " ( id int PRIMARY KEY, company text )");
    } catch (Exception e) {
      // This might happen because DriverTimeouts are retried on the ResourceManager, but the
      // might have gone through.
      if (ExceptionUtils.containsType(e, AlreadyExistsException.class)) {
        LOG.warn("Already exists creating table {}, ignoring", e);
      } else {
        throw e;
      }
    }

    cassandraResourceManager.insertDocuments(sourceTableName, records);

    String colFamily = "names";
    bigtableResourceManager.createTable(tableName, ImmutableList.of(colFamily));

    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("cassandraHosts", cassandraResourceManager.getHost())
            .addParameter("cassandraPort", String.valueOf(cassandraResourceManager.getPort()))
            .addParameter("cassandraKeyspace", cassandraResourceManager.getKeyspaceName())
            .addParameter("cassandraTable", sourceTableName)
            .addParameter("bigtableProjectId", PROJECT)
            .addParameter("bigtableInstanceId", bigtableResourceManager.getInstanceId())
            .addParameter("bigtableTableId", tableName)
            .addParameter("defaultColumnFamily", colFamily)
            .addParameter("setZeroTimestamp", "true");

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Row> rows = bigtableResourceManager.readTable(tableName);
    assertThat(rows).hasSize(3);
    assertThatBigtableRecords(rows, colFamily)
        .hasRecordsUnordered(
            List.of(
                Map.of("company", "ZeroTimestamp_Google"),
                Map.of("company", "ZeroTimestamp_Alphabet"),
                Map.of("company", "ZeroTimestamp_Acme Inc")));

    for (Row row : rows) {
      // Only one cell per row should be present. This timestamp is of microsecond format.
      long timestamp = row.getCells().get(0).getTimestamp();
      assertTrue(timestamp == 0);
    }
  }
}
