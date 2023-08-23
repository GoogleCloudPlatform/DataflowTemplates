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
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.it.cassandra.CassandraResourceManager;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigtable.BigtableResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration test for {@link CassandraToBigtable}. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(CassandraToBigtable.class)
@RunWith(JUnit4.class)
public class CassandraToBigtableIT extends TemplateTestBase {

  private CassandraResourceManager cassandraResourceManager;
  private BigtableResourceManager bigtableResourceManager;

  @Before
  public void setup() throws IOException {
    cassandraResourceManager = CassandraResourceManager.builder(testName).build();
    bigtableResourceManager =
        BigtableResourceManager.builder(testName, PROJECT, credentialsProvider)
            .maybeUseStaticInstance()
            .build();
  }

  @After
  public void tearDownClass() {
    ResourceManagerUtils.cleanResources(cassandraResourceManager, bigtableResourceManager);
  }

  @Test
  public void testCassandraToBigtable() throws IOException {
    // Arrange
    String tableName = "test_table_" + RandomStringUtils.randomAlphanumeric(8);
    List<Map<String, Object>> records = new ArrayList<>();
    records.add(Map.of("id", 1, "company", "Google"));
    records.add(Map.of("id", 2, "company", "Alphabet"));

    cassandraResourceManager.executeStatement(
        "CREATE TABLE source_data ( id int PRIMARY KEY, company text )");
    cassandraResourceManager.insertDocuments("source_data", records);

    String colFamily = "names";
    bigtableResourceManager.createTable(tableName, ImmutableList.of(colFamily));

    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(testName, specPath)
            .addParameter("cassandraHosts", cassandraResourceManager.getHost())
            .addParameter("cassandraPort", String.valueOf(cassandraResourceManager.getPort()))
            .addParameter("cassandraKeyspace", cassandraResourceManager.getKeyspaceName())
            .addParameter("cassandraTable", "source_data")
            .addParameter("bigtableProjectId", PROJECT)
            .addParameter("bigtableInstanceId", bigtableResourceManager.getInstanceId())
            .addParameter("bigtableTableId", tableName)
            .addParameter("defaultColumnFamily", colFamily);

    // Act
    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    PipelineOperator.Result result = pipelineOperator().waitUntilDone(createConfig(info));

    // Assert
    assertThatResult(result).isLaunchFinished();

    List<Row> rows = bigtableResourceManager.readTable(tableName);

    // Create a map of <id, name>
    Map<String, String> values =
        rows.stream()
            .collect(
                Collectors.toMap(
                    row -> row.getKey().toStringUtf8(),
                    row -> row.getCells().get(0).getValue().toStringUtf8()));
    assertThat(values.get("1")).isEqualTo("Google");
    assertThat(values.get("2")).isEqualTo("Alphabet");
  }
}
