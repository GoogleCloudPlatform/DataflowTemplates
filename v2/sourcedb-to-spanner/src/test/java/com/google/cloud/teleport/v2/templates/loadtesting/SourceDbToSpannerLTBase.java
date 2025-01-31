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
package com.google.cloud.teleport.v2.templates.loadtesting;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.v2.source.reader.io.jdbc.iowrapper.config.SQLDialect;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.TemplateLoadTestBase;
import org.apache.beam.it.gcp.secretmanager.SecretManagerResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.StaticJDBCResource;
import org.apache.beam.it.jdbc.StaticMySQLResource;
import org.apache.beam.it.jdbc.StaticPostgresqlResource;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Resources;
import org.junit.After;

public class SourceDbToSpannerLTBase extends TemplateLoadTestBase {

  private static final String SPEC_PATH =
      "gs://dataflow-templates/latest/flex/Sourcedb_to_Spanner_Flex";
  private static final int SPANNER_NODE_COUNT = 10;

  private static final int MAX_WORKERS = 100;

  private static final int NUM_WORKERS = 10;

  private static final Duration JOB_TIMEOUT = Duration.ofHours(3);
  private static final Duration CHECK_INTERVAL = Duration.ofMinutes(5);
  private static final Duration DONE_TIMEOUT = Duration.ofMinutes(20);

  private SQLDialect dialect;
  private GcsResourceManager gcsResourceManager;
  private StaticJDBCResource sourceDatabaseResource;
  private SpannerResourceManager spannerResourceManager;

  private final String artifactBucket;
  ;
  private final SecretManagerResourceManager secretClient;
  private final String testRootDir;

  public SourceDbToSpannerLTBase() {
    try {
      artifactBucket = TestProperties.artifactBucket();
      testRootDir = getClass().getSimpleName();
      secretClient = SecretManagerResourceManager.builder(project, CREDENTIALS_PROVIDER).build();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public void setUp(
      SQLDialect dialect, String host, int port, String username, String password, String database)
      throws IOException {
    this.dialect = dialect;

    spannerResourceManager =
        SpannerResourceManager.builder(testName, project, region)
            .maybeUseStaticInstance()
            .setNodeCount(SPANNER_NODE_COUNT)
            .setMonitoringClient(monitoringClient)
            .build();

    gcsResourceManager =
        GcsResourceManager.builder(artifactBucket, getClass().getSimpleName(), CREDENTIALS).build();

    if (dialect == SQLDialect.POSTGRESQL) {
      sourceDatabaseResource =
          new StaticPostgresqlResource.Builder(host, username, password, port, database).build();
    } else if (dialect == SQLDialect.MYSQL) {
      sourceDatabaseResource =
          new StaticMySQLResource.Builder(host, username, password, port, database).build();
    } else {
      throw new IllegalArgumentException("Dialect " + dialect + " not supported");
    }
  }

  public String accessSecret(String secretVersion) {
    return secretClient.accessSecret(secretVersion);
  }

  public void createSpannerDDL(String resourceName) throws IOException {
    String ddl =
        String.join(
                " ",
                Resources.readLines(Resources.getResource(resourceName), StandardCharsets.UTF_8))
            .trim();
    List<String> ddls =
        Arrays.stream(ddl.split(";")).filter(d -> !d.isBlank()).collect(Collectors.toList());
    spannerResourceManager.executeDdlStatements(ddls);
  }

  public void runLoadTest(Map<String, Integer> expectations)
      throws IOException, ParseException, InterruptedException {
    runLoadTest(expectations, new HashMap<>(), new HashMap<>());
  }

  public void runLoadTest(
      Map<String, Integer> expectations,
      Map<String, String> templateParameters,
      Map<String, String> environmentOptions)
      throws IOException, ParseException, InterruptedException {

    // Add all parameters for the template
    String outputDirectory =
        String.join(
            "/", new String[] {testRootDir, gcsResourceManager.runId(), testName, "output"});
    Map<String, String> params =
        new HashMap<>() {
          {
            put("projectId", project);
            put("instanceId", spannerResourceManager.getInstanceId());
            put("databaseId", spannerResourceManager.getDatabaseId());
            put("sourceDbDialect", dialect.name());
            put("sourceConfigURL", sourceDatabaseResource.getconnectionURL());
            put("username", sourceDatabaseResource.username());
            put("password", sourceDatabaseResource.password());
            put("outputDirectory", "gs://" + artifactBucket + "/" + outputDirectory);
            put("jdbcDriverClassName", driverClassName());
          }
        };
    params.putAll(templateParameters);

    // Configure job
    LaunchConfig.Builder options =
        LaunchConfig.builder(getClass().getSimpleName(), SPEC_PATH)
            .addEnvironment("maxWorkers", MAX_WORKERS)
            .addEnvironment("numWorkers", NUM_WORKERS)
            .setParameters(params);
    environmentOptions.forEach(options::addEnvironment);

    // Act
    PipelineLauncher.LaunchInfo jobInfo = pipelineLauncher.launch(project, region, options.build());
    assertThatPipeline(jobInfo).isRunning();

    ConditionCheck[] checks =
        expectations.entrySet().stream()
            .map(
                entry ->
                    SpannerRowsCheck.builder(spannerResourceManager, entry.getKey())
                        .setMinRows(entry.getValue())
                        .setMaxRows(entry.getValue())
                        .build())
            .toArray(ConditionCheck[]::new);

    // Wait and assert conditions
    Result result =
        pipelineOperator.waitForCondition(
            createConfig(jobInfo, JOB_TIMEOUT, CHECK_INTERVAL), checks);
    assertThatResult(result).meetsConditions();

    result = pipelineOperator.waitUntilDone(createConfig(jobInfo, DONE_TIMEOUT));
    assertThatResult(result).isLaunchFinished();

    Map<String, Double> metrics = getMetrics(jobInfo);
    populateResourceManagerMetrics(metrics);

    // Export results
    exportMetricsToBigQuery(jobInfo, metrics);
  }

  public void populateResourceManagerMetrics(Map<String, Double> metrics) {
    spannerResourceManager.collectMetrics(metrics);
  }

  /**
   * Cleanup resource managers.
   *
   * @throws IOException
   */
  @After
  public void cleanUp() throws IOException {
    ResourceManagerUtils.cleanResources(spannerResourceManager, gcsResourceManager);
  }

  private String driverClassName() {
    try {
      switch (dialect) {
        case MYSQL:
          return Class.forName("com.mysql.jdbc.Driver").getCanonicalName();
        case POSTGRESQL:
          return Class.forName("org.postgresql.Driver").getCanonicalName();
        default:
          throw new IllegalStateException("Unknown driver for " + dialect);
      }
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }
}
