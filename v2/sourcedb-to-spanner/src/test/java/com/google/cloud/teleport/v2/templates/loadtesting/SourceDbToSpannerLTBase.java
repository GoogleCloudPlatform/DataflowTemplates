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

/**
 * Base class for Load Tests (LT) of the Source-to-Spanner template.
 *
 * <p>This class provides common infrastructure for large-scale migrations, including:
 *
 * <ul>
 *   <li><b>Spanner Configuration</b>: Automatic setup of large Spanner instances (e.g., 10 nodes).
 *   <li><b>JDBC Resource Management</b>: Handling static connections to source databases (MySQL,
 *       PostgreSQL).
 *   <li><b>VPC Networking</b>: Support for shared VPCs required for template-to-DB connectivity.
 *   <li><b>Metrics Collection</b>: Integration with BigQuery for long-term performance tracking.
 * </ul>
 */
public class SourceDbToSpannerLTBase extends TemplateLoadTestBase {

  protected static final String SPEC_PATH =
      System.getProperty(
          "specPath", "gs://dataflow-templates/latest/flex/Sourcedb_to_Spanner_Flex");
  private static final int SPANNER_NODE_COUNT = 10;

  private static final int MAX_WORKERS = 100;

  private static final int NUM_WORKERS = 10;

  private static final Duration JOB_TIMEOUT = Duration.ofHours(3);
  private static final Duration CHECK_INTERVAL = Duration.ofMinutes(5);
  private static final Duration DONE_TIMEOUT = Duration.ofMinutes(20);

  protected SQLDialect dialect;
  protected GcsResourceManager gcsResourceManager;
  private StaticJDBCResource sourceDatabaseResource;
  protected SpannerResourceManager spannerResourceManager;

  private final String artifactBucket;
  private final SecretManagerResourceManager secretClient;
  private final String testRootDir;

  /**
   * VPC configuration required for large-scale tests where the Dataflow workers must connect to
   * databases within a private network.
   */
  protected static final String VPC_NAME = "spanner-wide-row-pr-test-vpc";

  protected static final String VPC_REGION = "us-central1";
  protected static final String SUBNET_NAME = "regions/" + VPC_REGION + "/subnetworks/" + VPC_NAME;
  protected static final Map<String, String> ADDITIONAL_JOB_PARAMS = new HashMap<>();

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

  protected PipelineLauncher.LaunchInfo launchJob(LaunchConfig.Builder options) throws IOException {
    PipelineLauncher.LaunchInfo jobInfo = pipelineLauncher.launch(project, region, options.build());
    assertThatPipeline(jobInfo).isRunning();
    return jobInfo;
  }

  protected void collectAndExportMetrics(PipelineLauncher.LaunchInfo jobInfo)
      throws ParseException, IOException, InterruptedException {
    Map<String, Double> metrics = getMetrics(jobInfo);
    populateResourceManagerMetrics(metrics);
    exportMetricsToBigQuery(jobInfo, metrics);
  }

  protected Map<String, String> getCommonParameters() {
    Map<String, String> params = new HashMap<>();
    params.put("projectId", project);
    params.put("instanceId", spannerResourceManager.getInstanceId());
    params.put("databaseId", spannerResourceManager.getDatabaseId());
    params.put("outputDirectory", getOutputDirectory());
    return params;
  }

  protected String getOutputDirectory() {
    return "gs://"
        + artifactBucket
        + "/"
        + String.join(
            "/", new String[] {testRootDir, gcsResourceManager.runId(), testName, "output"});
  }

  protected Map<String, String> getJdbcParameters(StaticJDBCResource jdbcResource) {
    return getJdbcParameters(
        jdbcResource.getconnectionURL(),
        jdbcResource.username(),
        jdbcResource.password(),
        driverClassName());
  }

  protected Map<String, String> getJdbcParameters(
      String connectionUrl, String username, String password, String driverClassName) {
    Map<String, String> params = new HashMap<>();
    params.put("sourceDbDialect", dialect.name());
    params.put("sourceConfigURL", connectionUrl);
    params.put("username", username);
    params.put("password", password);
    params.put("jdbcDriverClassName", driverClassName);
    return params;
  }

  /**
   * Orchestrates the execution of a load test, including job launch, row count verification, and
   * metrics export.
   *
   * @param expectations map of table names to expected row counts.
   * @param templateParameters additional parameters for the Dataflow template.
   * @param environmentOptions Dataflow environment options (e.g., workers, machine type).
   * @throws IOException if job launch fails.
   * @throws ParseException if metrics parsing fails.
   * @throws InterruptedException if waiting for the job is interrupted.
   */
  public void runLoadTest(
      Map<String, Integer> expectations,
      Map<String, String> templateParameters,
      Map<String, String> environmentOptions)
      throws IOException, ParseException, InterruptedException {

    Map<String, String> params = getCommonParameters();
    params.putAll(getJdbcParameters(sourceDatabaseResource));
    params.put("workerMachineType", "n2-standard-4");

    params.putAll(ADDITIONAL_JOB_PARAMS);
    params.putAll(templateParameters);
    // Configure job
    LaunchConfig.Builder options =
        LaunchConfig.builder(getClass().getSimpleName(), SPEC_PATH)
            .addEnvironment("maxWorkers", MAX_WORKERS)
            .addEnvironment("numWorkers", NUM_WORKERS)
            .setParameters(params);
    environmentOptions.forEach(options::addEnvironment);

    // Act
    PipelineLauncher.LaunchInfo jobInfo = launchJob(options);

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

    collectAndExportMetrics(jobInfo);
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
