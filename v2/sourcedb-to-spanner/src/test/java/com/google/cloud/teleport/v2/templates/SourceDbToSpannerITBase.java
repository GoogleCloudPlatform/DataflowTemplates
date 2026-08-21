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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.reader.io.jdbc.iowrapper.config.SQLDialect;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.cloud.teleport.v2.spanner.migrations.source.config.JdbcShardConfig;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.it.cassandra.CassandraResourceManager;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.ResourceManager;
import org.apache.beam.it.common.utils.IORedirectUtil;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.gcp.JDBCBaseIT;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.apache.beam.it.jdbc.PostgresResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;
import org.testcontainers.shaded.org.apache.commons.lang3.RandomStringUtils;

/**
 * Base class for SourceDbToSpanner integration tests. It provides helper functions related to
 * environment setup and assertConditions.
 */
public class SourceDbToSpannerITBase extends JDBCBaseIT {
  protected String testUsername = null;
  private static final Logger LOG = LoggerFactory.getLogger(SourceDbToSpannerITBase.class);

  public MySQLResourceManager setUpMySQLResourceManager() {
    return MySQLResourceManager.builder(testName).build();
  }

  public CloudMySQLResourceManager setUpCloudMySQLResourceManager() {
    return CloudMySQLResourceManager.builder(testName).build();
  }

  public org.apache.beam.it.jdbc.OracleResourceManager setUpOracleResourceManager() {
    return org.apache.beam.it.jdbc.OracleResourceManager.builder(testName).build();
  }

  protected void loadOracleSQLFileResource(
      JDBCResourceManager jdbcResourceManager, String resourcePath) throws Exception {
    String sql =
        String.join(
            " ", Resources.readLines(Resources.getResource(resourcePath), StandardCharsets.UTF_8));
    loadOracleSQLToJdbcResourceManager(jdbcResourceManager, sql);
  }

  protected void loadOracleSQLFileResource(
      JDBCResourceManager jdbcResourceManager, String resourcePath, String targetUsername)
      throws Exception {
    String sql =
        String.join(
            " ", Resources.readLines(Resources.getResource(resourcePath), StandardCharsets.UTF_8));
    loadOracleSQLToJdbcResourceManager(jdbcResourceManager, sql, targetUsername);
  }

  protected void loadOracleSQLToJdbcResourceManager(
      JDBCResourceManager jdbcResourceManager, String sql) throws Exception {
    loadOracleSQLToJdbcResourceManager(jdbcResourceManager, sql, "SYSTEM");
  }

  protected void loadOracleSQLToJdbcResourceManager(
      JDBCResourceManager jdbcResourceManager, String sql, String targetUsername) throws Exception {
    LOG.info("Loading Oracle sql to jdbc resource manager in schema {}", targetUsername);
    try (Connection connection =
        DriverManager.getConnection(
            jdbcResourceManager.getUri(),
            jdbcResourceManager.getUsername(),
            jdbcResourceManager.getPassword())) {

      // Ensure creation of tables occurs in the isolated namespace
      if (!"SYSTEM".equalsIgnoreCase(targetUsername)) {
        try (Statement stmt = connection.createStatement()) {
          stmt.execute("ALTER SESSION SET CURRENT_SCHEMA = " + targetUsername);
        }
      }

      // Preprocess SQL to handle multi-line statements and newlines
      sql = sql.replaceAll("\r\n", " ").replaceAll("\n", " ");

      // Split into individual statements based on -- SPLIT --
      String[] statements = sql.split("-- SPLIT --");

      // Execute each statement
      try (Statement statement = connection.createStatement()) {
        for (String stmt : statements) {
          if (!stmt.trim().isEmpty()) {
            LOG.info("Executing Oracle statement: {}", stmt);
            statement.executeUpdate(stmt);
          }
        }
      }
    } catch (Exception e) {
      LOG.info("failed to load SQL into database: {}", sql);
      throw new Exception("Failed to load SQL into database", e);
    }
    LOG.info("Successfully loaded sql to jdbc resource manager");
  }

  protected String setupOracleIsolatedUser(JDBCResourceManager jdbcResourceManager) {
    String testUsername =
        "BULK_"
            + java.util.UUID.randomUUID().toString().replace("-", "").substring(0, 8).toUpperCase();
    LOG.info("Creating isolated Oracle user: {}", testUsername);
    jdbcResourceManager.runSQLUpdate("CREATE USER " + testUsername + " IDENTIFIED BY password");
    jdbcResourceManager.runSQLUpdate("GRANT ALL PRIVILEGES TO " + testUsername);
    return testUsername;
  }

  public PostgresResourceManager setUpPostgreSQLResourceManager() {
    return PostgresResourceManager.builder(testName).build();
  }

  public CassandraResourceManager setupCassandraResourceManager() {
    /* The default is Cassandra 4.1 image. TODO: Explore testing with non 4.1 tags. */

    /* Max Cassandra Keyspace is 48 characters. Base Resource Manager adds 24 characters of date-time at the end.
     * That's why we need to take a smaller subsequence of the testId.
     */
    String uniqueId =
        testId.substring(0, Math.min(15, testId.length()))
            + "_"
            + RandomStringUtils.randomAlphabetic(4).toLowerCase();

    return CassandraResourceManager.builder(uniqueId).build();
  }

  public SpannerResourceManager setUpSpannerResourceManager() {
    return SpannerResourceManager.builder(testName, PROJECT, REGION)
        .maybeUseStaticInstance()
        .build();
  }

  public SpannerResourceManager setUpPGDialectSpannerResourceManager() {
    return SpannerResourceManager.builder(testName, PROJECT, REGION, Dialect.POSTGRESQL)
        .maybeUseStaticInstance()
        .build();
  }

  protected void loadSQLFileResource(JDBCResourceManager jdbcResourceManager, String resourcePath)
      throws Exception {
    String sql =
        String.join(
            " ", Resources.readLines(Resources.getResource(resourcePath), StandardCharsets.UTF_8));
    loadSQLToJdbcResourceManager(jdbcResourceManager, sql);
  }

  protected void loadSQLFileResource(
      JDBCResourceManager jdbcResourceManager, String resourcePath, String targetUsername)
      throws Exception {
    String sql =
        String.join(
            " ", Resources.readLines(Resources.getResource(resourcePath), StandardCharsets.UTF_8));
    loadSQLToJdbcResourceManager(jdbcResourceManager, sql, targetUsername);
  }

  protected void loadSQLToJdbcResourceManager(JDBCResourceManager jdbcResourceManager, String sql)
      throws Exception {
    loadSQLToJdbcResourceManager(jdbcResourceManager, sql, "SYSTEM");
  }

  protected void loadSQLToJdbcResourceManager(
      JDBCResourceManager jdbcResourceManager, String sql, String targetUsername) throws Exception {
    LOG.info(
        "Loading sql to jdbc resource manager with uri: {} as user: {}",
        jdbcResourceManager.getUri(),
        targetUsername);
    try (Connection connection =
        DriverManager.getConnection(
            jdbcResourceManager.getUri(),
            jdbcResourceManager.getUsername(),
            jdbcResourceManager.getPassword())) {

      // Ensure creation of tables occurs in the isolated namespace
      if (!"SYSTEM".equalsIgnoreCase(targetUsername)
          && jdbcResourceManager instanceof org.apache.beam.it.jdbc.OracleResourceManager) {
        try (Statement stmt = connection.createStatement()) {
          stmt.execute("ALTER SESSION SET CURRENT_SCHEMA = " + targetUsername);
        }
      }

      // Preprocess SQL to handle multi-line statements and newlines
      sql = sql.replaceAll("\r\n", " ").replaceAll("\n", " ");

      // Split into individual statements
      String[] statements = sql.split(";");

      // Execute each statement
      try (Statement statement = connection.createStatement()) {
        for (String stmt : statements) {
          if (!stmt.trim().isEmpty()) {
            // Skip SELECT statements
            if (!stmt.trim().toUpperCase().startsWith("SELECT")) {
              LOG.info("Executing statement: {}", stmt);
              statement.executeUpdate(stmt);
            }
          }
        }
      }
    } catch (Exception e) {
      LOG.info("failed to load SQL into database: {}", sql);
      throw new Exception("Failed to load SQL into database", e);
    }
    LOG.info("Successfully loaded sql to jdbc resource manager");
  }

  protected void loadCSQLFileResource(
      CassandraResourceManager cassandraResourceManager, String resourcePath) throws Exception {
    String sql =
        String.join(
            " ",
            Resources.readLines(Resources.getResource(resourcePath), StandardCharsets.UTF_8)
                .stream()
                .map(s -> s.replaceAll("--.*", ""))
                .collect(Collectors.toList()));
    loadSQLToCassandraResourceManager(cassandraResourceManager, sql);
  }

  protected void loadSQLToCassandraResourceManager(
      CassandraResourceManager cassandraResourceManager, String sql) throws Exception {
    LOG.info(
        "Loading sql to  cassandra resource manager with host: {}",
        cassandraResourceManager.getHost());
    try {
      // Preprocess SQL to handle multi-line statements and newlines
      sql = sql.replaceAll("\r\n", " ").replaceAll("\n", " ");

      // Split into individual statements
      String[] statements = sql.split(";");

      // Execute each statement
      for (String stmt : statements) {
        if (!stmt.trim().isEmpty()) {
          // Skip SELECT statements
          if (!stmt.trim().toUpperCase().startsWith("SELECT")) {
            LOG.info("Executing statement: {}", stmt);
            cassandraResourceManager.executeStatement(stmt);
          }
        }
      }
    } catch (Exception e) {
      LOG.info("failed to load SQL into database: {}", sql);
      throw new Exception("Failed to load SQL into database", e);
    }
    LOG.info("Successfully loaded sql to jdbc resource manager");
  }

  /**
   * Helper function for creating Spanner DDL. Reads the sql file from resources directory and
   * applies the DDL to Spanner instance.
   *
   * @param spannerResourceManager Initialized SpannerResourceManager instance
   * @param resourceName SQL file name with path relative to resources directory
   */
  public void createSpannerDDL(SpannerResourceManager spannerResourceManager, String resourceName)
      throws IOException {
    LOG.info("Creating spanner DDL");
    String ddl =
        String.join(
            " ",
            Resources.readLines(Resources.getResource(resourceName), StandardCharsets.UTF_8)
                .stream()
                .map(line -> line.replaceAll("\\s*--.*$", ""))
                .collect(ImmutableList.toImmutableList()));
    ddl = ddl.trim();
    List<String> ddls =
        Arrays.stream(ddl.split(";")).filter(d -> !d.isBlank()).collect(Collectors.toList());
    spannerResourceManager.executeDdlStatements(ddls);
    LOG.info("Successfully created spanner DDL");
  }

  /**
   * Performs the following steps: Uploads session file to GCS. Creates Pubsub resources. Launches
   * SourceDbToSpanner dataflow job for Jdbc Source.
   *
   * @param identifierSuffix will be used as postfix in generated resource ids
   * @param sessionFileResourceName Session file name with path relative to resources directory
   * @param gcsPathPrefix Prefix directory name for this DF job. Data and DLQ directories will be
   *     created under this prefix.
   * @return dataflow jobInfo object
   */
  protected PipelineLauncher.LaunchInfo launchDataflowJob(
      String identifierSuffix,
      String sessionFileResourceName,
      String gcsPathPrefix,
      ResourceManager sourceResourceManager,
      SpannerResourceManager spannerResourceManager,
      Map<String, String> jobParameters,
      CustomTransformation customTransformation)
      throws IOException {

    Map<String, String> params =
        new HashMap<>() {
          {
            put("projectId", PROJECT);
            put("instanceId", spannerResourceManager.getInstanceId());
            put("databaseId", spannerResourceManager.getDatabaseId());
          }
        };
    if (sourceResourceManager instanceof JDBCResourceManager) {
      params.putAll(
          getJdbcParameters(
              (JDBCResourceManager) sourceResourceManager, gcsPathPrefix, jobParameters));
    } else if (sourceResourceManager instanceof CassandraResourceManager) {
      params.putAll(
          getCassandraParameters((CassandraResourceManager) sourceResourceManager, gcsPathPrefix));
    }
    if (!params.containsKey("outputDirectory")) {
      params.put("outputDirectory", "gs://" + artifactBucketName);
    }

    if (sessionFileResourceName != null) {
      String sessionPath = gcsPathPrefix + "/session.json";
      LOG.info("uploading session file to: {}", sessionPath);
      gcsClient.uploadArtifact(
          sessionPath, Resources.getResource(sessionFileResourceName).getPath());
      params.put("sessionFilePath", getGcsPath(sessionPath));
    }

    if (customTransformation != null) {
      params.put(
          "transformationJarPath",
          getGcsPath(gcsPathPrefix + "/" + customTransformation.jarPath()));
      params.put("transformationClassName", customTransformation.classPath());
    }

    // overridden parameters
    String ipConfig = "WORKER_IP_PRIVATE";
    if (jobParameters != null) {
      if (jobParameters.containsKey("ipConfiguration")) {
        ipConfig = jobParameters.get("ipConfiguration");
      }
      for (Map.Entry<String, String> entry : jobParameters.entrySet()) {
        if ("namespace".equals(entry.getKey()) || "ipConfiguration".equals(entry.getKey())) {
          continue;
        }
        params.put(entry.getKey(), entry.getValue());
      }
    }

    // Construct template
    String jobName = PipelineUtils.createJobName(identifierSuffix);
    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(jobName, specPath);

    options.setParameters(params);
    options.addEnvironment("additionalExperiments", List.of("disable_runner_v2"));
    options.addEnvironment("numWorkers", 2);
    options.addEnvironment("ipConfiguration", ipConfig);
    options.addEnvironment("additionalPipelineOptions", List.of("resourceHints=cpu_count=4"));
    // Run
    PipelineLauncher.LaunchInfo jobInfo = launchTemplate(options);
    assertThatPipeline(jobInfo).isRunning();

    return jobInfo;
  }

  protected String createAndUploadShardConfigToGcs(
      String gcsPathPrefix,
      JDBCResourceManager jdbcResourceManager,
      Map<String, String> jobParameters)
      throws IOException {
    Shard shard = new Shard();
    shard.setLogicalShardId("Shard1");
    shard.setUser(jdbcResourceManager.getUsername());
    shard.setPassword(jdbcResourceManager.getPassword());
    if (jobParameters != null && jobParameters.containsKey("namespace")) {
      shard.setNamespace(jobParameters.get("namespace"));
    } else if (testUsername != null) {
      shard.setNamespace(testUsername);
      shard.setUser(testUsername);
      shard.setPassword("password");
    }

    if (jdbcResourceManager instanceof PostgresResourceManager pgRm) {
      shard.setHost(pgRm.getHost());
      shard.setPort(String.valueOf(pgRm.getPort()));
      shard.setDbName(pgRm.getDatabaseName());
    } else if (jdbcResourceManager instanceof MySQLResourceManager mySqlRm) {
      shard.setHost(mySqlRm.getHost());
      shard.setPort(String.valueOf(mySqlRm.getPort()));
      shard.setDbName(mySqlRm.getDatabaseName());
    } else if (jdbcResourceManager
        instanceof org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager cloudRm) {
      shard.setHost(cloudRm.getHost());
      shard.setPort(String.valueOf(cloudRm.getPort()));
      shard.setDbName(cloudRm.getDatabaseName());
    } else if (jdbcResourceManager
        instanceof org.apache.beam.it.jdbc.OracleResourceManager oracleRm) {
      shard.setHost(oracleRm.getHost());
      shard.setPort(String.valueOf(oracleRm.getPort()));
      shard.setDbName(oracleRm.getDatabaseName());
    } else {
      throw new IllegalArgumentException(
          "Unsupported JDBC resource manager type: " + jdbcResourceManager.getClass().getName());
    }

    if (jobParameters != null && jobParameters.containsKey("namespace")) {
      shard.setNamespace(jobParameters.get("namespace"));
    } else if (testUsername != null) {
      shard.setNamespace(testUsername);
      shard.setUser(testUsername);
      shard.setPassword("password");
    } else if (jdbcResourceManager instanceof org.apache.beam.it.jdbc.OracleResourceManager) {
      shard.setNamespace(jdbcResourceManager.getUsername().toUpperCase());
    }

    JdbcShardConfig jdbcShardConfig = new JdbcShardConfig();
    jdbcShardConfig.setShardConfigs(List.of(shard));
    String shardFileContents = new Gson().toJson(jdbcShardConfig);
    LOG.info("Shard file contents: {}", shardFileContents);

    String configBasePath = (gcsPathPrefix == null) ? "null" : gcsPathPrefix;
    if (configBasePath.endsWith("/")) {
      configBasePath = configBasePath.substring(0, configBasePath.length() - 1);
    }
    String configGcsPath = getGcsPath(configBasePath + "/shard.json");

    gcsClient.createArtifact(configBasePath + "/shard.json", shardFileContents);

    return configGcsPath;
  }

  private Map<String, String> getJdbcParameters(
      JDBCResourceManager jdbcResourceManager,
      String gcsPathPrefix,
      Map<String, String> jobParameters) {

    Map<String, String> params =
        new HashMap<>() {
          {
            put("sourceDbDialect", sqlDialectFrom(jdbcResourceManager));
            try {
              put(
                  "sourceConfigURL",
                  createAndUploadShardConfigToGcs(
                      gcsPathPrefix, jdbcResourceManager, jobParameters));
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            put("jdbcDriverClassName", driverClassNameFrom(jdbcResourceManager));
          }
        };
    return params;
  }

  private Map<String, String> getCassandraParameters(
      CassandraResourceManager cassandraResourceManager, String gcsPathPrefix) throws IOException {

    Map<String, String> params =
        new HashMap<>() {
          {
            put("sourceDbDialect", sqlDialectFrom(cassandraResourceManager));
          }
        };

    String configFile =
        String.format(
            """
                            datastax-java-driver {
                                  basic.contact-points = ["%s:%d"]
                                  basic.session-keyspace = %s
                                  basic.load-balancing-policy {
                                    local-datacenter = datacenter1
                                  }
                                }""",
            cassandraResourceManager.getHost(),
            cassandraResourceManager.getPort(),
            cassandraResourceManager.getKeyspaceName());
    Path tempFile = Files.createTempFile("temp", ".txt");

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile.toFile()))) {
      writer.write(configFile);
      writer.flush();
    }

    String configBasePath =
        (gcsPathPrefix == null)
            ? ""
            : gcsPathPrefix.endsWith("/")
                ? gcsPathPrefix.substring(0, gcsPathPrefix.length() - 1)
                : gcsPathPrefix;
    String fileNamePrefix = testId.substring(0, Math.min(20, testId.length()));
    configBasePath = configBasePath + "cassandra/" + fileNamePrefix + "-config.conf";
    String configGcsPath = getGcsPath(configBasePath);
    String configPath = configGcsPath.replace("gs://" + artifactBucketName + "/", "");
    String outputBasePath =
        (gcsPathPrefix == null)
            ? ""
            : gcsPathPrefix.endsWith("/")
                ? gcsPathPrefix.substring(0, gcsPathPrefix.length() - 1)
                : gcsPathPrefix;
    outputBasePath =
        outputBasePath + "cassandra/" + testId.substring(0, Math.min(20, testId.length())) + "/";
    String outputPath = getGcsPath(outputBasePath);
    LOG.info("OutputPath = {}", outputPath);

    gcsClient.copyFileToGcs(tempFile.toAbsolutePath(), configPath);
    LOG.info(
        "Cassandra Config File uploaded for test = {}, testID = {} ,at {}\nConfig={}\n",
        testName,
        testId,
        configGcsPath,
        configFile);
    Files.delete(tempFile);
    params.put("sourceConfigURL", configGcsPath);
    params.put("outputDirectory", outputPath);
    return params;
  }

  public void createAndUploadJarToGcs(String gcsPathPrefix)
      throws IOException, InterruptedException {
    String[] shellCommand = {"/bin/bash", "-c", "cd ../spanner-custom-shard"};

    Process exec = Runtime.getRuntime().exec(shellCommand);

    IORedirectUtil.redirectLinesLog(exec.getInputStream(), LOG);
    IORedirectUtil.redirectLinesLog(exec.getErrorStream(), LOG);

    if (exec.waitFor() != 0) {
      throw new RuntimeException("Error staging template, check Maven logs.");
    }
    gcsClient.uploadArtifact(
        gcsPathPrefix + "/customTransformation.jar",
        "../spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar");
  }

  private String sqlDialectFrom(ResourceManager resourceManager) {
    if (resourceManager instanceof CassandraResourceManager) {
      return SQLDialect.CASSANDRA.name();
    }
    if (resourceManager instanceof PostgresResourceManager) {
      return SQLDialect.POSTGRESQL.name();
    }
    if (resourceManager instanceof org.apache.beam.it.jdbc.OracleResourceManager) {
      return "ORACLE";
    }
    return SQLDialect.MYSQL.name();
  }

  private String driverClassNameFrom(JDBCResourceManager jdbcResourceManager) {
    try {
      if (jdbcResourceManager instanceof PostgresResourceManager) {
        return Class.forName("org.postgresql.Driver").getCanonicalName();
      }
      if (jdbcResourceManager instanceof org.apache.beam.it.jdbc.OracleResourceManager) {
        return "oracle.jdbc.OracleDriver";
      }
      return Class.forName("com.mysql.jdbc.Driver").getCanonicalName();
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  protected PipelineOperator.Config.Builder wrapConfiguration(
      PipelineOperator.Config.Builder builder) {
    if (System.getProperty("directRunnerTest") != null) {
      return builder.setTimeoutAfter(Duration.ofMinutes(15));
    }
    return builder;
  }
}
