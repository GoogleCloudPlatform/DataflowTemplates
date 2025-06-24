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
package com.google.cloud.teleport.v2.templates.endtoend;

import static java.util.Arrays.stream;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.datastream.v1.DestinationConfig;
import com.google.cloud.datastream.v1.SourceConfig;
import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.teleport.v2.spanner.migrations.shard.Shard;
import com.google.common.io.Resources;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.dataflow.FlexTemplateDataflowJobResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.JDBCSource;
import org.apache.beam.it.gcp.datastream.MySQLSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.shaded.org.apache.commons.lang3.StringUtils;

public abstract class EndToEndTestingITBase extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(EndToEndTestingITBase.class);
  private FlexTemplateDataflowJobResourceManager flexTemplateDataflowJobResourceManager;
  protected static DatastreamResourceManager datastreamResourceManager;
  protected JDBCSource jdbcSource;

  protected class DataShard {
    String dataShardId;
    String host;
    String user;
    String password;
    String port;
    String dbName;
    String namespace;
    String connectionProperties;
    ArrayList<Database> databases;

    public DataShard(
        String dataShardId,
        String host,
        String user,
        String password,
        String port,
        String dbName,
        String namespace,
        String connectionProperties,
        ArrayList<Database> databases) {

      this.dataShardId = dataShardId;
      this.host = host;
      this.user = user;
      this.password = password;
      this.port = port;
      this.dbName = dbName;
      this.namespace = namespace;
      this.connectionProperties = connectionProperties;
      this.databases = databases;
    }
  }

  protected class Database {
    String dbName;
    String databaseId;
    String refDataShardId;

    public Database(String dbName, String databaseId, String refDataShardId) {
      this.dbName = dbName;
      this.databaseId = databaseId;
      this.refDataShardId = refDataShardId;
    }
  }

  protected SpannerResourceManager createSpannerDatabase(String spannerSchemaFile)
      throws IOException {
    SpannerResourceManager spannerResourceManager =
        SpannerResourceManager.builder("e2e-main-" + testName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();

    String ddl;
    try (InputStream inputStream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(spannerSchemaFile)) {
      if (inputStream == null) {
        throw new FileNotFoundException("Resource file not found: " + spannerSchemaFile);
      }
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
        ddl = reader.lines().collect(Collectors.joining("\n"));
      }
    }

    if (ddl.isBlank()) {
      throw new IllegalStateException("DDL file is empty: " + spannerSchemaFile);
    }

    String[] ddls = ddl.trim().split(";");
    for (String d : ddls) {
      d = d.trim();
      if (!d.isEmpty()) {
        spannerResourceManager.executeDdlStatement(d);
      }
    }
    return spannerResourceManager;
  }

  protected SpannerResourceManager createSpannerMetadataDatabase() {
    SpannerResourceManager spannerMetadataResourceManager =
        SpannerResourceManager.builder("e2e-meta-" + testName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();
    String dummy = "create table t1(id INT64 ) primary key(id)";
    spannerMetadataResourceManager.executeDdlStatement(dummy);
    return spannerMetadataResourceManager;
  }

  public PubsubResourceManager setUpPubSubResourceManager() throws IOException {
    return PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
  }

  // createPubsubResources generates pubsub topic, subscription and notification for migration.
  // It can be run in different modes based on type of migration.
  // Modes can be rr for reverse replication and fwd for forward migration
  public SubscriptionName createPubsubResources(
      String identifierSuffix,
      PubsubResourceManager pubsubResourceManager,
      String gcsPrefix,
      GcsResourceManager gcsResourceManager,
      String mode) {
    String topicNameSuffix = mode + "-it" + identifierSuffix;
    String subscriptionNameSuffix = mode + "-it-sub" + identifierSuffix;
    TopicName topic = pubsubResourceManager.createTopic(topicNameSuffix);
    SubscriptionName subscription =
        pubsubResourceManager.createSubscription(topic, subscriptionNameSuffix);
    String prefix = gcsPrefix;
    if (prefix.startsWith("/")) {
      prefix = prefix.substring(1);
    }
    // create retry directory for reverse migration
    if (mode == "rr") {
      prefix += "/retry/";
    }
    gcsResourceManager.createNotification(topic.toString(), prefix);
    return subscription;
  }

  protected void createAndUploadReverseShardConfigToGcs(
      GcsResourceManager gcsResourceManager,
      CloudSqlResourceManager cloudSqlResourceManager,
      String privateHost) {
    Shard shard = new Shard();
    shard.setLogicalShardId("Shard1");
    shard.setUser(cloudSqlResourceManager.getUsername());
    shard.setHost(privateHost);
    shard.setPassword(cloudSqlResourceManager.getPassword());
    shard.setPort(String.valueOf(cloudSqlResourceManager.getPort()));
    shard.setDbName(cloudSqlResourceManager.getDatabaseName());
    JsonObject jsObj = new Gson().toJsonTree(shard).getAsJsonObject();
    jsObj.remove("secretManagerUri"); // remove field secretManagerUri
    JsonArray ja = new JsonArray();
    ja.add(jsObj);
    String shardFileContents = ja.toString();
    LOG.info("Shard file contents: {}", shardFileContents);
    gcsResourceManager.createArtifact("input/shard.json", shardFileContents);
  }

  protected void createAndUploadReverseMultiShardConfigToGcs(
      GcsResourceManager gcsResourceManager, Map<String, CloudSqlResourceManager> shardsList) {
    JsonArray ja = new JsonArray();
    for (Entry<String, CloudSqlResourceManager> shardInfo : shardsList.entrySet()) {
      Shard shard = new Shard();
      shard.setLogicalShardId(shardInfo.getKey());
      shard.setUser(shardInfo.getValue().getUsername());
      shard.setHost(shardInfo.getValue().getHost());
      shard.setPassword(shardInfo.getValue().getPassword());
      shard.setPort(String.valueOf(shardInfo.getValue().getPort()));
      shard.setDbName(shardInfo.getValue().getDatabaseName());
      JsonObject jsObj = new Gson().toJsonTree(shard).getAsJsonObject();
      jsObj.remove("secretManagerUri"); // remove field secretManagerUri
      ja.add(jsObj);
    }
    String shardFileContents = ja.toString();
    LOG.info("Shard file contents: {}", shardFileContents);
    gcsResourceManager.createArtifact("input/shard.json", shardFileContents);
  }

  protected void createAndUploadBulkShardConfigToGcs(
      ArrayList<DataShard> dataShardsList, GcsResourceManager gcsResourceManager) {
    JSONObject bulkConfig = new JSONObject();
    bulkConfig.put("configType", "dataflow");

    JSONObject shardConfigBulk = new JSONObject();

    JSONObject schemaSourceJson = new JSONObject();
    schemaSourceJson.put("dataShardId", "");
    schemaSourceJson.put("host", "");
    schemaSourceJson.put("user", "");
    schemaSourceJson.put("password", "");
    schemaSourceJson.put("port", "");
    schemaSourceJson.put("dbName", "");
    shardConfigBulk.put("schemaSource", schemaSourceJson);

    JSONArray dataShardsArray = new JSONArray();
    if (dataShardsList != null) {
      for (DataShard shardData : dataShardsList) {
        JSONObject shardJson = new JSONObject();

        shardJson.put("dataShardId", shardData.dataShardId);
        shardJson.put("host", shardData.host);
        shardJson.put("user", shardData.user);
        shardJson.put("password", shardData.password);
        shardJson.put("port", shardData.port);
        shardJson.put("dbName", shardData.dbName);
        shardJson.put("namespace", shardData.namespace);
        shardJson.put("connectionProperties", shardData.connectionProperties);

        JSONArray databasesArray = new JSONArray();

        for (Database dbData : shardData.databases) {
          JSONObject dbJson = new JSONObject();
          dbJson.put("dbName", dbData.dbName);
          dbJson.put("databaseId", dbData.databaseId);
          dbJson.put("refDataShardId", dbData.refDataShardId);
          databasesArray.put(dbJson);
        }
        shardJson.put("databases", databasesArray);
        dataShardsArray.put(shardJson);
      }
    }
    shardConfigBulk.put("dataShards", dataShardsArray);

    bulkConfig.put("shardConfigurationBulk", shardConfigBulk);
    String shardFileContents = bulkConfig.toString();
    LOG.info("Shard file contents: {}", shardFileContents);
    gcsResourceManager.createArtifact("input/shard-bulk.json", shardFileContents);
  }

  protected void createAndUploadShardContextFileToGcs(
      Map<String, Map<String, String>> streamDbMapping, GcsResourceManager gcsResourceManager) {
    JSONObject shardConfig = new JSONObject();
    JSONObject streams = new JSONObject();

    for (String stream : streamDbMapping.keySet()) {
      JSONObject dbs = new JSONObject();
      for (String db : streamDbMapping.get(stream).keySet()) {
        dbs.put(db, streamDbMapping.get(stream).get(db));
      }
      streams.put(stream, dbs);
    }

    shardConfig.put("StreamToDbAndShardMap", streams);
    String shardFileContents = shardConfig.toString();
    LOG.info("Shard context file contents: {}", shardFileContents);
    gcsResourceManager.createArtifact("input/sharding-context.json", shardFileContents);
  }

  protected PipelineLauncher.LaunchInfo launchBulkDataflowJob(
      String jobName,
      SpannerResourceManager spannerResourceManager,
      GcsResourceManager gcsResourceManager,
      CloudSqlResourceManager cloudSqlResourceManager,
      Boolean multiSharded)
      throws IOException {
    // launch dataflow template
    if (multiSharded) {
      flexTemplateDataflowJobResourceManager =
          FlexTemplateDataflowJobResourceManager.builder(jobName)
              .withTemplateName("Sourcedb_to_Spanner_Flex")
              .withTemplateModulePath("v2/sourcedb-to-spanner")
              .addParameter("instanceId", spannerResourceManager.getInstanceId())
              .addParameter("databaseId", spannerResourceManager.getDatabaseId())
              .addParameter("projectId", PROJECT)
              .addParameter("outputDirectory", "gs://" + artifactBucketName)
              .addParameter("sessionFilePath", getGcsPath("input/session.json", gcsResourceManager))
              .addParameter(
                  "sourceConfigURL", getGcsPath("input/shard-bulk.json", gcsResourceManager))
              .addEnvironmentVariable(
                  "additionalExperiments", Collections.singletonList("disable_runner_v2"))
              .build();
    } else {
      flexTemplateDataflowJobResourceManager =
          FlexTemplateDataflowJobResourceManager.builder(jobName)
              .withTemplateName("Sourcedb_to_Spanner_Flex")
              .withTemplateModulePath("v2/sourcedb-to-spanner")
              .addParameter("instanceId", spannerResourceManager.getInstanceId())
              .addParameter("databaseId", spannerResourceManager.getDatabaseId())
              .addParameter("projectId", PROJECT)
              .addParameter("outputDirectory", "gs://" + artifactBucketName)
              .addParameter("sessionFilePath", getGcsPath("input/session.json", gcsResourceManager))
              .addParameter("sourceConfigURL", cloudSqlResourceManager.getUri())
              .addParameter("username", cloudSqlResourceManager.getUsername())
              .addParameter("password", cloudSqlResourceManager.getPassword())
              .addParameter("jdbcDriverClassName", "com.mysql.jdbc.Driver")
              .addEnvironmentVariable(
                  "additionalExperiments", Collections.singletonList("disable_runner_v2"))
              .build();
    }

    // Run
    PipelineLauncher.LaunchInfo jobInfo = flexTemplateDataflowJobResourceManager.launchJob();
    assertThatPipeline(jobInfo).isRunning();
    return jobInfo;
  }

  public PipelineLauncher.LaunchInfo launchRRDataflowJob(
      String jobName,
      SpannerResourceManager spannerResourceManager,
      GcsResourceManager gcsResourceManager,
      SpannerResourceManager spannerMetadataResourceManager,
      PubsubResourceManager pubsubResourceManager,
      String sourceType)
      throws IOException {

    // create subscription
    SubscriptionName rrSubscriptionName =
        createPubsubResources(
            getClass().getSimpleName(),
            pubsubResourceManager,
            getGcsPath("dlq", gcsResourceManager).replace("gs://" + artifactBucketName, ""),
            gcsResourceManager,
            "rr");

    // Launch Dataflow template
    flexTemplateDataflowJobResourceManager =
        FlexTemplateDataflowJobResourceManager.builder(jobName)
            .withTemplateName("Spanner_to_SourceDb")
            .withTemplateModulePath("v2/spanner-to-sourcedb")
            .addParameter("sessionFilePath", getGcsPath("input/session.json", gcsResourceManager))
            .addParameter("instanceId", spannerResourceManager.getInstanceId())
            .addParameter("databaseId", spannerResourceManager.getDatabaseId())
            .addParameter("spannerProjectId", PROJECT)
            .addParameter("metadataDatabase", spannerMetadataResourceManager.getDatabaseId())
            .addParameter("metadataInstance", spannerMetadataResourceManager.getInstanceId())
            .addParameter(
                "sourceShardsFilePath", getGcsPath("input/shard.json", gcsResourceManager))
            .addParameter("changeStreamName", "allstream")
            .addParameter("dlqGcsPubSubSubscription", rrSubscriptionName.toString())
            .addParameter("deadLetterQueueDirectory", getGcsPath("dlq", gcsResourceManager))
            .addParameter("maxShardConnections", "5")
            .addParameter("maxNumWorkers", "1")
            .addParameter("numWorkers", "1")
            .addParameter("sourceType", sourceType)
            .addEnvironmentVariable(
                "additionalExperiments", Collections.singletonList("use_runner_v2"))
            .build();

    // Run
    PipelineLauncher.LaunchInfo jobInfo = flexTemplateDataflowJobResourceManager.launchJob();
    assertThatPipeline(jobInfo).isRunning();
    return jobInfo;
  }

  public String getGcsPath(String... pathParts) {
    checkArgument(pathParts.length != 0, "Must provide at least one path part");
    checkArgument(
        stream(pathParts).noneMatch(Strings::isNullOrEmpty), "No path part can be null or empty");

    return String.format("gs://%s", String.join("/", pathParts));
  }

  public PipelineLauncher.LaunchInfo launchFwdDataflowJob(
      String jobName,
      SpannerResourceManager spannerResourceManager,
      GcsResourceManager gcsResourceManager,
      PubsubResourceManager pubsubResourceManager,
      Boolean multiSharded,
      Map<String, String> dbs,
      Boolean backfill)
      throws IOException {
    String testRootDir = getClass().getSimpleName();

    // create subscriptions
    String gcsPrefix =
        String.join("/", new String[] {testRootDir, gcsResourceManager.runId(), testName, "cdc"});
    SubscriptionName subscription =
        createPubsubResources(
            testRootDir + testName, pubsubResourceManager, gcsPrefix, gcsResourceManager, "fwd");

    String dlqGcsPrefix =
        String.join("/", new String[] {testRootDir, gcsResourceManager.runId(), testName, "dlq"});
    SubscriptionName dlqSubscription =
        createPubsubResources(
            testRootDir + testName + "dlq",
            pubsubResourceManager,
            dlqGcsPrefix,
            gcsResourceManager,
            "fwd");
    String artifactBucket = TestProperties.artifactBucket();

    // launch datastream
    datastreamResourceManager =
        DatastreamResourceManager.builder(testName, PROJECT, REGION)
            .setCredentialsProvider(credentialsProvider)
            .setPrivateConnectivity("datastream-private-connect-us-central1")
            .build();
    Stream stream =
        createDataStreamResources(
            artifactBucket, gcsPrefix, jdbcSource, datastreamResourceManager, backfill);
    if (multiSharded) {
      createAndUploadShardContextFileToGcs(
          new HashMap<>() {
            {
              put(stream.getDisplayName(), dbs);
            }
          },
          gcsResourceManager);
    }

    if (multiSharded) {
      flexTemplateDataflowJobResourceManager =
          FlexTemplateDataflowJobResourceManager.builder(jobName)
              .withTemplateName("Cloud_Datastream_to_Spanner")
              .withTemplateModulePath("v2/datastream-to-spanner")
              .addParameter("inputFilePattern", getGcsPath(artifactBucket, gcsPrefix))
              .addParameter("streamName", stream.getName())
              .addParameter("instanceId", spannerResourceManager.getInstanceId())
              .addParameter("databaseId", spannerResourceManager.getDatabaseId())
              .addParameter("projectId", PROJECT)
              .addParameter("deadLetterQueueDirectory", getGcsPath(artifactBucket, dlqGcsPrefix))
              .addParameter("gcsPubSubSubscription", subscription.toString())
              .addParameter("dlqGcsPubSubSubscription", dlqSubscription.toString())
              .addParameter("datastreamSourceType", "mysql")
              .addParameter("inputFileFormat", "avro")
              .addParameter("sessionFilePath", getGcsPath("input/session.json", gcsResourceManager))
              .addParameter(
                  "shardingContextFilePath",
                  getGcsPath("input/sharding-context.json", gcsResourceManager))
              .addEnvironmentVariable(
                  "additionalExperiments", Collections.singletonList("use_runner_v2"))
              .build();
    } else {
      flexTemplateDataflowJobResourceManager =
          FlexTemplateDataflowJobResourceManager.builder(jobName)
              .withTemplateName("Cloud_Datastream_to_Spanner")
              .withTemplateModulePath("v2/datastream-to-spanner")
              .addParameter("inputFilePattern", getGcsPath(artifactBucket, gcsPrefix))
              .addParameter("streamName", stream.getName())
              .addParameter("instanceId", spannerResourceManager.getInstanceId())
              .addParameter("databaseId", spannerResourceManager.getDatabaseId())
              .addParameter("projectId", PROJECT)
              .addParameter("deadLetterQueueDirectory", getGcsPath(artifactBucket, dlqGcsPrefix))
              .addParameter("gcsPubSubSubscription", subscription.toString())
              .addParameter("dlqGcsPubSubSubscription", dlqSubscription.toString())
              .addParameter("datastreamSourceType", "mysql")
              .addParameter("inputFileFormat", "avro")
              .addParameter("sessionFilePath", getGcsPath("input/session.json", gcsResourceManager))
              .addEnvironmentVariable(
                  "additionalExperiments", Collections.singletonList("use_runner_v2"))
              .build();
    }

    // Run
    PipelineLauncher.LaunchInfo jobInfo = flexTemplateDataflowJobResourceManager.launchJob();
    assertThatPipeline(jobInfo).isRunning();
    return jobInfo;
  }

  public Stream createDataStreamResources(
      String artifactBucketName,
      String gcsPrefix,
      JDBCSource jdbcSource,
      DatastreamResourceManager datastreamResourceManager,
      Boolean backfill) {
    SourceConfig sourceConfig =
        datastreamResourceManager.buildJDBCSourceConfig("mysql", jdbcSource);

    // Create DataStream GCS Destination Connection profile and config
    DestinationConfig destinationConfig =
        datastreamResourceManager.buildGCSDestinationConfig(
            "gcs",
            artifactBucketName,
            gcsPrefix,
            DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT);

    // Create and start DataStream stream
    Stream stream;
    if (backfill) {
      stream =
          datastreamResourceManager.createStream("ds-spanner", sourceConfig, destinationConfig);
    } else {
      stream =
          datastreamResourceManager.createStreamWoBackfill(
              "ds-spanner", sourceConfig, destinationConfig);
    }
    datastreamResourceManager.startStream(stream);
    return stream;
  }

  protected ConditionCheck writeJdbcData(
      String tableName,
      Integer numRows,
      Map<String, Object> columns,
      Map<String, List<Map<String, Object>>> cdcEvents,
      Integer startValue,
      CloudSqlResourceManager cloudSqlResourceManager) {
    return new ConditionCheck() {
      @Override
      protected String getDescription() {
        return "Send initial JDBC events.";
      }

      @Override
      protected CheckResult check() {
        List<String> messages = new ArrayList<>();
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = startValue; i < numRows + startValue; i++) {
          Map<String, Object> values = new HashMap<>();
          values.put("id", i);
          values.putAll(columns);
          rows.add(values);
        }
        cdcEvents.put(tableName, rows);
        boolean success = cloudSqlResourceManager.write(tableName, rows);
        LOG.info(String.format("%d rows to %s", rows.size(), tableName));
        messages.add(String.format("%d rows to %s", rows.size(), tableName));
        return new CheckResult(success, "Sent " + String.join(", ", messages) + ".");
      }
    };
  }

  protected boolean writeRows(
      String tableName,
      Integer numRows,
      Map<String, Object> columns,
      Map<String, List<Map<String, Object>>> cdcEvents,
      Integer startValue,
      CloudSqlResourceManager cloudSqlResourceManager) {
    List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = startValue; i < numRows + startValue; i++) {
      Map<String, Object> values = new HashMap<>();
      values.put("id", i);
      values.putAll(columns);
      rows.add(values);
    }
    cdcEvents.put(tableName, rows);
    boolean success = cloudSqlResourceManager.write(tableName, rows);
    LOG.info(String.format("%d rows to %s", rows.size(), tableName));
    return success;
  }

  protected String generateSessionFile(String srcDb, String spannerDb, String sessionFileResource)
      throws IOException {
    String sessionFile =
        Files.readString(Paths.get(Resources.getResource(sessionFileResource).getPath()));
    return sessionFile.replaceAll("SRC_DATABASE", srcDb).replaceAll("SP_DATABASE", spannerDb);
  }

  protected JDBCSource createMySqlDatabase(
      List<CloudSqlResourceManager> cloudSqlResourceManagers,
      Map<String, Map<String, String>> tables) {
    for (CloudSqlResourceManager cloudSqlResourceManager : cloudSqlResourceManagers) {
      for (HashMap.Entry<String, Map<String, String>> entry : tables.entrySet()) {
        cloudSqlResourceManager.createTable(
            entry.getKey(), new JDBCResourceManager.JDBCSchema(entry.getValue(), "id"));
      }
    }
    Map<String, List<String>> allowedTables = new HashMap<>();
    for (CloudSqlResourceManager cloudSqlResourceManager : cloudSqlResourceManagers) {
      allowedTables.put(
          cloudSqlResourceManager.getDatabaseName(), tables.keySet().stream().toList());
    }
    return MySQLSource.builder(
            cloudSqlResourceManagers.get(0).getHost(),
            cloudSqlResourceManagers.get(0).getUsername(),
            cloudSqlResourceManagers.get(0).getPassword(),
            cloudSqlResourceManagers.get(0).getPort())
        .setAllowedTables(allowedTables)
        .build();
  }

  protected void generateAndUploadSessionFileUsingSMT(
      JDBCSource jdbcSourceShard,
      CloudSqlResourceManager cloudSqlResourceManager,
      SpannerResourceManager spannerResourceManager,
      GcsResourceManager gcsResourceManager)
      throws IOException, InterruptedException {
    String spannerMigrationToolPath = System.getenv("spanner_migration_tool_path");
    if (StringUtils.isBlank(spannerMigrationToolPath)) {
      throw new RuntimeException(
          "Error: spanner_migration_tool_path environment variable is not set or is empty.");
    }
    List<String> command = new ArrayList<>();
    command.add(spannerMigrationToolPath);
    command.add("schema");
    command.add("--source=MySQL");
    String sourceProfile =
        String.format(
            "host=%s,port=%s,user=%s,password=%s,dbName=%s",
            jdbcSourceShard.hostname(),
            jdbcSourceShard.port(),
            jdbcSourceShard.username(),
            jdbcSourceShard.password(),
            cloudSqlResourceManager.getDatabaseName());
    command.add("--source-profile=" + sourceProfile);
    String targetProfile =
        String.format("project=%s,instance=%s", PROJECT, spannerResourceManager.getInstanceId());
    command.add("--target-profile=" + targetProfile);
    command.add("--project=span-cloud-testing");

    ProcessBuilder processBuilder = new ProcessBuilder(command);

    Process process = processBuilder.start();
    // Regex to capture the session filename
    Pattern sessionFilePattern =
        Pattern.compile("^Wrote session to file '([^']+\\.session\\.json)'\\.?$");

    final List<String> capturedOutputLines = new ArrayList<>();
    final List<String> capturedErrorLines = new ArrayList<>();
    String[] tempCapturedSessionFileName = new String[1]; // Effectively final for lambda

    ExecutorService executor = Executors.newFixedThreadPool(2);

    // Read stdout
    Future<?> stdoutFuture =
        executor.submit(
            () -> {
              try (BufferedReader reader =
                  new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                  LOG.info("TOOL_STDOUT: " + line); // Log all output
                  capturedOutputLines.add(line);
                  Matcher matcher = sessionFilePattern.matcher(line);
                  if (matcher.find()) {
                    tempCapturedSessionFileName[0] = matcher.group(1);
                    LOG.debug(">>>> Captured session filename: " + tempCapturedSessionFileName[0]);
                  }
                }
              } catch (IOException e) {
                LOG.error("Error reading tool stdout: " + e.getMessage());
              }
            });

    // Read stderr
    Future<?> stderrFuture =
        executor.submit(
            () -> {
              try (BufferedReader reader =
                  new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                  LOG.error("TOOL_STDERR: " + line); // Log all error output
                  capturedErrorLines.add(line);
                }
              } catch (IOException e) {
                LOG.error("Error reading tool stderr: " + e.getMessage());
              }
            });

    boolean exited = process.waitFor(5, TimeUnit.MINUTES);

    // Wait for stream readers to finish
    try {
      stdoutFuture.get(1, TimeUnit.MINUTES); // Timeout for stdout reader
      stderrFuture.get(1, TimeUnit.MINUTES); // Timeout for stderr reader
    } catch (Exception e) {
      LOG.error("Timeout or error waiting for stream readers to finish: " + e.getMessage());
    }
    executor.shutdownNow(); // Terminate threads if they are still running

    if (exited) {
      if (process.exitValue() != 0) {
        throw new RuntimeException(
            "Spanner Migration Tool failed with exit code: "
                + process.exitValue()
                + "\nSTDOUT:\n"
                + String.join("\n", capturedOutputLines)
                + "\nSTDERR:\n"
                + String.join("\n", capturedErrorLines));
      }
      if (tempCapturedSessionFileName[0] != null) {
        gcsResourceManager.uploadArtifact(
            "input/session.json", Resources.getResource(tempCapturedSessionFileName[0]).getPath());
      } else {
        LOG.warn("Warning: Session filename was not found in the tool output.");
      }
    } else {
      process.destroyForcibly();
      throw new RuntimeException(
          "Spanner Migration Tool timed out."
              + "\nPartial STDOUT:\n"
              + String.join("\n", capturedOutputLines)
              + "\nPartial STDERR:\n"
              + String.join("\n", capturedErrorLines));
    }
  }
}
