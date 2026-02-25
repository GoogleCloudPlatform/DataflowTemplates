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
package com.google.cloud.teleport.v2.templates;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;

import com.google.cloud.datastream.v1.DestinationConfig;
import com.google.cloud.datastream.v1.SourceConfig;
import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.CustomTransformation;
import com.google.common.io.Resources;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.utils.IORedirectUtil;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager.DestinationOutputFormat;
import org.apache.beam.it.gcp.datastream.JDBCSource;
import org.apache.beam.it.gcp.datastream.OracleSource;
import org.apache.beam.it.gcp.datastream.PostgresqlSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for DataStreamToSpanner integration tests. It provides helper functions related to
 * environment setup and assertConditions.
 */
public abstract class DataStreamToSpannerITBase extends TemplateTestBase {

  // Format of avro file path in GCS - {table}/2023/12/20/06/57/{fileName}
  public static final String DATA_STREAM_EVENT_FILES_PATH_FORMAT_IN_GCS = "%s/2023/12/20/06/57/%s";
  private static final Logger LOG = LoggerFactory.getLogger(DataStreamToSpannerITBase.class);

  public static final int CUTOVER_MILLIS = 30 * 1000;

  public PubsubResourceManager setUpPubSubResourceManager() throws IOException {
    return PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
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

  public String generateSessionFile(
      int numOfTables, String srcDb, String spannerDb, List<String> tableNames, String sessionFile)
      throws IOException {
    String sessionFileContent =
        Resources.toString(Resources.getResource(sessionFile), StandardCharsets.UTF_8);
    sessionFileContent =
        sessionFileContent.replaceAll("SRC_DATABASE", srcDb).replaceAll("SP_DATABASE", spannerDb);
    for (int i = 1; i <= numOfTables; i++) {
      sessionFileContent = sessionFileContent.replaceAll("TABLE" + i, tableNames.get(i - 1));
    }
    return sessionFileContent;
  }

  public SpannerResourceManager setUpShadowSpannerResourceManager() {
    // Create a separate spanner resource manager with different db name for shadow tables.
    SpannerResourceManager sp =
        SpannerResourceManager.builder("shadow_" + testName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .build();
    // Set up the Spanner instance and database with the empty DDL for the resource manager.
    sp.ensureUsableAndCreateResources();
    return sp;
  }

  /**
   * Helper function for creating Spanner DDL. Reads the sql file from resources directory and
   * applies the DDL to Spanner instance.
   *
   * @param spannerResourceManager Initialized SpannerResourceManager instance
   * @param resourceName SQL file name with path relative to resources directory
   */
  public static void createSpannerDDL(
      SpannerResourceManager spannerResourceManager, String resourceName) throws IOException {
    String ddl =
        String.join(
            " ", Resources.readLines(Resources.getResource(resourceName), StandardCharsets.UTF_8));
    ddl = ddl.trim();
    List<String> ddls = Arrays.stream(ddl.split(";")).toList();
    spannerResourceManager.executeDdlStatements(ddls);
  }

  /**
   * Helper function for executing Spanner DML. Reads the sql file from resources directory and
   * applies the DML to Spanner instance.
   *
   * @param spannerResourceManager Initialized SpannerResourceManager instance
   * @param resourceName SQL file name with path relative to resources directory
   */
  public static void executeSpannerDML(
      SpannerResourceManager spannerResourceManager, String resourceName) throws IOException {
    String dml =
        String.join(
            " ", Resources.readLines(Resources.getResource(resourceName), StandardCharsets.UTF_8));
    dml = dml.trim();
    List<String> dmls = Arrays.stream(dml.split(";")).toList();
    spannerResourceManager.executeDMLStatements(dmls);
  }

  /**
   * Helper function for creating all pubsub resources required by DataStreamToSpanner template.
   * PubSub topic, Subscription and notification setup on a GCS bucket with gcsPrefix filter.
   *
   * @param pubsubResourceManager Initialized PubSubResourceManager instance
   * @param gcsPrefix Prefix of Avro file names in GCS relative to bucket name
   * @return SubscriptionName object of the created PubSub subscription.
   */
  public SubscriptionName createPubsubResources(
      String identifierSuffix,
      PubsubResourceManager pubsubResourceManager,
      String gcsPrefix,
      GcsResourceManager gcsResourceManager) {
    String topicNameSuffix = "it" + identifierSuffix;
    String subscriptionNameSuffix = "it-sub" + identifierSuffix;
    TopicName topic = pubsubResourceManager.createTopic(topicNameSuffix);
    SubscriptionName subscription =
        pubsubResourceManager.createSubscription(topic, subscriptionNameSuffix);
    String prefix = gcsPrefix;
    if (prefix.startsWith("/")) {
      prefix = prefix.substring(1);
    }
    gcsResourceManager.createNotification(topic.toString(), prefix);
    return subscription;
  }

  /**
   * Helper function for constructing a ConditionCheck whose check() method uploads a given file to
   * GCS with path similar to DataStream generated file path.
   *
   * @param jobInfo Dataflow job info
   * @param table
   * @param destinationFileName
   * @param resourceName Avro file name with path relative to resources directory
   * @return A ConditionCheck containing the GCS Upload operation.
   */
  public ConditionCheck uploadDataStreamFile(
      LaunchInfo jobInfo,
      String table,
      String destinationFileName,
      String resourceName,
      GcsResourceManager gcsResourceManager) {
    return new ConditionCheck() {
      @Override
      protected String getDescription() {
        return "Upload DataStream files.";
      }

      @Override
      protected CheckResult check() {
        boolean success = true;
        String message = String.format("Successfully uploaded %s file to GCS", resourceName);
        try {
          // Get destination GCS path from the dataflow job parameter.
          String destinationPath =
              jobInfo
                  .parameters()
                  .get("inputFilePattern")
                  .replace("gs://" + gcsResourceManager.getBucket() + "/", "");
          destinationPath =
              destinationPath
                  + String.format(
                      DATA_STREAM_EVENT_FILES_PATH_FORMAT_IN_GCS, table, destinationFileName);
          gcsResourceManager.copyFileToGcs(
              Paths.get(Resources.getResource(resourceName).getPath()), destinationPath);
        } catch (IOException e) {
          success = false;
          message = e.getMessage();
        }

        return new CheckResult(success, message);
      }
    };
  }

  /**
   * Performs the following steps: Uploads session file to GCS. Creates Pubsub resources. Launches
   * DataStreamToSpanner dataflow job.
   *
   * @param identifierSuffix will be used as postfix in generated resource ids
   * @param sessionFileResourceName Session file name with path relative to resources directory
   * @param transformationContextFileResourceName Transformation context file name with path
   *     relative to resources directory
   * @param gcsPathPrefix Prefix directory name for this DF job. Data and DLQ directories will be
   *     created under this prefix.
   * @return dataflow jobInfo object
   * @throws IOException
   */
  protected LaunchInfo launchDataflowJob(
      String identifierSuffix,
      String sessionFileResourceName,
      String transformationContextFileResourceName,
      String gcsPathPrefix,
      SpannerResourceManager spannerResourceManager,
      PubsubResourceManager pubsubResourceManager,
      Map<String, String> jobParameters,
      CustomTransformation customTransformation,
      String shardingContextFileResourceName,
      GcsResourceManager gcsResourceManager)
      throws IOException {
    return launchDataflowJob(
        identifierSuffix,
        sessionFileResourceName,
        transformationContextFileResourceName,
        gcsPathPrefix,
        spannerResourceManager,
        pubsubResourceManager,
        jobParameters,
        customTransformation,
        shardingContextFileResourceName,
        gcsResourceManager,
        null,
        null,
        null);
  }

  protected LaunchInfo launchDataflowJob(
      String identifierSuffix,
      String sessionFileResourceName,
      String transformationContextFileResourceName,
      String gcsPathPrefix,
      SpannerResourceManager spannerResourceManager,
      PubsubResourceManager pubsubResourceManager,
      Map<String, String> jobParameters,
      CustomTransformation customTransformation,
      String shardingContextFileResourceName,
      GcsResourceManager gcsResourceManager,
      DatastreamResourceManager datastreamResourceManager,
      String sessionResourceContent,
      JDBCSource jdbcSource)
      throws IOException {

    LOG.info("Starting Dataflow job launch for identifier: {}", identifierSuffix);
    LOG.info("GCS Path Prefix: {}", gcsPathPrefix);

    if (sessionFileResourceName != null) {
      LOG.info("Uploading session file from resource: {}", sessionFileResourceName);
      gcsResourceManager.uploadArtifact(
          gcsPathPrefix + "/session.json",
          Resources.getResource(sessionFileResourceName).getPath());
    } else {
      LOG.info("No session file resource name provided, skipping upload.");
    }

    if (sessionResourceContent != null) {
      LOG.info("Creating session file from content.");
      gcsResourceManager.createArtifact(gcsPathPrefix + "/session.json", sessionResourceContent);
    } else {
      LOG.info("No session file content provided, skipping creation.");
    }

    if (transformationContextFileResourceName != null) {
      LOG.info(
          "Uploading transformation context file from resource: {}",
          transformationContextFileResourceName);
      gcsResourceManager.uploadArtifact(
          gcsPathPrefix + "/transformationContext.json",
          Resources.getResource(transformationContextFileResourceName).getPath());
    } else {
      LOG.info("No transformation context file provided, skipping upload.");
    }

    if (shardingContextFileResourceName != null) {
      LOG.info(
          "Uploading sharding context file from resource: {}", shardingContextFileResourceName);
      gcsResourceManager.uploadArtifact(
          gcsPathPrefix + "/shardingContext.json",
          Resources.getResource(shardingContextFileResourceName).getPath());
    } else {
      LOG.info("No sharding context file provided, skipping upload.");
    }

    String gcsPrefix =
        getGcsPath(gcsPathPrefix + "/cdc/", gcsResourceManager)
            .replace("gs://" + gcsResourceManager.getBucket(), "");
    SubscriptionName subscription =
        createPubsubResources(
            identifierSuffix, pubsubResourceManager, gcsPrefix, gcsResourceManager);

    String dlqGcsPrefix =
        getGcsPath(gcsPathPrefix + "/dlq/", gcsResourceManager)
            .replace("gs://" + gcsResourceManager.getBucket(), "");
    SubscriptionName dlqSubscription =
        createPubsubResources(
            identifierSuffix + "dlq", pubsubResourceManager, dlqGcsPrefix, gcsResourceManager);

    // default parameters
    Map<String, String> params =
        new HashMap<>() {
          {
            put("inputFilePattern", getGcsPath(gcsPathPrefix + "/cdc/", gcsResourceManager));
            put("instanceId", spannerResourceManager.getInstanceId());
            put("databaseId", spannerResourceManager.getDatabaseId());
            put("projectId", PROJECT);
            put(
                "deadLetterQueueDirectory",
                getGcsPath(gcsPathPrefix + "/dlq/", gcsResourceManager));
            put("gcsPubSubSubscription", subscription.toString());
            put("dlqGcsPubSubSubscription", dlqSubscription.toString());
            put("inputFileFormat", "avro");
            put("workerMachineType", "n2-standard-4");
          }
        };

    if (jdbcSource != null) {
      if (jdbcSource instanceof PostgresqlSource) {
        params.put("datastreamSourceType", "postgresql");
      } else if (jdbcSource instanceof OracleSource) {
        params.put("datastreamSourceType", "oracle");
      } else {
        params.put("datastreamSourceType", "mysql");
      }
    } else {
      params.put("datastreamSourceType", "mysql");
    }

    if (jdbcSource != null) {
      LOG.info("JDBC source provided. Creating Datastream stream...");
      LOG.info("Datastream GCS Destination Prefix: {}", gcsPrefix);
      LOG.info("Datastream JDBC Source: {}", jdbcSource);
      params.put(
          "streamName",
          createDataStream(
                  datastreamResourceManager,
                  gcsResourceManager,
                  gcsPrefix,
                  jdbcSource,
                  DestinationOutputFormat.AVRO_FILE_FORMAT)
              .getName());
      LOG.info("Successfully created Datastream stream and added to parameters.");
    } else {
      LOG.info("No JDBC source provided, skipping Datastream stream creation.");
    }

    if (sessionFileResourceName != null || sessionResourceContent != null) {
      params.put(
          "sessionFilePath", getGcsPath(gcsPathPrefix + "/session.json", gcsResourceManager));
    }

    if (transformationContextFileResourceName != null) {
      params.put(
          "transformationContextFilePath",
          getGcsPath(gcsPathPrefix + "/transformationContext.json", gcsResourceManager));
    }

    if (shardingContextFileResourceName != null) {
      params.put(
          "shardingContextFilePath",
          getGcsPath(gcsPathPrefix + "/shardingContext.json", gcsResourceManager));
    }

    if (customTransformation != null) {
      LOG.info("Custom transformation provided: {}", customTransformation.classPath());
      params.put(
          "transformationJarPath",
          getGcsPath(gcsPathPrefix + "/" + customTransformation.jarPath(), gcsResourceManager));
      params.put("transformationClassName", customTransformation.classPath());
    } else {
      LOG.info("No custom transformation provided.");
    }

    // overridden parameters
    if (jobParameters != null) {
      for (Entry<String, String> entry : jobParameters.entrySet()) {
        params.put(entry.getKey(), entry.getValue());
      }
    }

    // Construct template
    String jobName = PipelineUtils.createJobName(identifierSuffix);
    LaunchConfig.Builder options = LaunchConfig.builder(jobName, specPath);

    options.setParameters(params);
    options.addEnvironment("ipConfiguration", "WORKER_IP_PRIVATE");

    // Run
    LOG.info("Launching Dataflow job with parameters: {}", params);
    LaunchInfo jobInfo = launchTemplate(options);
    assertThatPipeline(jobInfo).isRunning();

    return jobInfo;
  }

  public void createAndUploadJarToGcs(String gcsPathPrefix, GcsResourceManager gcsResourceManager)
      throws IOException, InterruptedException {
    String[] shellCommand = {"/bin/bash", "-c", "cd ../spanner-custom-shard"};

    Process exec = Runtime.getRuntime().exec(shellCommand);

    IORedirectUtil.redirectLinesLog(exec.getInputStream(), LOG);
    IORedirectUtil.redirectLinesLog(exec.getErrorStream(), LOG);

    if (exec.waitFor() != 0) {
      throw new RuntimeException("Error staging template, check Maven logs.");
    }
    gcsResourceManager.uploadArtifact(
        gcsPathPrefix + "/customTransformation.jar",
        "../spanner-custom-shard/target/spanner-custom-shard-1.0-SNAPSHOT.jar");
  }

  public static Map<String, Object> createSessionTemplate(
      int numTables,
      List<Map<String, Object>> columnConfigs,
      List<Map<String, Object>> primaryKeyConfig) {
    Map<String, Object> sessionTemplate = new LinkedHashMap<>();
    sessionTemplate.put("SessionName", "NewSession");
    sessionTemplate.put("EditorName", "");
    sessionTemplate.put("DatabaseType", "mysql");
    sessionTemplate.put("DatabaseName", "SP_DATABASE");
    sessionTemplate.put("Dialect", "google_standard_sql");
    sessionTemplate.put("Notes", null);
    sessionTemplate.put("Tags", null);
    sessionTemplate.put("SpSchema", new LinkedHashMap<>());
    sessionTemplate.put("SyntheticPKeys", new LinkedHashMap<>());
    sessionTemplate.put("SrcSchema", new LinkedHashMap<>());
    sessionTemplate.put("SchemaIssues", new LinkedHashMap<>());
    sessionTemplate.put("Location", new LinkedHashMap<>());
    sessionTemplate.put("TimezoneOffset", "+00:00");
    sessionTemplate.put("SpDialect", "google_standard_sql");
    sessionTemplate.put("UniquePKey", new LinkedHashMap<>());
    sessionTemplate.put("Rules", new ArrayList<>());
    sessionTemplate.put("IsSharded", false);
    sessionTemplate.put("SpRegion", "");
    sessionTemplate.put("ResourceValidation", false);
    sessionTemplate.put("UI", false);

    for (int i = 1; i <= numTables; i++) {
      String tableName = "TABLE" + i;
      List<String> colIds = new ArrayList<>();
      Map<String, Object> colDefs = new LinkedHashMap<>();

      for (int j = 0; j < columnConfigs.size(); j++) {
        Map<String, Object> colConfig = columnConfigs.get(j);
        String colId = (String) colConfig.getOrDefault("id", "c" + (j + 1));
        colIds.add(colId);

        Map<String, Object> colType = new LinkedHashMap<>();
        colType.put("Name", colConfig.getOrDefault("Type", "STRING"));
        colType.put("Len", colConfig.getOrDefault("Length", 0));
        colType.put("IsArray", colConfig.getOrDefault("IsArray", false));

        Map<String, Object> column = new LinkedHashMap<>();
        column.put("Name", colConfig.getOrDefault("Name", "column_" + (j + 1)));
        column.put("T", colType);
        column.put("NotNull", colConfig.getOrDefault("NotNull", false));
        column.put("Comment", colConfig.getOrDefault("Comment", ""));
        column.put("Id", colId);
        colDefs.put(colId, column);
      }

      List<Map<String, Object>> primaryKeys = new ArrayList<>();
      for (Map<String, Object> pk : primaryKeyConfig) {
        Map<String, Object> pkEntry = new LinkedHashMap<>();
        pkEntry.put("ColId", pk.get("ColId"));
        pkEntry.put("Desc", pk.getOrDefault("Desc", false));
        pkEntry.put("Order", pk.getOrDefault("Order", 1));
        primaryKeys.add(pkEntry);
      }

      Map<String, Object> spSchemaEntry = new LinkedHashMap<>();
      spSchemaEntry.put("Name", tableName);
      spSchemaEntry.put("ColIds", colIds);
      spSchemaEntry.put("ShardIdColumn", "");
      spSchemaEntry.put("ColDefs", colDefs);
      spSchemaEntry.put("PrimaryKeys", primaryKeys);
      spSchemaEntry.put("ForeignKeys", null);
      spSchemaEntry.put("Indexes", null);
      spSchemaEntry.put("ParentId", "");
      spSchemaEntry.put("Comment", "Spanner schema for source table " + tableName);
      spSchemaEntry.put("Id", "t" + i);
      ((Map<String, Object>) sessionTemplate.get("SpSchema")).put("t" + i, spSchemaEntry);

      Map<String, Object> srcSchemaEntry = new LinkedHashMap<>(spSchemaEntry);
      srcSchemaEntry.put("Schema", "SRC_DATABASE");
      ((Map<String, Object>) sessionTemplate.get("SrcSchema")).put("t" + i, srcSchemaEntry);

      Map<String, Object> schemaIssuesEntry = new LinkedHashMap<>();
      schemaIssuesEntry.put("ColumnLevelIssues", new LinkedHashMap<>());
      schemaIssuesEntry.put("TableLevelIssues", null);
      ((Map<String, Object>) sessionTemplate.get("SchemaIssues")).put("t" + i, schemaIssuesEntry);
    }

    return sessionTemplate;
  }

  /** Helper function for checking the rows of the destination Spanner tables. */
  public static void checkSpannerTables(
      SpannerResourceManager spannerResourceManager,
      List<String> tableNames,
      Map<String, List<Map<String, Object>>> cdcEvents,
      List<String> cols) {
    tableNames.forEach(
        tableName -> {
          SpannerAsserts.assertThatStructs(spannerResourceManager.readTableRecords(tableName, cols))
              .hasRecordsUnorderedCaseInsensitiveColumns(cdcEvents.get(tableName));
        });
  }

  protected Stream createDataStream(
      DatastreamResourceManager datastreamResourceManager,
      GcsResourceManager gcsResourceManager,
      String gcsPrefix,
      JDBCSource jdbcSource,
      DatastreamResourceManager.DestinationOutputFormat destinationOutputFormat) {
    try {
      SourceConfig sourceConfig =
          datastreamResourceManager.buildJDBCSourceConfig("jdbc-profile", jdbcSource);

      DestinationConfig destinationConfig =
          datastreamResourceManager.buildGCSDestinationConfig(
              "gcs-profile", gcsResourceManager.getBucket(), gcsPrefix, destinationOutputFormat);

      Stream stream =
          datastreamResourceManager.createStream("stream1", sourceConfig, destinationConfig);
      datastreamResourceManager.startStream(stream);
      return stream;
    } catch (Exception e) {
      LOG.error("Error while creating datastream", e);
      throw e;
    }
  }

  protected void executeSqlScript(JDBCResourceManager resourceManager, String resourceName)
      throws IOException {
    String ddl =
        String.join(
            " ", Resources.readLines(Resources.getResource(resourceName), StandardCharsets.UTF_8));
    ddl = ddl.trim();
    List<String> ddls = Arrays.stream(ddl.split(";")).toList();
    for (String d : ddls) {
      if (!d.isBlank()) {
        try {
          if (d.toLowerCase().trim().startsWith("select")) {
            resourceManager.runSQLQuery(d);
          } else {
            resourceManager.runSQLUpdate(d);
          }
        } catch (Exception e) {
          LOG.error("Exception while executing DDL {}", d, e);
          throw e;
        }
      }
    }
  }
}
