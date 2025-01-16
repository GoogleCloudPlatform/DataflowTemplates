package com.google.cloud.teleport.v2.templates;

import static org.apache.beam.it.gcp.artifacts.utils.ArtifactUtils.getFullGcsPath;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.common.io.Resources;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.gcp.datagenerator.DataGenerator;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.beam.it.jdbc.MySQLResourceManager;
import org.apache.beam.it.jdbc.conditions.JDBCRowsCheck;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(TemplateLoadTest.class)
@TemplateLoadTest(SpannerToSourceDb.class)
@RunWith(JUnit4.class)
public class SpannerToMySqlSourceLT extends SpannerToSourceDbLTBase {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerToMySqlSourceLT.class);

  private String generatorSchemaPath;
  private final String artifactBucket = TestProperties.artifactBucket();
  private final String spannerDdlResource = "SpannerToMySqlSourceLT/spanner-schema.sql";
  private final String sessionFileResource = "SpannerToMySqlSourceLT/session.json";
  private final String dataGeneratorSchemaResource =
      "SpannerToMySqlSourceLT/datagenerator-schema.json";
  private final String table = "Person";
  private final int maxWorkers = 1;
  private final int numWorkers = 1;
  private PipelineLauncher.LaunchInfo jobInfo;
  private final int numShards = 1;

  @Before
  public void setup() throws IOException {
    setupResourceManagers(spannerDdlResource, sessionFileResource, artifactBucket);
    setupMySQLResourceManager(numShards);
    generatorSchemaPath =
        getFullGcsPath(
            artifactBucket,
            gcsResourceManager
                .uploadArtifact(
                    "input/schema.json",
                    Resources.getResource(dataGeneratorSchemaResource).getPath())
                .name());

    createMySQLSchema(jdbcResourceManagers);
    
    // Launch the Dataflow job with resource constraints
    jobInfo = launchDataflowJob(
        artifactBucket,
        numWorkers,
        maxWorkers,
        PipelineLauncher.LaunchConfig.builder()
            .setJobName(testName)
            .setParameters(getParameters())
            .addParameter("maxNumWorkers", String.valueOf(maxWorkers))
            .addParameter("numWorkers", String.valueOf(numWorkers))
            .addParameter("autoscalingAlgorithm", "NONE") // Disable autoscaling
            .build());
  }

  @After
  public void tearDown() {
    cleanupResourceManagers();
  }

  @Test
  public void reverseReplication1KTpsLoadTest()
      throws IOException, ParseException, InterruptedException {
    // Configure data generator with minimal resources
    DataGenerator dataGenerator =
        DataGenerator.builderWithSchemaLocation(testName, generatorSchemaPath)
            .setQPS("10")
            .setMessagesLimit(String.valueOf(300000))
            .setSpannerInstanceName(spannerResourceManager.getInstanceId())
            .setSpannerDatabaseName(spannerResourceManager.getDatabaseId())
            .setSpannerTableName(table)
            .setNumWorkers("1")
            .setMaxNumWorkers("1")
            .setSinkType("SPANNER")
            .setProjectId(project)
            .setBatchSizeBytes("0")
            .build();

    dataGenerator.execute(Duration.ofMinutes(30));
    assertThatPipeline(jobInfo).isRunning();

    JDBCRowsCheck check =
        JDBCRowsCheck.builder(jdbcResourceManagers.get(0), table)
            .setMinRows(300000)
            .setMaxRows(300000)
            .build();

    PipelineOperator.Result result =
        pipelineOperator.waitForCondition(
            createConfig(jobInfo, Duration.ofMinutes(10), Duration.ofSeconds(30)), check);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    PipelineOperator.Result result1 =
        pipelineOperator.cancelJobAndFinish(createConfig(jobInfo, Duration.ofMinutes(20)));

    assertThatResult(result1).isLaunchFinished();

    exportMetrics(jobInfo, numShards);
  }

  private void createMySQLSchema(List<JDBCResourceManager> jdbcResourceManagers) {
    if (!(jdbcResourceManagers.get(0) instanceof MySQLResourceManager)) {
      throw new IllegalArgumentException(jdbcResourceManagers.get(0).getClass().getSimpleName());
    }
    MySQLResourceManager jdbcResourceManager = (MySQLResourceManager) jdbcResourceManagers.get(0);
    HashMap<String, String> columns = new HashMap<>();
    columns.put("first_name1", "varchar(500)");
    columns.put("last_name1", "varchar(500)");
    columns.put("first_name2", "varchar(500)");
    columns.put("last_name2", "varchar(500)");
    columns.put("first_name3", "varchar(500)");
    columns.put("last_name3", "varchar(500)");
    columns.put("ID", "varchar(100) NOT NULL");

    JDBCResourceManager.JDBCSchema schema = new JDBCResourceManager.JDBCSchema(columns, "ID");

    jdbcResourceManager.createTable(table, schema);
  }
}