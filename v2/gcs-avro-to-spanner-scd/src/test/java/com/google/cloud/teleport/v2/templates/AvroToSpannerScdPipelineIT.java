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

import static java.util.stream.Collectors.toList;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.AvroToSpannerScdPipeline.AvroToSpannerScdOptions.ScdType;
import com.google.cloud.teleport.v2.utils.StructHelper.ValueHelper.NullTypes;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({TemplateIntegrationTest.class})
@TemplateIntegrationTest(AvroToSpannerScdPipeline.class)
@RunWith(JUnit4.class)
public final class AvroToSpannerScdPipelineIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(AvroToSpannerScdPipelineIT.class);

  @Rule public TestPipeline testPipeline = TestPipeline.create();

  public static SpannerResourceManager spannerResourceManager;

  private static final String RESOURCE_DIR = "AvroToSpannerScdPipelineITTest";

  private static final String JOB_NAME =
      PipelineUtils.createJobName("AvroToSpannerScdPipelineITTest");

  @Before
  public void setUp() {
    spannerResourceManager =
        SpannerResourceManager.builder(testName, PROJECT, REGION)
            .maybeUseStaticInstance()
            .maybeUseCustomHost()
            .build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(spannerResourceManager);
  }

  /**
   * Creates DDL mutation on Spanner.
   *
   * <p>Reads the sql file from resources directory and applies the DDL to Spanner instance.
   *
   * @param resourceName SQL file name with path relative to resources directory
   */
  private void createSpannerDDL(String resourceName) throws IOException {
    String ddl =
        String.join(
            " ", Resources.readLines(Resources.getResource(resourceName), StandardCharsets.UTF_8));
    ddl = ddl.trim();
    List<String> ddls = Arrays.stream(ddl.split(";")).filter(d -> !d.isBlank()).collect(toList());
    spannerResourceManager.executeDdlStatements(ddls);
  }

  /**
   * Uploads a file to GCS for this run.
   *
   * @param filePath
   * @return GCS file path of the file.
   * @throws IOException
   */
  private String uploadResourceFileToGcs(String filePath) throws IOException {
    LOG.info("Uploading file to: {}", filePath);
    gcsClient.uploadArtifact(filePath, Resources.getResource(filePath).getPath());
    return getGcsPath(filePath);
  }

  private PipelineLauncher.LaunchInfo launchDataflowJob(Map<String, String> pipelineParams)
      throws IOException {
    LOG.info("Launching pipeline.");
    PipelineLauncher.LaunchConfig.Builder options =
        PipelineLauncher.LaunchConfig.builder(JOB_NAME, specPath);
    options.setParameters(pipelineParams);
    PipelineLauncher.LaunchInfo jobInfo = launchTemplate(options, false);
    assertThatPipeline(jobInfo).isRunning();
    LOG.info("Pipeline is running.");
    return jobInfo;
  }

  private List<Struct> readTableRows(ScdType scdType) {
    ImmutableList<String> columns;
    switch (scdType) {
      case TYPE_1:
        columns =
            ImmutableList.of("id", "first_name", "last_name", "department", "salary", "hire_date");
        break;
      case TYPE_2:
        columns =
            ImmutableList.of(
                "id",
                "first_name",
                "last_name",
                "department",
                "salary",
                "hire_date",
                "start_date",
                "end_date");
        break;
      default:
        columns = ImmutableList.of();
        break;
    }
    return spannerResourceManager.readTableRecords("employees", columns).stream().collect(toList());
  }

  private List<Struct> createSampleRows(
      AvroToSpannerScdPipeline.AvroToSpannerScdOptions.ScdType scdType) {
    List<Mutation> mutations;
    switch (scdType) {
      default:
      case TYPE_1:
        mutations =
            List.of(
                Mutation.newInsertBuilder("employees")
                    .set("id")
                    .to(9968777427L)
                    .set("first_name")
                    .to("Nadean")
                    .set("last_name")
                    .to("Macie")
                    .set("department")
                    .to("Engineering")
                    .set("salary")
                    .to(63500000.0)
                    .set("hire_date")
                    .to("1937-10-08")
                    .build(),
                Mutation.newInsertBuilder("employees")
                    .set("id")
                    .to(9970229008L)
                    .set("first_name")
                    .to("Dilan")
                    .set("last_name")
                    .to("Duayne")
                    .set("department")
                    .to("Research and Development")
                    .set("salary")
                    .to(84900000.0)
                    .set("hire_date")
                    .to("1992-02-25")
                    .build(),
                Mutation.newInsertBuilder("employees")
                    .set("id")
                    .to(9972236478L)
                    .set("first_name")
                    .to("Perry")
                    .set("last_name")
                    .to("Hollyn")
                    .set("department")
                    .to("Accounting")
                    .set("salary")
                    .to(61600000.0)
                    .set("hire_date")
                    .to("1971-09-02")
                    .build(),
                Mutation.newInsertBuilder("employees")
                    .set("id")
                    .to(9975339673L)
                    .set("first_name")
                    .to("Sophie")
                    .set("last_name")
                    .to("Danah")
                    .set("department")
                    .to("Internal Audit")
                    .set("salary")
                    .to(66300000.0)
                    .set("hire_date")
                    .to("1999-05-14")
                    .build(),
                Mutation.newInsertBuilder("employees")
                    .set("id")
                    .to(9976152507L)
                    .set("first_name")
                    .to("Hillari")
                    .set("last_name")
                    .to("Sally")
                    .set("department")
                    .to("Production")
                    .set("salary")
                    .to(31900000.0)
                    .set("hire_date")
                    .to("1973-06-03")
                    .build());
        break;
      case TYPE_2:
        mutations =
            List.of(
                Mutation.newInsertBuilder("employees")
                    .set("id")
                    .to(9968777427L)
                    .set("first_name")
                    .to("Nadean")
                    .set("last_name")
                    .to("Macie")
                    .set("department")
                    .to("Engineering")
                    .set("salary")
                    .to(63500000.0)
                    .set("hire_date")
                    .to("1937-10-08")
                    .set("start_date")
                    .to(Timestamp.parseTimestamp("2024-01-01T00:00:01.000Z"))
                    .set("end_date")
                    .to(NullTypes.NULL_TIMESTAMP)
                    .build(),
                Mutation.newInsertBuilder("employees")
                    .set("id")
                    .to(9970229008L)
                    .set("first_name")
                    .to("Dilan")
                    .set("last_name")
                    .to("Duayne")
                    .set("department")
                    .to("Research and Development")
                    .set("salary")
                    .to(84900000.0)
                    .set("hire_date")
                    .to("1992-02-25")
                    .set("start_date")
                    .to(Timestamp.parseTimestamp("2024-02-01T00:00:01.000Z"))
                    .set("end_date")
                    .to(Timestamp.parseTimestamp("2024-02-15T00:00:01.000Z"))
                    .build(),
                Mutation.newInsertBuilder("employees")
                    .set("id")
                    .to(9970229008L)
                    .set("first_name")
                    .to("Dilan")
                    .set("last_name")
                    .to("Duayne")
                    .set("department")
                    .to("Management")
                    .set("salary")
                    .to(84900000.0)
                    .set("hire_date")
                    .to("1992-02-25")
                    .set("start_date")
                    .to(Timestamp.parseTimestamp("2024-02-15T00:00:01.000Z"))
                    .set("end_date")
                    .to(NullTypes.NULL_TIMESTAMP)
                    .build(),
                Mutation.newInsertBuilder("employees")
                    .set("id")
                    .to(9972236478L)
                    .set("first_name")
                    .to("Perry")
                    .set("last_name")
                    .to("Hollyn")
                    .set("department")
                    .to("Accounting")
                    .set("salary")
                    .to(61600000.0)
                    .set("hire_date")
                    .to("1971-09-02")
                    .set("start_date")
                    .to(Timestamp.parseTimestamp("2024-03-01T00:00:01.000Z"))
                    .set("end_date")
                    .to(NullTypes.NULL_TIMESTAMP)
                    .build(),
                Mutation.newInsertBuilder("employees")
                    .set("id")
                    .to(9975339673L)
                    .set("first_name")
                    .to("Sophie")
                    .set("last_name")
                    .to("Danah")
                    .set("department")
                    .to("Internal Audit")
                    .set("salary")
                    .to(66300000.0)
                    .set("hire_date")
                    .to("1999-05-14")
                    .set("start_date")
                    .to(Timestamp.parseTimestamp("2024-04-01T00:00:01.000Z"))
                    .set("end_date")
                    .to(NullTypes.NULL_TIMESTAMP)
                    .build(),
                Mutation.newInsertBuilder("employees")
                    .set("id")
                    .to(9976152507L)
                    .set("first_name")
                    .to("Hillari")
                    .set("last_name")
                    .to("Sally")
                    .set("department")
                    .to("Production")
                    .set("salary")
                    .to(31900000.0)
                    .set("hire_date")
                    .to("1973-06-03")
                    .set("start_date")
                    .to(Timestamp.parseTimestamp("2024-05-01T00:00:01.000Z"))
                    .set("end_date")
                    .to(NullTypes.NULL_TIMESTAMP)
                    .build());
        break;
    }
    spannerResourceManager.write(mutations);
    return readTableRows(scdType);
  }

  @Test
  public void runScdType1Pipeline() throws IOException {
    createSpannerDDL(RESOURCE_DIR + "/spanner-schema-type-1.sql");
    List<Struct> initialRows = createSampleRows(ScdType.TYPE_1);
    String dataFilePath = uploadResourceFileToGcs(RESOURCE_DIR + "/data-write.avro");
    Map<String, String> pipelineParams =
        new HashMap<>() {
          {
            put("inputFilePattern", dataFilePath);
            put("instanceId", spannerResourceManager.getInstanceId());
            put("databaseId", spannerResourceManager.getDatabaseId());
            put("spannerHost", spannerResourceManager.getSpannerHost());
            put("spannerBatchSize", "2");
            put("tableName", "employees");
            put("scdType", "TYPE_1");
            put("primaryKeyColumnNames", "id");
          }
        };

    PipelineOperator.Result result =
        pipelineOperator().waitUntilDone(createConfig(launchDataflowJob(pipelineParams)));

    assertThatResult(result).isLaunchFinished();
    List<Struct> outputRecords = readTableRows(ScdType.TYPE_1);
    SpannerAsserts.assertThatStructs(outputRecords)
        // Pipeline makes 7 changes: inserts 4 new rows and updates 1 existing row once and 1
        // existing row twice. All updates are done in place.
        .hasRows(initialRows.size() + 4);
  }

  @Test
  public void runScdType2Pipeline() throws IOException {
    createSpannerDDL(RESOURCE_DIR + "/spanner-schema-type-2.sql");
    List<Struct> initialRows = createSampleRows(ScdType.TYPE_2);
    String dataFilePath = uploadResourceFileToGcs(RESOURCE_DIR + "/data-write.avro");
    Map<String, String> pipelineParams =
        new HashMap<>() {
          {
            put("inputFilePattern", dataFilePath);
            put("instanceId", spannerResourceManager.getInstanceId());
            put("databaseId", spannerResourceManager.getDatabaseId());
            put("spannerHost", spannerResourceManager.getSpannerHost());
            put("spannerBatchSize", "2");
            put("tableName", "employees");
            put("scdType", "TYPE_2");
            put("primaryKeyColumnNames", "id,end_date");
            put("startDateColumnName", "start_date");
            put("endDateColumnName", "end_date");
          }
        };

    PipelineOperator.Result result =
        pipelineOperator().waitUntilDone(createConfig(launchDataflowJob(pipelineParams)));

    assertThatResult(result).isLaunchFinished();
    List<Struct> outputRecords = readTableRows(ScdType.TYPE_2);
    SpannerAsserts.assertThatStructs(outputRecords)
        // Pipeline makes 7 changes: inserts 4 new rows and updates 1 existing row once and 1
        // existing row twice.
        // However, since it is SCD Type 2, both are handled by adding a new row.
        .hasRows(initialRows.size() + 7);
  }
}
