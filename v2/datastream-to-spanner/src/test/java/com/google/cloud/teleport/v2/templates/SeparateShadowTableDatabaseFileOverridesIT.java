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

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.spanner.matchers.SpannerAsserts;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.testcontainers.shaded.com.google.common.io.Resources;

/**
 * An integration test using separate shadow table database for {@link DataStreamToSpanner} Flex
 * template which test use-cases where an override is provided by an override file.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class SeparateShadowTableDatabaseFileOverridesIT extends DataStreamToSpannerITBase {

  private static final String MYSQL_TABLE = "person1";

  private static final String SPANNER_TABLE = "human1";

  private static final String SPANNER_DDL_RESOURCE =
      "DataStreamToSpannerFileOverridesIT/spanner-schema.sql";

  private static final String OVERRIDE_FILE = "DataStreamToSpannerFileOverridesIT/override.json";

  private static final String GCS_PATH_PREFIX = "SeparateShadowTableDatabaseFileOverridesIT";

  private static PipelineLauncher.LaunchInfo jobInfo;

  private static HashSet<SeparateShadowTableDatabaseFileOverridesIT> testInstances =
      new HashSet<>();

  public static PubsubResourceManager pubsubResourceManager;

  public static SpannerResourceManager spannerResourceManager;
  public static SpannerResourceManager shadowSpannerResourceManager;
  public static GcsResourceManager gcsResourceManager;

  /**
   * Setup resource managers and Launch dataflow job once during the execution of this test class.
   *
   * @throws IOException
   */
  @Before
  public void setUp() throws IOException {
    // Prevent cleaning up of dataflow job after a test method is executed.
    skipBaseCleanup = true;
    synchronized (SeparateShadowTableDatabaseFileOverridesIT.class) {
      testInstances.add(this);
      if (jobInfo == null) {
        spannerResourceManager = setUpSpannerResourceManager();
        shadowSpannerResourceManager = setUpShadowSpannerResourceManager();
        pubsubResourceManager = setUpPubSubResourceManager();
        gcsResourceManager = setUpSpannerITGcsResourceManager();
        createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);
        gcsResourceManager.uploadArtifact(
            GCS_PATH_PREFIX + "/override.json", Resources.getResource(OVERRIDE_FILE).getPath());
        Map<String, String> overridesMap =
            new HashMap<>() {
              {
                put("shadowTableSpannerInstanceId", shadowSpannerResourceManager.getInstanceId());
                put("shadowTableSpannerDatabaseId", shadowSpannerResourceManager.getDatabaseId());
                put(
                    "schemaOverridesFilePath",
                    getGcsPath(GCS_PATH_PREFIX + "/override.json", gcsResourceManager));
              }
            };
        jobInfo =
            launchDataflowJob(
                getClass().getSimpleName(),
                null,
                null,
                GCS_PATH_PREFIX,
                spannerResourceManager,
                pubsubResourceManager,
                overridesMap,
                null,
                null,
                gcsResourceManager);
      }
    }
  }

  /**
   * Cleanup dataflow job and all the resources and resource managers.
   *
   * @throws IOException
   */
  @AfterClass
  public static void cleanUp() throws IOException {
    for (SeparateShadowTableDatabaseFileOverridesIT instance : testInstances) {
      instance.tearDownBase();
    }
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        pubsubResourceManager,
        shadowSpannerResourceManager,
        gcsResourceManager);
  }

  @Test
  public void migrationTestWithRenameTableAndColumns() {
    // Construct a ChainedConditionCheck with 2 stages.
    // 1. Send initial wave of events
    // 2. Wait on Spanner to have events
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    uploadDataStreamFile(
                        jobInfo,
                        MYSQL_TABLE,
                        "cdc_person1.avro",
                        "DataStreamToSpannerFileOverridesIT/mysql-cdc-person1.avro",
                        gcsResourceManager),
                    SpannerRowsCheck.builder(spannerResourceManager, SPANNER_TABLE)
                        .setMinRows(2)
                        .setMaxRows(2)
                        .build()))
            .build();

    // Wait for conditions
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(createConfig(jobInfo, Duration.ofMinutes(8)), conditionCheck);

    // Assert conditions
    assertThatResult(result).meetsConditions();
    assertHumanTableContents();
  }

  private void assertHumanTableContents() {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> row1 = new HashMap<>();
    // assert content with the changed column name
    row1.put("name1", "John");
    row1.put("last_name1", "Doe");

    Map<String, Object> row2 = new HashMap<>();
    row2.put("name1", "Alice");
    row2.put("last_name1", "Johnson");

    events.add(row1);
    events.add(row2);

    // query the table with the name changed as per the override
    SpannerAsserts.assertThatStructs(
            spannerResourceManager.runQuery("select name1, last_name1 from human1"))
        .hasRecordsUnorderedCaseInsensitiveColumns(events);
  }
}
