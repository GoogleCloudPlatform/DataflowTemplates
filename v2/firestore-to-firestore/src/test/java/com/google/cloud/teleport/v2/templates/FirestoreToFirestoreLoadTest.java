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

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.gcp.artifacts.utils.ArtifactUtils.getFullGcsPath;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.common.base.MoreObjects;
import com.google.firestore.admin.v1.Database.DatabaseEdition;
import com.google.firestore.admin.v1.Database.DatabaseType;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateLoadTestBase;
import org.apache.beam.it.gcp.firestore.FirestoreAdminResourceManager;
import org.apache.beam.it.gcp.firestore.FirestoreResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance test for {@link FirestoreToFirestore Firestore to Firestore} template. */
@Category(TemplateLoadTest.class)
@TemplateLoadTest(FirestoreToFirestore.class)
@RunWith(JUnit4.class)
public class FirestoreToFirestoreLoadTest extends TemplateLoadTestBase {

  private static final String PROJECT = TestProperties.project();
  private static final String REGION = TestProperties.region();
  private static final String SOURCE_DATABASE_ID = "source-database";
  private static final String DESTINATION_DATABASE_ID = "destination-database";

  private static final String SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(), "gs://dataflow-templates/latest/Firestore_to_Firestore");
  private static final String ARTIFACT_BUCKET = TestProperties.artifactBucket();
  private static final String TEST_ROOT_DIR =
      FirestoreToFirestoreLoadTest.class.getSimpleName().toLowerCase();
  // 35,000,000 messages of the given schema make up approximately 10GB
  private static final String NUM_MESSAGES = "35000000";
  private static final String INPUT_PCOLLECTION =
      "Read all records/Read from Cloud Firestore/Read from Partitions.out0";
  private static final String OUTPUT_PCOLLECTION =
      "Write to storage/WriteFiles/RewindowIntoGlobal/Window.Assign.out0";
  private static FirestoreResourceManager sourceFirestoreResourceManager;
  private static FirestoreResourceManager destinationFirestoreResourceManager;
  private static FirestoreAdminResourceManager firestoreAdminResourceManager;
  private static GcsResourceManager gcsClient;

  @Before
  public void setup() throws IOException {
    // Set up resource managers
    firestoreAdminResourceManager =
        FirestoreAdminResourceManager.builder(testName)
            .setProject(PROJECT)
            .setRegion(REGION)
            .build();
    sourceFirestoreResourceManager =
        FirestoreResourceManager.builder(testName)
            .setProject(PROJECT)
            .setDatabase(SOURCE_DATABASE_ID)
            .setCredentials(TestProperties.googleCredentials())
            .build();
    destinationFirestoreResourceManager =
        FirestoreResourceManager.builder(testName)
            .setProject(PROJECT)
            .setDatabase(DESTINATION_DATABASE_ID)
            .setCredentials(TestProperties.googleCredentials())
            .build();
    gcsClient = GcsResourceManager.builder(ARTIFACT_BUCKET, TEST_ROOT_DIR, CREDENTIALS).build();
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(
        sourceFirestoreResourceManager,
        destinationFirestoreResourceManager,
        firestoreAdminResourceManager,
        gcsClient);
  }

  @Test
  public void testBacklog10gb() throws IOException, ParseException, InterruptedException {
    testBacklog(this::disableRunnerV2);
  }

  @Test
  public void testBacklog10gbUsingRunnerV2()
      throws IOException, ParseException, InterruptedException {
    testBacklog(this::enableRunnerV2);
  }

  public void testBacklog(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException, ParseException, InterruptedException {
    // Arrange
    String name = testName;
    firestoreAdminResourceManager.createDatabase(
        "source-" + name, DatabaseType.FIRESTORE_NATIVE, DatabaseEdition.STANDARD);
    firestoreAdminResourceManager.createDatabase(
        "destination-" + name, DatabaseType.FIRESTORE_NATIVE, DatabaseEdition.ENTERPRISE);
    // TODO: Load data with our own thing.
    // DataGenerator dataGenerator =
    //     DataGenerator.builderWithSchemaTemplate(testName, "GAME_EVENT")
    //         .setQPS("1000000")
    //         .setMessagesLimit(NUM_MESSAGES)
    //         .setSinkType("SPANNER")
    //         .setProjectId(project)
    //         .setFirestoreInstanceName(firestoreResourceManager.getInstanceId())
    //         .setFirestoreDatabaseName(firestoreResourceManager.getDatabaseId())
    //         .setFirestoreTableName(name)
    //         .setNumWorkers("50")
    //         .setMaxNumWorkers("100")
    //         .build();
    // dataGenerator.execute(Duration.ofMinutes(30));
    LaunchConfig options =
        paramsAdder
            .apply(
                LaunchConfig.builder(testName, SPEC_PATH)
                    .addParameter("sourceProjectId", PROJECT)
                    .addParameter("sourceDatabaseId", SOURCE_DATABASE_ID)
                    .addParameter("destinationProjectId", PROJECT)
                    .addParameter("destinationDatabaseId", DESTINATION_DATABASE_ID)
                    .addParameter("maxNumWorkers", "500"))
            .build();

    // Act
    LaunchInfo info = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(info).isRunning();
    Result result = pipelineOperator.waitUntilDone(createConfig(info, Duration.ofMinutes(60)));

    // Assert
    assertThatResult(result).isLaunchFinished();
    // check to see if messages reached the output bucket
    assertThat(gcsClient.listArtifacts(name, Pattern.compile(".*"))).isNotEmpty();

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION));
  }

  private String getTestMethodDirPath() {
    return getFullGcsPath(ARTIFACT_BUCKET, TEST_ROOT_DIR, gcsClient.runId(), testName);
  }
}
