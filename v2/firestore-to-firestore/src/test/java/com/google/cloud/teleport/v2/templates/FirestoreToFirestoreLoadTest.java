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
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateLoadTestBase;
import org.apache.beam.it.gcp.firestore.FirestoreAdminResourceManager;
import org.apache.beam.it.gcp.firestore.FirestoreResourceManager;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.io.gcp.firestore.RpcQosOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
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
  private static final String SOURCE_DATABASE_ID = "loadtest-source";
  private static final String DESTINATION_DATABASE_ID = "loadtest-destination";
  private static final String COLLECTION_ID = "it-load-data";

  private static final String SPEC_PATH =
      MoreObjects.firstNonNull(
          TestProperties.specPath(), "gs://dataflow-templates/latest/Firestore_to_Firestore");
  private static final String ARTIFACT_BUCKET = TestProperties.artifactBucket();
  private static final String TEST_ROOT_DIR =
      FirestoreToFirestoreLoadTest.class.getSimpleName().toLowerCase();

  private static final double LOAD_DATA_TARGET_SIZE_GIB = 0.01;
  private static final int LOAD_DATA_MAX_WORKERS = 500;
  private static final String INPUT_PCOLLECTION =
      "Read all records/Read from Cloud Firestore/Read from Partitions.out0";
  private static final String OUTPUT_PCOLLECTION =
      "Write to storage/WriteFiles/RewindowIntoGlobal/Window.Assign.out0";
  private static FirestoreResourceManager sourceFirestoreResourceManager;
  private static FirestoreResourceManager destinationFirestoreResourceManager;
  private static FirestoreAdminResourceManager firestoreAdminResourceManager;
  // TODO: Do we really need this?
  private static GcsResourceManager gcsClient;

  @Rule public TestPipeline pipeline = TestPipeline.create();

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
        true,
        sourceFirestoreResourceManager,
        destinationFirestoreResourceManager,
        firestoreAdminResourceManager,
        gcsClient);
  }

  @Test
  public void testBacklog10mb() throws IOException, ParseException, InterruptedException {
    testPipeline(this::disableRunnerV2);
  }

  @Test
  public void testBacklog10mbUsingRunnerV2()
      throws IOException, ParseException, InterruptedException {
    testPipeline(this::enableRunnerV2);
  }

  public void testPipeline(Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException, ParseException, InterruptedException {
    if (false) {
      firestoreAdminResourceManager.createDatabase(
          SOURCE_DATABASE_ID, DatabaseType.FIRESTORE_NATIVE, DatabaseEdition.STANDARD);
      firestoreAdminResourceManager.createDatabase(
          DESTINATION_DATABASE_ID, DatabaseType.FIRESTORE_NATIVE, DatabaseEdition.ENTERPRISE);
    }
    generateLoad();

    LaunchInfo info =
        pipelineLauncher.launch(
            project,
            region,
            paramsAdder
                .apply(
                    LaunchConfig.builder(testName, SPEC_PATH)
                        .addParameter("sourceProjectId", PROJECT)
                        .addParameter("sourceDatabaseId", SOURCE_DATABASE_ID)
                        .addParameter("destinationProjectId", PROJECT)
                        .addParameter("destinationDatabaseId", DESTINATION_DATABASE_ID)
                        .addParameter("maxNumWorkers", "500"))
                .build());
    assertThatPipeline(info).isRunning();

    Result result = pipelineOperator.waitUntilDone(createConfig(info, Duration.ofMinutes(60)));
    assertThatResult(result).isLaunchFinished();

    long destDocCount = destinationFirestoreResourceManager.readDocCount(COLLECTION_ID);
    assertThat(destDocCount).isGreaterThan(0);

    // export results
    exportMetricsToBigQuery(info, getMetrics(info, INPUT_PCOLLECTION, OUTPUT_PCOLLECTION));
  }

  private void generateLoad() {

    DataflowPipelineOptions options = pipeline.getOptions().as(DataflowPipelineOptions.class);

    // Set the streaming option to prevent OOMing on too much data.
    options.setStreaming(true);
    pipeline.apply(
        "GenerateFirestoreLoad",
        new FirestoreLoadGeneratorTransform(
            PROJECT,
            SOURCE_DATABASE_ID,
            COLLECTION_ID,
            LOAD_DATA_TARGET_SIZE_GIB,
            RpcQosOptions.newBuilder().withHintMaxNumWorkers(LOAD_DATA_MAX_WORKERS).build()));

    pipeline.run();

    long sourceDocCount = sourceFirestoreResourceManager.readDocCount(COLLECTION_ID);
    assertThat(sourceDocCount).isGreaterThan(0);
    // ... assertions after pipeline run ...
  }
}
