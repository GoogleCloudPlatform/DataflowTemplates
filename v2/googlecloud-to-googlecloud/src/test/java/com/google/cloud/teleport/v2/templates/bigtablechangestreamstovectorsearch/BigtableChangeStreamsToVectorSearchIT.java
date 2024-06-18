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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstovectorsearch;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigtable.admin.v2.models.StorageType;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.Lists;
import com.google.common.primitives.Floats;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigtable.BigtableResourceManager;
import org.apache.beam.it.gcp.bigtable.BigtableResourceManagerCluster;
import org.apache.beam.it.gcp.bigtable.BigtableTableSpec;
import org.apache.commons.lang3.RandomUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link BigtableChangeStreamsToVectorSearch}. */
@Category({TemplateIntegrationTest.class})
@TemplateIntegrationTest(BigtableChangeStreamsToVectorSearch.class)
@RunWith(JUnit4.class)
@Ignore("Tests are flaky as indexes take forever to be created causing timeouts.")
public final class BigtableChangeStreamsToVectorSearchIT extends TemplateTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(BigtableChangeStreamsToVectorSearchIT.class);

  private static final String TEST_ZONE = "us-central1-b";
  private static final String PROJECT_NUMBER = "269744978479"; // cloud-teleport-testing
  private BigtableResourceManager bigtableResourceManager;
  private static VectorSearchResourceManager vectorSearchResourceManager;

  private static final String clusterName = "teleport-c1";
  private String appProfileId;
  private String srcTable;

  // Fixture embeddings for tests, which match the dimensionality of the test index
  private static final List<Float> GOOD_EMBEDDINGS =
      Floats.asList(0.01f, 0.02f, 0.03f, 0.04f, 0.05f, 0.06f, 0.07f, 0.08f, 0.09f, 0.10f);

  // The GOOD_EMBEDDINGS list of floats converted to a byte array of single-precision (4 byte)
  // big-endian floats, as they would be sent to CBT
  private static final byte[] GOOD_EMBEDDING_BYTES = {
    (byte) 0x3c, (byte) 0x23, (byte) 0xd7, (byte) 0x0a, // 0.01
    (byte) 0x3c, (byte) 0xa3, (byte) 0xd7, (byte) 0x0a, // 0.02
    (byte) 0x3c, (byte) 0xf5, (byte) 0xc2, (byte) 0x8f, // 0.03
    (byte) 0x3d, (byte) 0x23, (byte) 0xd7, (byte) 0x0a, // 0.04
    (byte) 0x3d, (byte) 0x4c, (byte) 0xcc, (byte) 0xcd, // 0.05
    (byte) 0x3d, (byte) 0x75, (byte) 0xc2, (byte) 0x8f, // 0.06
    (byte) 0x3d, (byte) 0x8f, (byte) 0x5c, (byte) 0x29, // 0.07
    (byte) 0x3d, (byte) 0xa3, (byte) 0xd7, (byte) 0x0a, // 0.08
    (byte) 0x3d, (byte) 0xb8, (byte) 0x51, (byte) 0xec, // 0.09
    (byte) 0x3d, (byte) 0xcc, (byte) 0xcc, (byte) 0xcd, // 0.10
  };

  // A list of embeddings that is too short, and should produce an error on sync
  private static final List<Float> BAD_EMBEDDINGS =
      Floats.asList(0.01f, 0.02f, 0.03f, 0.04f, 0.05f);

  // The BAD_EMBEDDINGS list of floats converted to a byte array  of single-precision (4 byte)
  // big-endian floats, as they would be sent to CBT
  private static final byte[] BAD_EMBEDDING_BYTES = {
    (byte) 0x3c, (byte) 0x23, (byte) 0xd7, (byte) 0x0a, // 0.01
    (byte) 0x3c, (byte) 0xa3, (byte) 0xd7, (byte) 0x0a, // 0.02
    (byte) 0x3c, (byte) 0xf5, (byte) 0xc2, (byte) 0x8f, // 0.03
    (byte) 0x3d, (byte) 0x23, (byte) 0xd7, (byte) 0x0a, // 0.04
    (byte) 0x3d, (byte) 0x4c, (byte) 0xcc, (byte) 0xcd, // 0.05
  };

  private static final String EMBEDDING_BYTE_SIZE = "4";

  // Columns for writing to CBT and their ByteString equivalent, so we don't continually have to
  // pass them through ByteString.copyFrom(...) in tests
  private static final String SOURCE_COLUMN_FAMILY = "cf";
  private static final String EMBEDDING_COLUMN_NAME = "embeddings";
  private static final String CROWDING_TAG_COLUMN_NAME = "crowding_tag";
  private static final String ALLOW_RESTRICTS_COLUMN_NAME = "allow";
  private static final String DENY_RESTRICTS_COLUMN_NAME = "deny";
  private static final String INT_RESTRICTS_COLUMN_NAME = "int-restrict";
  private static final String FLOAT_RESTRICTS_COLUMN_NAME = "float-restrict";
  private static final String DOUBLE_RESTRICTS_COLUMN_NAME = "double-restrict";

  private static final ByteString EMBEDDING_COLUMN =
      ByteString.copyFrom(EMBEDDING_COLUMN_NAME, Charset.defaultCharset());
  private static final ByteString CROWDING_TAG_COLUMN =
      ByteString.copyFrom(CROWDING_TAG_COLUMN_NAME, Charset.defaultCharset());
  private static final ByteString ALLOW_RESTRICTS_COLUMN =
      ByteString.copyFrom(ALLOW_RESTRICTS_COLUMN_NAME, Charset.defaultCharset());
  private static final ByteString DENY_RESTRICTS_COLUMN =
      ByteString.copyFrom(DENY_RESTRICTS_COLUMN_NAME, Charset.defaultCharset());
  private static final ByteString INT_RESTRICTS_COLUMN =
      ByteString.copyFrom(INT_RESTRICTS_COLUMN_NAME, Charset.defaultCharset());
  private static final ByteString FLOAT_RESTRICTS_COLUMN =
      ByteString.copyFrom(FLOAT_RESTRICTS_COLUMN_NAME, Charset.defaultCharset());
  private static final ByteString DOUBLE_RESTRICTS_COLUMN =
      ByteString.copyFrom(DOUBLE_RESTRICTS_COLUMN_NAME, Charset.defaultCharset());

  // Tags we'll read from in a datapoint
  private static final String ALLOW_RESTRICTS_TAG = "allowtag";
  private static final String DENY_RESTRICTS_TAG = "denytag";
  private static final String INT_RESTRICTS_TAG = "inttag";
  private static final String FLOAT_RESTRICTS_TAG = "floattag";
  private static final String DOUBLE_RESTRICTS_TAG = "doubletag";

  @BeforeClass
  public static void setupClass() throws Exception {
    vectorSearchResourceManager =
        VectorSearchResourceManager.findOrCreateTestInfra(PROJECT_NUMBER, REGION);
  }

  @Before
  public void setup() throws Exception {
    // REGION and PROJECT are available, but we need the project number, not its name
    LOG.info("Have project number {}", PROJECT_NUMBER);
    LOG.info("Have project {}", PROJECT);
    LOG.info("Have region number {}", REGION);

    bigtableResourceManager =
        BigtableResourceManager.builder(
                removeUnsafeCharacters(testName), PROJECT, credentialsProvider)
            .maybeUseStaticInstance()
            .build();

    appProfileId = generateAppProfileId();
    srcTable = generateTableName();

    List<BigtableResourceManagerCluster> clusters = new ArrayList<>();
    clusters.add(BigtableResourceManagerCluster.create(clusterName, TEST_ZONE, 1, StorageType.HDD));
    bigtableResourceManager.createInstance(clusters);

    bigtableResourceManager.createAppProfile(
        appProfileId, true, bigtableResourceManager.getClusterNames());

    BigtableTableSpec cdcTableSpec = new BigtableTableSpec();
    cdcTableSpec.setCdcEnabled(true);
    cdcTableSpec.setColumnFamilies(Lists.asList(SOURCE_COLUMN_FAMILY, new String[] {}));
    bigtableResourceManager.createTable(srcTable, cdcTableSpec);

    LOG.info("Cluster names: {}", bigtableResourceManager.getClusterNames());
    LOG.info("Have instance {}", bigtableResourceManager.getInstanceId());
  }

  @After
  public void tearDown() {
    ResourceManagerUtils.cleanResources(bigtableResourceManager, vectorSearchResourceManager);
  }

  private PipelineLauncher.LaunchConfig.Builder defaultLaunchConfig() {
    return PipelineLauncher.LaunchConfig.builder("test-job", specPath)
        // Working configuration required by every test
        .addParameter("bigtableReadTableId", srcTable)
        .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
        .addParameter("bigtableChangeStreamAppProfile", appProfileId)
        .addParameter("bigtableChangeStreamCharset", "KOI8-R")
        .addParameter("vectorSearchIndex", vectorSearchResourceManager.getTestIndex().getName())
        // Optional configuration that some tests may with to override
        .addParameter("embeddingColumn", SOURCE_COLUMN_FAMILY + ":" + EMBEDDING_COLUMN_NAME)
        .addParameter("embeddingByteSize", EMBEDDING_BYTE_SIZE)
        .addParameter("crowdingTagColumn", SOURCE_COLUMN_FAMILY + ":" + CROWDING_TAG_COLUMN_NAME)
        .addParameter(
            "allowRestrictsMappings",
            SOURCE_COLUMN_FAMILY + ":" + ALLOW_RESTRICTS_COLUMN_NAME + "->" + ALLOW_RESTRICTS_TAG)
        .addParameter("upsertMaxBatchSize", "1")
        .addParameter("upsertMaxBufferDuration", "1s")
        .addParameter("deleteMaxBatchSize", "1")
        .addParameter("deleteMaxBufferDuration", "1s")
        .addParameter(
            "denyRestrictsMappings",
            SOURCE_COLUMN_FAMILY + ":" + DENY_RESTRICTS_COLUMN_NAME + "->" + DENY_RESTRICTS_TAG)
        .addParameter(
            "intNumericRestrictsMappings",
            SOURCE_COLUMN_FAMILY + ":" + INT_RESTRICTS_COLUMN_NAME + "->" + INT_RESTRICTS_TAG)
        .addParameter(
            "floatNumericRestrictsMappings",
            SOURCE_COLUMN_FAMILY + ":" + FLOAT_RESTRICTS_COLUMN_NAME + "->" + FLOAT_RESTRICTS_TAG)
        .addParameter(
            "doubleNumericRestrictsMappings",
            SOURCE_COLUMN_FAMILY
                + ":"
                + DOUBLE_RESTRICTS_COLUMN_NAME
                + "->"
                + DOUBLE_RESTRICTS_TAG);
  }

  @Test
  public void testRowMutationsThatAddEmbeddingsAreSyncedAsUpserts() throws Exception {
    LOG.info("Testname: {}", testName);
    LOG.info("specPath: {}", specPath);
    LOG.info("srcTable: {}", srcTable);
    //    LOG.info("bigtableResourceManagerInstanceId: {}",
    // bigtableResourceManager.getInstanceId());
    //    LOG.info("appProfileId: {}", appProfileId);
    PipelineLauncher.LaunchInfo launchInfo = launchTemplate(defaultLaunchConfig());
    LOG.info("Pipeline launched: {}", launchInfo.pipelineName());

    assertThatPipeline(launchInfo).isRunning();
    LOG.info("Pipeline is running");
    String rowkey = vectorSearchResourceManager.makeDatapointId();
    LOG.info("Writing rowkey {}", rowkey);

    long timestamp = 12000L;
    RowMutation rowMutation =
        RowMutation.create(srcTable, rowkey)
            .setCell(
                SOURCE_COLUMN_FAMILY,
                EMBEDDING_COLUMN,
                timestamp,
                ByteString.copyFrom(GOOD_EMBEDDING_BYTES));

    LOG.info("Writing row {}", rowkey);
    bigtableResourceManager.write(rowMutation);

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(launchInfo, Duration.ofMinutes(30)),
                () -> {
                  LOG.info("Looking for new datapoint");
                  var datapoint = vectorSearchResourceManager.findDatapoint(rowkey);
                  if (datapoint == null) {
                    LOG.info("No result");

                    return false;
                  } else {
                    LOG.info("Found result: {}", datapoint.getFeatureVectorList());
                    assertEqualVectors(GOOD_EMBEDDINGS, datapoint.getFeatureVectorList());
                    return true;
                  }
                });

    assertThatResult(result).meetsConditions();
  }

  @Test
  public void testBadEmbeddingsAreRejected() throws Exception {
    LOG.info("Testname: {}", testName);
    LOG.info("specPath: {}", specPath);
    LOG.info("srcTable: {}", srcTable);

    PipelineLauncher.LaunchInfo launchInfo =
        launchTemplate(defaultLaunchConfig().addParameter("dlqDirectory", getGcsPath("dlq")));

    assertThatPipeline(launchInfo).isRunning();
    LOG.info("Pipeline launched: {}", launchInfo.pipelineName());

    String rowkey = vectorSearchResourceManager.makeDatapointId();
    LOG.info("Writing rowkey {}", rowkey);

    long timestamp = 12000L;

    // This row mutation should fail, the Index API should reject it due to the incorrect
    // dimensionality of the embeddings vector
    RowMutation rowMutation =
        RowMutation.create(srcTable, rowkey)
            .setCell(
                SOURCE_COLUMN_FAMILY,
                EMBEDDING_COLUMN,
                timestamp,
                ByteString.copyFrom(BAD_EMBEDDING_BYTES));

    LOG.info("Writing row {}", rowkey);
    bigtableResourceManager.write(rowMutation);

    Storage storage = StorageOptions.newBuilder().setProjectId(PROJECT).build().getService();

    String filterPrefix =
        String.join("/", getClass().getSimpleName(), gcsClient.runId(), "dlq", "severe");
    LOG.info("Looking for files with a prefix: {}", filterPrefix);

    await("The failed message was not found in DLQ")
        .atMost(Duration.ofMinutes(30))
        .pollInterval(Duration.ofSeconds(1))
        .until(
            () -> {
              Page<Blob> blobs =
                  storage.list(artifactBucketName, BlobListOption.prefix(filterPrefix));

              for (Blob blob : blobs.iterateAll()) {
                // Ignore temp files
                if (blob.getName().contains(".temp-beam/")) {
                  continue;
                }

                byte[] content = storage.readAllBytes(blob.getBlobId());
                var errorMessage = new String(content);

                LOG.info("Have message '{}'", errorMessage);
                String wantMessage =
                    String.format(
                        "Error writing to vector search:io.grpc.StatusRuntimeException: INVALID_ARGUMENT: Incorrect dimensionality. Expected 10, got 5. Datapoint ID: %s.\n",
                        rowkey);
                LOG.info("Want message '{}'", wantMessage);
                assertEquals(errorMessage, wantMessage);

                return true;
              }

              return false;
            });
  }

  @Test
  public void testRowMutationsThatIncludeOptionalFieldsAreSynced() throws Exception {
    LOG.info("Testname: {}", testName);
    LOG.info("specPath: {}", specPath);
    LOG.info("srcTable: {}", srcTable);

    PipelineLauncher.LaunchInfo launchInfo = launchTemplate(defaultLaunchConfig());

    LOG.info("Pipeline launched1");
    assertThatPipeline(launchInfo).isRunning();
    LOG.info("Pipeline launched2");
    LOG.info("Writing mutation");
    String rowkey = vectorSearchResourceManager.makeDatapointId();

    LOG.info("Writing rowkey {}", rowkey);

    final String crowdingTag = randomAlphanumeric(10);
    final String allowTag = randomAlphanumeric(10);
    final String denyTag = randomAlphanumeric(10);
    final int intRestrict = RandomUtils.nextInt();
    final byte[] intBytes = ByteBuffer.allocate(4).putInt(intRestrict).array();
    final float floatRestrict = RandomUtils.nextFloat();
    final byte[] floatBytes = ByteBuffer.allocate(4).putFloat(floatRestrict).array();
    final double doubleRestrict = RandomUtils.nextDouble();
    final byte[] doubleBytes = ByteBuffer.allocate(8).putDouble(doubleRestrict).array();

    long timestamp = 12000L;
    RowMutation rowMutation =
        RowMutation.create(srcTable, rowkey)
            .setCell(
                SOURCE_COLUMN_FAMILY,
                EMBEDDING_COLUMN,
                timestamp,
                ByteString.copyFrom(GOOD_EMBEDDING_BYTES))
            .setCell(
                SOURCE_COLUMN_FAMILY,
                CROWDING_TAG_COLUMN,
                timestamp,
                ByteString.copyFrom(crowdingTag, Charset.defaultCharset()))
            .setCell(
                SOURCE_COLUMN_FAMILY,
                ALLOW_RESTRICTS_COLUMN,
                timestamp,
                ByteString.copyFrom(allowTag, Charset.defaultCharset()))
            .setCell(
                SOURCE_COLUMN_FAMILY,
                DENY_RESTRICTS_COLUMN,
                timestamp,
                ByteString.copyFrom(denyTag, Charset.defaultCharset()))
            .setCell(
                SOURCE_COLUMN_FAMILY,
                INT_RESTRICTS_COLUMN,
                timestamp,
                ByteString.copyFrom(intBytes))
            .setCell(
                SOURCE_COLUMN_FAMILY,
                FLOAT_RESTRICTS_COLUMN,
                timestamp,
                ByteString.copyFrom(floatBytes))
            .setCell(
                SOURCE_COLUMN_FAMILY,
                DOUBLE_RESTRICTS_COLUMN,
                timestamp,
                ByteString.copyFrom(doubleBytes));

    bigtableResourceManager.write(rowMutation);

    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(launchInfo, Duration.ofMinutes(30)),
                () -> {
                  LOG.info("Looking for new datapoint");
                  var datapoint = vectorSearchResourceManager.findDatapoint(rowkey);
                  if (datapoint == null) {
                    LOG.info("No result");
                    return false;
                  }

                  LOG.info("Found result: {}", datapoint);

                  assertEquals(2, datapoint.getRestrictsCount());

                  assertEquals(GOOD_EMBEDDINGS.size(), datapoint.getFeatureVectorCount());
                  assertEqualVectors(GOOD_EMBEDDINGS, datapoint.getFeatureVectorList());

                  for (var r : datapoint.getRestrictsList()) {
                    if (r.getNamespace().equals(ALLOW_RESTRICTS_TAG)) {
                      // It's our allow-restrict, verify tag matches
                      assertEquals(1, r.getAllowListCount());
                      // LOG("Have allow list {}", r.getAllowListList());
                      assertEquals(allowTag, r.getAllowList(0));
                    } else {
                      // it's necessarily our deny-restrict, verify tag matches
                      assertEquals(DENY_RESTRICTS_TAG, r.getNamespace());
                      assertEquals(1, r.getDenyListCount());
                      assertEquals(denyTag, r.getDenyList(0));
                    }
                  }

                  assertEquals(3, datapoint.getNumericRestrictsCount());

                  for (var r : datapoint.getNumericRestrictsList()) {
                    if (r.getNamespace().equals(INT_RESTRICTS_TAG)) {
                      assertEquals(intRestrict, r.getValueInt());
                    } else if (r.getNamespace().equals(FLOAT_RESTRICTS_TAG)) {
                      assertEquals(floatRestrict, r.getValueFloat(), 0.001);
                    } else if (r.getNamespace().equals(DOUBLE_RESTRICTS_TAG)) {
                      assertEquals(doubleRestrict, r.getValueDouble(), 0.001);
                    } else {
                      throw new RuntimeException("Unexpected numeric restrict");
                    }
                  }

                  return true;
                });

    assertThatResult(result).meetsConditions();
  }

  @Test
  public void testDeleteFamilyMutationsThatDeletetEmbeddingsColumnAreSyncedAsDeletes()
      throws Exception {

    var datapointId = vectorSearchResourceManager.makeDatapointId();
    vectorSearchResourceManager.addDatapoint(datapointId, GOOD_EMBEDDINGS);

    PipelineLauncher.LaunchInfo launchInfo = launchTemplate(defaultLaunchConfig());

    assertThatPipeline(launchInfo).isRunning();

    LOG.info("Waiting for datapoint to become queryable");
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForCondition(
                createConfig(launchInfo, Duration.ofMinutes(30)),
                () -> {
                  // Make sure the datapoint exists and is findable
                  var dp = vectorSearchResourceManager.findDatapoint(datapointId);
                  LOG.info(dp == null ? "DP does not yet exist" : "DP exists");
                  return dp != null;
                });

    assertThatResult(result).meetsConditions();

    RowMutation rowMutation =
        RowMutation.create(srcTable, datapointId).deleteFamily(SOURCE_COLUMN_FAMILY);

    bigtableResourceManager.write(rowMutation);

    LOG.info("Waiting for row to be deleted to become queryable");
    PipelineOperator.Result result2 =
        pipelineOperator()
            .waitForConditionAndCancel(
                createConfig(launchInfo, Duration.ofMinutes(30)),
                () -> {
                  // Make sure the datapoint exists and is findable
                  var dp = vectorSearchResourceManager.findDatapoint(datapointId);
                  LOG.info(dp == null ? "DP has been deleted" : "DP still exists");
                  return dp == null;
                });

    assertThatResult(result).meetsConditions();
  }

  @NotNull
  public static Boolean assertEqualVectors(Iterable<Float> expected, Iterable<Float> actual) {
    var i = actual.iterator();
    var j = actual.iterator();

    while (i.hasNext() && j.hasNext()) {
      assertEquals(i.next(), j.next(), 0.0001);
    }

    return !i.hasNext() && !j.hasNext();
  }

  @NotNull
  private static String generateAppProfileId() {
    return "cdc_app_profile_" + randomAlphanumeric(8).toLowerCase() + "_" + System.nanoTime();
  }

  @NotNull
  private String generateTableName() {
    return "table_" + randomAlphanumeric(8).toLowerCase() + "_" + System.nanoTime();
  }

  private String removeUnsafeCharacters(String testName) {
    return testName.replaceAll("[\\[\\]]", "-");
  }
}
