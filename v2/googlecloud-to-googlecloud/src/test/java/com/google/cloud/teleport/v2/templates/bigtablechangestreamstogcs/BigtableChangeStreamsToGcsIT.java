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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigtable.admin.v2.models.StorageType;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.teleport.bigtable.ChangelogEntry;
import com.google.cloud.teleport.bigtable.ChangelogEntryJson;
import com.google.cloud.teleport.bigtable.ModType;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.PipelineOperator.Config;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigtable.BigtableResourceManager;
import org.apache.beam.it.gcp.bigtable.BigtableResourceManagerCluster;
import org.apache.beam.it.gcp.bigtable.BigtableTableSpec;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link BigtableChangeStreamsToGcs}. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(BigtableChangeStreamsToGcs.class)
@RunWith(JUnit4.class)
public final class BigtableChangeStreamsToGcsIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(BigtableChangeStreamsToGcsIT.class);
  public static final String SOURCE_COLUMN_FAMILY = "cf";
  private static final Duration EXPECTED_REPLICATION_MAX_WAIT_TIME = Duration.ofMinutes(10);
  private static final String TEST_ZONE = "us-central1-a";
  private static BigtableResourceManager bigtableResourceManager;
  private static GcsResourceManager gcsResourceManager;

  private String outputPath;
  private String outputPrefix;
  private String outputPath2;
  private String outputPrefix2;
  private String appProfileId;
  private String srcTable;

  @Test
  public void testSingleMutationChangelogEntryJsonE2E() throws Exception {
    LaunchInfo launchInfo =
        launchTemplate(
            LaunchConfig.builder(testName, specPath)
                .addParameter("bigtableReadTableId", srcTable)
                .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
                .addParameter("bigtableChangeStreamAppProfile", appProfileId)
                .addParameter("bigtableChangeStreamCharset", "KOI8-R")
                .addParameter("outputFileFormat", "TEXT")
                .addParameter("windowDuration", "10s")
                .addParameter("gcsOutputDirectory", outputPath)
                .addParameter("schemaOutputFormat", "CHANGELOG_ENTRY"));

    assertThatPipeline(launchInfo).isRunning();

    String rowkey = UUID.randomUUID().toString();
    String column = UUID.randomUUID().toString();

    // https://en.wikipedia.org/wiki/KOI8-R
    byte[] valueRussianLetterBinKoi8R = new byte[] {(byte) 0xc2};

    long nowMillis = System.currentTimeMillis();
    long timestampMicros = nowMillis * 1000;

    RowMutation rowMutation =
        RowMutation.create(srcTable, rowkey)
            .setCell(
                SOURCE_COLUMN_FAMILY,
                ByteString.copyFrom(column, Charset.defaultCharset()),
                timestampMicros,
                ByteString.copyFrom(valueRussianLetterBinKoi8R));

    ChangelogEntryJson expected = new ChangelogEntryJson();
    expected.setRowKey(rowkey);
    expected.setTimestamp(timestampMicros);
    expected.setCommitTimestamp(nowMillis - 10000); // clock skew tolerance
    expected.setLowWatermark(0);
    expected.setColumn(column);
    expected.setValue(new String(valueRussianLetterBinKoi8R, Charset.forName("KOI8-R")));
    expected.setColumnFamily(SOURCE_COLUMN_FAMILY);
    expected.setModType(ModType.SET_CELL);
    expected.setIsGC(false);

    bigtableResourceManager.write(rowMutation);

    if (!waitForFilesToShowUpAtPath(
        outputPath,
        outputPrefix,
        Duration.ofMinutes(10),
        new LookForChangelogEntryJsonRecord(expected))) {
      Assert.fail("Unable to find output file containing row mutation: " + expected);
    }
  }

  @Test
  public void testSingleMutationChangelogEntryJsonBase64E2E() throws Exception {
    LaunchInfo launchInfo =
        launchTemplate(
            LaunchConfig.builder(testName, specPath)
                .addParameter("bigtableReadTableId", srcTable)
                .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
                .addParameter("bigtableChangeStreamAppProfile", appProfileId)
                .addParameter("outputFileFormat", "TEXT")
                .addParameter("useBase64Rowkeys", "true")
                .addParameter("useBase64ColumnQualifiers", "true")
                .addParameter("useBase64Values", "true")
                .addParameter("windowDuration", "10s")
                .addParameter("gcsOutputDirectory", outputPath)
                .addParameter("schemaOutputFormat", "CHANGELOG_ENTRY"));

    assertThatPipeline(launchInfo).isRunning();

    String rowkey = UUID.randomUUID().toString();
    String column = UUID.randomUUID().toString();
    String value = UUID.randomUUID().toString();

    long nowMillis = System.currentTimeMillis();
    long timestampMicros = nowMillis * 1000;

    RowMutation rowMutation =
        RowMutation.create(srcTable, rowkey)
            .setCell(SOURCE_COLUMN_FAMILY, column, timestampMicros, value);

    ChangelogEntry expected = new ChangelogEntry();
    expected.setRowKey(bb(rowkey));
    expected.setTimestamp(timestampMicros);
    expected.setCommitTimestamp(nowMillis - 10000); // clock skew tolerance
    expected.setLowWatermark(0);
    expected.setColumn(bb(column));
    expected.setValue(bb(value));
    expected.setColumnFamily(SOURCE_COLUMN_FAMILY);
    expected.setModType(ModType.SET_CELL);
    expected.setIsGC(false);

    bigtableResourceManager.write(rowMutation);

    if (!waitForFilesToShowUpAtPath(
        outputPath,
        outputPrefix,
        Duration.ofMinutes(10),
        new LookForChangelogEntryJsonBase64Record(expected))) {
      Assert.fail("Unable to find output file containing row mutation: " + expected);
    }
  }

  @Test
  public void testSingleMutationChangelogEntryAvroE2E() throws Exception {
    LaunchInfo launchInfo =
        launchTemplate(
            LaunchConfig.builder(testName, specPath)
                .addParameter("bigtableReadTableId", srcTable)
                .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
                .addParameter("bigtableChangeStreamAppProfile", appProfileId)
                .addParameter("outputFileFormat", "AVRO")
                .addParameter("windowDuration", "10s")
                .addParameter("gcsOutputDirectory", outputPath)
                .addParameter("schemaOutputFormat", "CHANGELOG_ENTRY"));

    assertThatPipeline(launchInfo).isRunning();

    String rowkey = UUID.randomUUID().toString();
    String column = UUID.randomUUID().toString();
    String value = UUID.randomUUID().toString();

    long nowMillis = System.currentTimeMillis();
    long timestampMicros = nowMillis * 1000;

    RowMutation rowMutation =
        RowMutation.create(srcTable, rowkey)
            .setCell(SOURCE_COLUMN_FAMILY, column, timestampMicros, value);

    ChangelogEntry expected = new ChangelogEntry();

    expected.setRowKey(bb(rowkey));
    expected.setTimestamp(timestampMicros);
    expected.setCommitTimestamp(nowMillis - 10000); // clock skew tolerance
    expected.setLowWatermark(0);
    expected.setColumn(bb(column));
    expected.setValue(bb(value));
    expected.setColumnFamily(SOURCE_COLUMN_FAMILY);
    expected.setModType(ModType.SET_CELL);
    expected.setIsGC(false);

    bigtableResourceManager.write(rowMutation);

    if (!waitForFilesToShowUpAtPath(
        outputPath,
        outputPrefix,
        Duration.ofMinutes(10),
        new LookForChangelogEntryAvroRecord(expected))) {
      Assert.fail("Unable to find output file containing row mutation: " + expected);
    }
  }

  @Test
  public void testSingleMutationBigtableRowAvroE2E() throws Exception {
    LaunchInfo launchInfo =
        launchTemplate(
            LaunchConfig.builder(testName, specPath)
                .addParameter("bigtableReadTableId", srcTable)
                .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
                .addParameter("bigtableChangeStreamAppProfile", appProfileId)
                .addParameter("outputFileFormat", "AVRO")
                .addParameter("windowDuration", "10s")
                .addParameter("gcsOutputDirectory", outputPath)
                .addParameter("schemaOutputFormat", "BIGTABLE_ROW"));

    assertThatPipeline(launchInfo).isRunning();

    String rowkey = UUID.randomUUID().toString();
    String column = UUID.randomUUID().toString();
    String value = UUID.randomUUID().toString();

    long nowMillis = System.currentTimeMillis();
    long timestampMicros = nowMillis * 1000;

    RowMutation rowMutation =
        RowMutation.create(srcTable, rowkey)
            .setCell(SOURCE_COLUMN_FAMILY, column, timestampMicros, value);

    ChangelogEntry expected = new ChangelogEntry();

    expected.setRowKey(bb(rowkey));
    expected.setTimestamp(timestampMicros);
    expected.setCommitTimestamp(nowMillis - 10000); // clock skew tolerance
    expected.setLowWatermark(0);
    expected.setColumn(bb(column));
    expected.setValue(bb(value));
    expected.setColumnFamily(SOURCE_COLUMN_FAMILY);
    expected.setModType(ModType.SET_CELL);
    expected.setIsGC(false);

    bigtableResourceManager.write(rowMutation);

    if (!waitForFilesToShowUpAtPath(
        outputPath,
        outputPrefix,
        Duration.ofMinutes(10),
        new LookForBigtableRowAvroRecord(expected))) {
      Assert.fail("Unable to find output file containing row mutation: " + expected);
    }
  }

  @Test
  public void testSingleMutationBigtableRowAvroWithStartTimeE2E() throws Exception {
    LaunchInfo launchInfo =
        launchTemplate(
            LaunchConfig.builder(testName, specPath)
                .addParameter("bigtableReadTableId", srcTable)
                .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
                .addParameter("bigtableChangeStreamAppProfile", appProfileId)
                .addParameter("outputFileFormat", "AVRO")
                .addParameter("windowDuration", "10s")
                .addParameter("gcsOutputDirectory", outputPath)
                .addParameter("schemaOutputFormat", "BIGTABLE_ROW"));

    assertThatPipeline(launchInfo).isRunning();

    String rowkey = UUID.randomUUID().toString();
    String column = UUID.randomUUID().toString();
    String value = UUID.randomUUID().toString();

    long nowMillis = System.currentTimeMillis();
    long timestampMicros = nowMillis * 1000;

    RowMutation rowMutation =
        RowMutation.create(srcTable, rowkey)
            .setCell(SOURCE_COLUMN_FAMILY, column, timestampMicros, value);

    ChangelogEntry expected = new ChangelogEntry();

    expected.setRowKey(bb(rowkey));
    expected.setTimestamp(timestampMicros);
    expected.setCommitTimestamp(timestampMicros - 10000000); // clock skew tolerance
    expected.setLowWatermark(0);
    expected.setColumn(bb(column));
    expected.setValue(bb(value));
    expected.setColumnFamily(SOURCE_COLUMN_FAMILY);
    expected.setModType(ModType.SET_CELL);
    expected.setIsGC(false);

    bigtableResourceManager.write(rowMutation);

    Thread.sleep(10000);

    bigtableResourceManager.write(rowMutation);

    var predicate = new LookForBigtableRowAvroRecord(expected);
    if (!waitForFilesToShowUpAtPath(outputPath, outputPrefix, Duration.ofMinutes(10), predicate)) {
      Assert.fail("Unable to find output file containing row mutation: " + expected);
    }

    // Adding 1.5s just to make sure we don't capture this earliest record
    long microsCutoff = predicate.getEarliestCommitTime() + 1500000L;

    pipelineLauncher.cancelJob(launchInfo.projectId(), launchInfo.region(), launchInfo.jobId());

    launchInfo =
        launchTemplate(
            LaunchConfig.builder(testName, specPath)
                .addParameter("bigtableReadTableId", srcTable)
                .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
                .addParameter("bigtableChangeStreamAppProfile", appProfileId)
                .addParameter("outputFileFormat", "AVRO")
                .addParameter("bigtableChangeStreamStartTimestamp", formatTimeMicros(microsCutoff))
                .addParameter("windowDuration", "10s")
                .addParameter("gcsOutputDirectory", outputPath2)
                .addParameter("schemaOutputFormat", "BIGTABLE_ROW"));

    assertThatPipeline(launchInfo).isRunning();

    var latestPredicate = new LookForBigtableRowAvroRecord(expected);
    if (!waitForFilesToShowUpAtPath(
        outputPath2, outputPrefix2, Duration.ofMinutes(10), latestPredicate)) {
      Assert.fail("Unable to find latest mutation: " + expected);
    }
    Assert.assertTrue(latestPredicate.getEarliestCommitTime() >= microsCutoff);
  }

  @Test
  public void testSingleMutationBigtableRowAvroWStopAndResumeE2E() throws Exception {
    String persistentName = UUID.randomUUID().toString();
    LaunchInfo launchInfo =
        launchTemplate(
            LaunchConfig.builder(testName, specPath)
                .addParameter("bigtableChangeStreamName", persistentName)
                .addParameter("bigtableChangeStreamResume", "false")
                .addParameter("bigtableReadTableId", srcTable)
                .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
                .addParameter("bigtableChangeStreamAppProfile", appProfileId)
                .addParameter("outputFileFormat", "AVRO")
                .addParameter("windowDuration", "10s")
                .addParameter("gcsOutputDirectory", outputPath)
                .addParameter("schemaOutputFormat", "BIGTABLE_ROW"));

    assertThatPipeline(launchInfo).isRunning();

    String rowkey = UUID.randomUUID().toString();
    String column = UUID.randomUUID().toString();
    String value = UUID.randomUUID().toString();

    long nowMillis = System.currentTimeMillis();
    long timestampMicros = nowMillis * 1000;

    RowMutation rowMutation =
        RowMutation.create(srcTable, rowkey)
            .setCell(SOURCE_COLUMN_FAMILY, column, timestampMicros, value);

    ChangelogEntry expected = new ChangelogEntry();

    expected.setRowKey(bb(rowkey));
    expected.setTimestamp(timestampMicros);
    expected.setCommitTimestamp(timestampMicros - 10000000); // clock skew tolerance
    expected.setLowWatermark(0);
    expected.setColumn(bb(column));
    expected.setValue(bb(value));
    expected.setColumnFamily(SOURCE_COLUMN_FAMILY);
    expected.setModType(ModType.SET_CELL);
    expected.setIsGC(false);

    bigtableResourceManager.write(rowMutation);

    if (!waitForFilesToShowUpAtPath(
        outputPath,
        outputPrefix,
        Duration.ofMinutes(10),
        new LookForBigtableRowAvroRecord(expected))) {
      Assert.fail("Unable to find output file containing row mutation: " + expected);
    }

    pipelineLauncher.cancelJob(launchInfo.projectId(), launchInfo.region(), launchInfo.jobId());

    waitUntilCancelled(launchInfo, Duration.ofMinutes(10));

    // Scanning the dir one more time to capture the latest commit time
    var predicate = new LookForBigtableRowAvroRecord(expected);
    if (!waitForFilesToShowUpAtPath(outputPath, outputPrefix, Duration.ofMinutes(10), predicate)) {
      Assert.fail("Unable to find output file containing row mutation: " + expected);
    }

    bigtableResourceManager.write(rowMutation);

    launchInfo =
        launchTemplate(
            LaunchConfig.builder(testName, specPath)
                .addParameter("bigtableChangeStreamName", persistentName)
                .addParameter("bigtableChangeStreamResume", "true")
                .addParameter("bigtableReadTableId", srcTable)
                .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
                .addParameter("bigtableChangeStreamAppProfile", appProfileId)
                .addParameter("outputFileFormat", "AVRO")
                .addParameter(
                    "bigtableChangeStreamStartTimestamp",
                    formatTimeMicros(predicate.getEarliestCommitTime() - 1000000))
                .addParameter("windowDuration", "10s")
                .addParameter("gcsOutputDirectory", outputPath2)
                .addParameter("schemaOutputFormat", "BIGTABLE_ROW"));

    assertThatPipeline(launchInfo).isRunning();

    var latestPredicate = new LookForBigtableRowAvroRecord(expected);
    if (!waitForFilesToShowUpAtPath(
        outputPath2, outputPrefix2, Duration.ofMinutes(10), latestPredicate)) {
      Assert.fail("Unable to find latest mutation: " + expected);
    }
    Assert.assertTrue(latestPredicate.getEarliestCommitTime() > predicate.getLatestCommitTime());
  }

  private String generateClusterName() {
    return "teleport-c1";
  }

  private String generateTableName() {
    return "table" + System.nanoTime();
  }

  private void waitUntilCancelled(LaunchInfo launchInfo, Duration maxWait) {
    long started = System.currentTimeMillis();
    while (System.currentTimeMillis() < started + maxWait.toMillis()) {
      try {
        var state =
            pipelineLauncher.getJobStatus(
                TestProperties.project(), TestProperties.region(), launchInfo.jobId());
        switch (state) {
          case CANCELLED:
            LOG.info("Job is finally CANCELLED!");
            return;
          case CANCELLING:
          case RUNNING:
            LOG.info("Job is not cancelled yet: " + state);
            continue;
          default:
            throw new RuntimeException("Unexpected job state: " + state);
        }
      } catch (Exception e) {
        LOG.warn("failed to obtain job state: ", e);
      }
    }
  }

  @Test
  public void testSingleMutationBigtableRowTextE2E() throws Exception {
    String srcTable = generateTableName();

    try {
      launchTemplate(
          LaunchConfig.builder(testName, specPath)
              .addParameter("bigtableReadTableId", srcTable)
              .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
              .addParameter("bigtableChangeStreamAppProfile", "anything")
              .addParameter("outputFileFormat", "TEXT")
              .addParameter("windowDuration", "10s")
              .addParameter("gcsOutputDirectory", outputPath)
              .addParameter("schemaOutputFormat", "BIGTABLE_ROW"));
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().contains("The job failed before launch"));
    }
  }

  private ByteBuffer bb(String value) {
    return ByteBuffer.wrap(value.getBytes(Charset.defaultCharset()));
  }

  private boolean waitForFilesToShowUpAtPath(
      String outputPath, String outputPrefix, Duration howLong, Predicate<? super Blob> checkFile)
      throws Exception {
    long polUntil = System.currentTimeMillis() + howLong.toMillis();

    Storage storage =
        StorageOptions.newBuilder().setProjectId(TestProperties.project()).build().getService();

    boolean found = false;
    while (System.currentTimeMillis() < polUntil && !found) {
      LOG.info("Looking for files at " + outputPath);

      Page<Blob> blobPa =
          storage.list(TestProperties.artifactBucket(), BlobListOption.prefix(outputPrefix));
      found = blobPa.streamAll().anyMatch(checkFile);
      if (!found) {
        Thread.sleep(1000);
      }
    }
    return found;
  }

  @NotNull
  private static String generateAppProfileId() {
    return "cdc_app_profile_" + System.nanoTime();
  }

  @Before
  public void setup() throws IOException {
    gcsResourceManager =
        GcsResourceManager.builder(
                TestProperties.artifactBucket(),
                getClass().getSimpleName(),
                TestProperties.googleCredentials())
            .build();

    String outputDir = generateSafeDirectoryName();
    gcsResourceManager.registerTempDir(outputDir);
    outputPath = String.format("gs://%s/%s/output", TestProperties.artifactBucket(), outputDir);
    outputPrefix = String.format("%s/output", outputDir);

    outputPath2 = String.format("gs://%s/%s/output2", TestProperties.artifactBucket(), outputDir);
    outputPrefix2 = String.format("%s/output2", outputDir);

    BigtableResourceManager.Builder rmBuilder =
        BigtableResourceManager.builder(testName, PROJECT, credentialsProvider);

    bigtableResourceManager = rmBuilder.maybeUseStaticInstance().build();

    appProfileId = generateAppProfileId();
    String clusterName = generateClusterName();
    srcTable = generateTableName();

    List<BigtableResourceManagerCluster> clusters = new ArrayList<>();
    clusters.add(BigtableResourceManagerCluster.create(clusterName, TEST_ZONE, 1, StorageType.HDD));

    bigtableResourceManager.createInstance(clusters);

    bigtableResourceManager.createAppProfile(
        appProfileId, true, Lists.asList(clusterName, new String[] {}));

    BigtableTableSpec cdcTableSpec = new BigtableTableSpec();
    cdcTableSpec.setCdcEnabled(true);
    cdcTableSpec.setColumnFamilies(Lists.asList(SOURCE_COLUMN_FAMILY, new String[] {}));
    bigtableResourceManager.createTable(srcTable, cdcTableSpec);
  }

  @After
  public void tearDownClass() {
    ResourceManagerUtils.cleanResources(bigtableResourceManager, gcsResourceManager);
  }

  @Override
  protected PipelineOperator.Config createConfig(LaunchInfo info) {
    Config.Builder configBuilder =
        Config.builder().setJobId(info.jobId()).setProject(PROJECT).setRegion(REGION);

    // For DirectRunner tests, reduce the max time and the interval, as there is no worker required
    if (System.getProperty("directRunnerTest") != null) {
      configBuilder =
          configBuilder
              .setTimeoutAfter(EXPECTED_REPLICATION_MAX_WAIT_TIME.minus(Duration.ofMinutes(3)))
              .setCheckAfter(Duration.ofSeconds(5));
    } else {
      configBuilder.setTimeoutAfter(EXPECTED_REPLICATION_MAX_WAIT_TIME);
    }

    return configBuilder.build();
  }

  // We don't want any unexpected date format characters in the dir name
  private String generateSafeDirectoryName() {
    return UUID.randomUUID().toString().replaceAll("[da]", "x");
  }

  private String formatTimeMicros(long microsCutoff) {
    SimpleDateFormat format = new SimpleDateFormat("yyy-MM-dd'T'HH:mm:ssXXX");
    return format.format(new Date(microsCutoff / 1000));
  }
}
