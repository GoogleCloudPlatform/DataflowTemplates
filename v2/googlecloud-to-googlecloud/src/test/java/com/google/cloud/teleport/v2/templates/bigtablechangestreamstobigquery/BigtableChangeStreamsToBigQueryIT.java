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
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.api.gax.paging.Page;
import com.google.cloud.Timestamp;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigtable.data.v2.models.RowMutation;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.ChangelogColumn;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.PipelineOperator.Config;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ExceptionUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigtable.BigtableResourceManager;
import org.apache.beam.it.gcp.bigtable.BigtableResourceManagerUtils;
import org.apache.beam.it.gcp.bigtable.BigtableTableSpec;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Integration test for {@link BigtableChangeStreamsToBigQuery}. */
@Category(TemplateIntegrationTest.class)
@TemplateIntegrationTest(BigtableChangeStreamsToBigQuery.class)
@RunWith(JUnit4.class)
public final class BigtableChangeStreamsToBigQueryIT extends TemplateTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(BigtableChangeStreamsToBigQueryIT.class);

  public static final String SOURCE_COLUMN_FAMILY = "cf";
  private static final Duration EXPECTED_REPLICATION_MAX_WAIT_TIME = Duration.ofMinutes(10);
  private BigtableResourceManager bigtableResourceManager;
  private BigQueryResourceManager bigQueryResourceManager;

  private LaunchInfo launchInfo;

  @Before
  public void setup() throws IOException {
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
    BigtableResourceManager.Builder rmBuilder =
        BigtableResourceManager.builder(testName, PROJECT, credentialsProvider)
            .maybeUseStaticInstance();

    bigtableResourceManager = rmBuilder.build();
  }

  @After
  public void tearDownClass() {
    ResourceManagerUtils.cleanResources(bigtableResourceManager, bigQueryResourceManager);
  }

  @Test
  public void testBigtableChangeStreamsToBigQuerySingleMutationE2E() throws Exception {
    long timeNowMicros = System.currentTimeMillis() * 1000;
    String appProfileId = generateAppProfileId();

    BigtableTableSpec cdcTableSpec = new BigtableTableSpec();
    cdcTableSpec.setCdcEnabled(true);
    cdcTableSpec.setColumnFamilies(Lists.asList(SOURCE_COLUMN_FAMILY, new String[] {}));

    String table = BigtableResourceManagerUtils.generateTableId("single-mutation");
    String cdcTable = BigtableResourceManagerUtils.generateTableId("cdc-single-mutation");
    bigtableResourceManager.createTable(table, cdcTableSpec);

    bigtableResourceManager.createAppProfile(
        appProfileId, true, bigtableResourceManager.getClusterNames());
    bigQueryResourceManager.createDataset(REGION);

    Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder = Function.identity();
    launchInfo =
        launchTemplate(
            paramsAdder.apply(
                LaunchConfig.builder(testName, specPath)
                    .addParameter("bigtableReadTableId", table)
                    .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
                    .addParameter("bigtableChangeStreamAppProfile", appProfileId)
                    .addParameter("bigQueryDataset", bigQueryResourceManager.getDatasetId())
                    .addParameter("bigQueryChangelogTableName", cdcTable)));

    assertThatPipeline(launchInfo).isRunning();

    String rowkey = UUID.randomUUID().toString();
    String column = UUID.randomUUID().toString();
    String value = UUID.randomUUID().toString();

    RowMutation rowMutation =
        RowMutation.create(table, rowkey).setCell(SOURCE_COLUMN_FAMILY, column, value);
    bigtableResourceManager.write(rowMutation);

    String query = newLookForValuesQuery(cdcTable, rowkey, column, value);
    waitForQueryToReturnRows(query, 1, true);

    TableResult tableResult = bigQueryResourceManager.runQuery(query);
    tableResult
        .iterateAll()
        .forEach(
            fvl -> {
              assertTrue(
                  fvl.get(ChangelogColumn.TIMESTAMP.getBqColumnName()).getTimestampValue()
                      >= timeNowMicros);
              assertFalse(fvl.get(ChangelogColumn.IS_GC.getBqColumnName()).getBooleanValue());
              assertTrue(fvl.get(ChangelogColumn.TIMESTAMP_FROM.getBqColumnName()).isNull());
              assertTrue(fvl.get(ChangelogColumn.TIMESTAMP_TO.getBqColumnName()).isNull());
              assertEquals(
                  table, fvl.get(ChangelogColumn.SOURCE_TABLE.getBqColumnName()).getStringValue());
              assertEquals(
                  bigtableResourceManager.getInstanceId(),
                  fvl.get(ChangelogColumn.SOURCE_INSTANCE.getBqColumnName()).getStringValue());
              assertTrue(fvl.get(ChangelogColumn.TIEBREAKER.getBqColumnName()).getLongValue() >= 0);
            });
  }

  @NotNull
  private static String generateAppProfileId() {
    return "cdc_app_profile_" + randomAlphanumeric(8).toLowerCase() + "_" + System.nanoTime();
  }

  @Test
  public void testBigtableChangeStreamsToBigQueryMutationsStartTimeE2E() throws Exception {
    long timeNowMicros = System.currentTimeMillis() * 1000;
    String appProfileId = generateAppProfileId();

    BigtableTableSpec cdcTableSpec = new BigtableTableSpec();
    cdcTableSpec.setCdcEnabled(true);
    cdcTableSpec.setColumnFamilies(Lists.asList(SOURCE_COLUMN_FAMILY, new String[] {}));
    String table = BigtableResourceManagerUtils.generateTableId("mutations-start");
    String cdcTable = BigtableResourceManagerUtils.generateTableId("cdc-mutations-start");
    bigtableResourceManager.createTable(table, cdcTableSpec);

    bigtableResourceManager.createAppProfile(
        appProfileId, true, bigtableResourceManager.getClusterNames());
    bigQueryResourceManager.createDataset(REGION);

    String rowkey = UUID.randomUUID().toString();
    String column = UUID.randomUUID().toString();
    String tooEarlyValue = UUID.randomUUID().toString();
    String valueToBeRead = UUID.randomUUID().toString();
    String nextValueToBeRead = UUID.randomUUID().toString();

    RowMutation earlyMutation =
        RowMutation.create(table, rowkey).setCell(SOURCE_COLUMN_FAMILY, column, tooEarlyValue);
    bigtableResourceManager.write(earlyMutation);

    TimeUnit.SECONDS.sleep(5);
    long afterFirstMutation = System.currentTimeMillis();
    afterFirstMutation -= (afterFirstMutation % 1000);
    TimeUnit.SECONDS.sleep(5);

    RowMutation toBeReadMutation =
        RowMutation.create(table, rowkey).setCell(SOURCE_COLUMN_FAMILY, column, valueToBeRead);
    bigtableResourceManager.write(toBeReadMutation);

    TimeUnit.SECONDS.sleep(5);

    RowMutation nextToBeReadMutation =
        RowMutation.create(table, rowkey).setCell(SOURCE_COLUMN_FAMILY, column, nextValueToBeRead);
    bigtableResourceManager.write(nextToBeReadMutation);

    Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder = Function.identity();
    launchInfo =
        launchTemplate(
            paramsAdder.apply(
                LaunchConfig.builder(testName, specPath)
                    .addParameter("bigtableReadTableId", table)
                    .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
                    .addParameter("bigtableChangeStreamAppProfile", appProfileId)
                    .addParameter("bigQueryDataset", bigQueryResourceManager.getDatasetId())
                    .addParameter("bigQueryChangelogTableName", cdcTable)
                    .addParameter(
                        "bigtableChangeStreamStartTimestamp",
                        Timestamp.of(new Date(afterFirstMutation)).toString())));

    assertThatPipeline(launchInfo).isRunning();

    String query = newLookForValuesQuery(cdcTable, rowkey, column, null);
    waitForQueryToReturnRows(query, 2, true);

    HashSet<String> toBeReadValues = new HashSet<>();
    toBeReadValues.add(valueToBeRead);
    toBeReadValues.add(nextValueToBeRead);

    TableResult tableResult = bigQueryResourceManager.runQuery(query);
    tableResult
        .iterateAll()
        .forEach(
            fvl -> {
              assertTrue(
                  toBeReadValues.contains(
                      fvl.get(ChangelogColumn.VALUE_STRING.getBqColumnName()).getStringValue()));
              assertTrue(
                  fvl.get(ChangelogColumn.TIMESTAMP.getBqColumnName()).getTimestampValue()
                      >= timeNowMicros);
              assertFalse(fvl.get(ChangelogColumn.IS_GC.getBqColumnName()).getBooleanValue());
              assertTrue(fvl.get(ChangelogColumn.TIMESTAMP_FROM.getBqColumnName()).isNull());
              assertTrue(fvl.get(ChangelogColumn.TIMESTAMP_TO.getBqColumnName()).isNull());
              assertEquals(
                  table, fvl.get(ChangelogColumn.SOURCE_TABLE.getBqColumnName()).getStringValue());
              assertEquals(
                  bigtableResourceManager.getInstanceId(),
                  fvl.get(ChangelogColumn.SOURCE_INSTANCE.getBqColumnName()).getStringValue());
              assertTrue(fvl.get(ChangelogColumn.TIEBREAKER.getBqColumnName()).getLongValue() >= 0);

              toBeReadValues.remove(
                  fvl.get(ChangelogColumn.VALUE_STRING.getBqColumnName()).getStringValue());
            });
  }

  @Test
  public void testBigtableChangeStreamsToBigQueryDeadLetterQueueE2E() throws Exception {
    long timeNowMicros = System.currentTimeMillis() * 1000;
    String appProfileId = generateAppProfileId();

    BigtableTableSpec cdcTableSpec = new BigtableTableSpec();
    cdcTableSpec.setCdcEnabled(true);
    cdcTableSpec.setColumnFamilies(Lists.asList(SOURCE_COLUMN_FAMILY, new String[] {}));
    String table = BigtableResourceManagerUtils.generateTableId("dlq");
    String cdcTable = BigtableResourceManagerUtils.generateTableId("cdc-dlq");
    bigtableResourceManager.createTable(table, cdcTableSpec);

    bigtableResourceManager.createAppProfile(
        appProfileId, true, bigtableResourceManager.getClusterNames());
    bigQueryResourceManager.createDataset(REGION);

    String rowkey = UUID.randomUUID().toString();
    String column = UUID.randomUUID().toString();
    String goodValue = UUID.randomUUID().toString();

    // Making some 15MB value
    String tooBigValue =
        StringUtils.repeat(UUID.randomUUID().toString(), 15 * 1024 * 1024 / goodValue.length());

    long beforeMutations = System.currentTimeMillis();
    beforeMutations -= (beforeMutations % 1000);

    TimeUnit.SECONDS.sleep(5);

    RowMutation tooLargeMutation =
        RowMutation.create(table, rowkey).setCell(SOURCE_COLUMN_FAMILY, column, tooBigValue);
    bigtableResourceManager.write(tooLargeMutation);

    RowMutation smallMutation =
        RowMutation.create(table, rowkey).setCell(SOURCE_COLUMN_FAMILY, column, goodValue);
    bigtableResourceManager.write(smallMutation);

    TimeUnit.SECONDS.sleep(5);

    Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder = Function.identity();
    launchInfo =
        launchTemplate(
            paramsAdder.apply(
                LaunchConfig.builder(testName, specPath)
                    .addParameter("bigtableReadTableId", table)
                    .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
                    .addParameter("bigtableChangeStreamAppProfile", appProfileId)
                    .addParameter("bigQueryDataset", bigQueryResourceManager.getDatasetId())
                    .addParameter("bigQueryChangelogTableName", cdcTable)
                    .addParameter("dlqDirectory", getGcsPath("dlq"))
                    .addParameter(
                        "bigtableChangeStreamStartTimestamp",
                        Timestamp.of(new Date(beforeMutations)).toString())));

    assertThatPipeline(launchInfo).isRunning();

    String query = newLookForValuesQuery(cdcTable, rowkey, column, null);
    waitForQueryToReturnRows(query, 1, false);

    HashSet<String> toBeReadValues = new HashSet<>();
    toBeReadValues.add(goodValue);

    TableResult tableResult = bigQueryResourceManager.runQuery(query);
    tableResult
        .iterateAll()
        .forEach(
            fvl -> {
              assertTrue(
                  toBeReadValues.contains(
                      fvl.get(ChangelogColumn.VALUE_STRING.getBqColumnName()).getStringValue()));
              assertTrue(
                  fvl.get(ChangelogColumn.TIMESTAMP.getBqColumnName()).getTimestampValue()
                      >= timeNowMicros);
              assertFalse(fvl.get(ChangelogColumn.IS_GC.getBqColumnName()).getBooleanValue());
              assertTrue(fvl.get(ChangelogColumn.TIMESTAMP_FROM.getBqColumnName()).isNull());
              assertTrue(fvl.get(ChangelogColumn.TIMESTAMP_TO.getBqColumnName()).isNull());
              assertEquals(
                  table, fvl.get(ChangelogColumn.SOURCE_TABLE.getBqColumnName()).getStringValue());
              assertEquals(
                  bigtableResourceManager.getInstanceId(),
                  fvl.get(ChangelogColumn.SOURCE_INSTANCE.getBqColumnName()).getStringValue());
              assertTrue(fvl.get(ChangelogColumn.TIEBREAKER.getBqColumnName()).getLongValue() >= 0);

              toBeReadValues.remove(
                  fvl.get(ChangelogColumn.VALUE_STRING.getBqColumnName()).getStringValue());
            });

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
                ObjectMapper om = new ObjectMapper();
                JsonNode severeError = om.readTree(content);
                assertNotNull(severeError);
                JsonNode errorMessageNode = severeError.get("error_message");
                assertNotNull(errorMessageNode);
                assertTrue(errorMessageNode instanceof TextNode);
                String messageText = errorMessageNode.asText();
                assertTrue(
                    "Unexpected message text: " + messageText,
                    StringUtils.contains(
                        messageText, "Row payload too large. Maximum size 10000000"));
                return true;
              }

              return false;
            });
  }

  @NotNull
  private Supplier<Boolean> dataShownUp(String query, int minRows) {
    return () -> {
      try {
        return bigQueryResourceManager.runQuery(query).getTotalRows() >= minRows;
      } catch (Exception e) {
        if (ExceptionUtils.containsMessage(e, "Not found: Table")) {
          return false;
        } else {
          throw e;
        }
      }
    };
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

  private void waitForQueryToReturnRows(String query, int resultsRequired, boolean cancelOnceDone)
      throws IOException {
    Config config = createConfig(launchInfo);
    Result result =
        cancelOnceDone
            ? pipelineOperator()
                .waitForConditionAndCancel(config, dataShownUp(query, resultsRequired))
            : pipelineOperator().waitForCondition(config, dataShownUp(query, resultsRequired));
    assertThatResult(result).meetsConditions();
  }

  private String newLookForValuesQuery(
      String cdcTable, String rowkey, String column, String value) {
    return "SELECT * FROM `"
        + bigQueryResourceManager.getDatasetId()
        + "."
        + cdcTable
        + "`"
        + " WHERE "
        + String.format(
            "%s = '%s'", ChangelogColumn.COLUMN_FAMILY.getBqColumnName(), SOURCE_COLUMN_FAMILY)
        + (rowkey != null
            ? String.format(
                " AND %s = '%s'", ChangelogColumn.ROW_KEY_STRING.getBqColumnName(), rowkey)
            : "")
        + (column != null
            ? String.format(" AND %s = '%s'", ChangelogColumn.COLUMN.getBqColumnName(), column)
            : "")
        + (value != null
            ? String.format(" AND %s = '%s'", ChangelogColumn.VALUE_STRING.getBqColumnName(), value)
            : "");
  }

  @Test
  public void testBigtableChangeStreamsToBigQueryColumnTransform() throws Exception {
    String appProfileId = generateAppProfileId();

    BigtableTableSpec cdcTableSpec = new BigtableTableSpec();
    cdcTableSpec.setCdcEnabled(true);
    cdcTableSpec.setColumnFamilies(Lists.asList(SOURCE_COLUMN_FAMILY, new String[] {}));

    String table = BigtableResourceManagerUtils.generateTableId("col-transform");
    String cdcTable = BigtableResourceManagerUtils.generateTableId("cdc-col-transform");
    bigtableResourceManager.createTable(table, cdcTableSpec);

    bigtableResourceManager.createAppProfile(
        appProfileId, true, bigtableResourceManager.getClusterNames());
    bigQueryResourceManager.createDataset(REGION);

    String rowkey = UUID.randomUUID().toString();
    String column = "ts_col";

    // Encode 1704067200000L (2024-01-01T00:00:00Z) as 8-byte big-endian uint64
    long timestampMillis = 1704067200000L;
    byte[] timestampBytes = new byte[8];
    ByteBuffer.wrap(timestampBytes).order(ByteOrder.BIG_ENDIAN).putLong(timestampMillis);

    Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder = Function.identity();
    launchInfo =
        launchTemplate(
            paramsAdder.apply(
                LaunchConfig.builder(testName, specPath)
                    .addParameter("bigtableReadTableId", table)
                    .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
                    .addParameter("bigtableChangeStreamAppProfile", appProfileId)
                    .addParameter("bigQueryDataset", bigQueryResourceManager.getDatasetId())
                    .addParameter("bigQueryChangelogTableName", cdcTable)
                    .addParameter(
                        "columnTransforms",
                        SOURCE_COLUMN_FAMILY + ":" + column + ":BIG_ENDIAN_TIMESTAMP")));

    assertThatPipeline(launchInfo).isRunning();

    RowMutation rowMutation =
        RowMutation.create(table, rowkey)
            .setCell(
                SOURCE_COLUMN_FAMILY,
                ByteString.copyFromUtf8(column),
                ByteString.copyFrom(timestampBytes));
    bigtableResourceManager.write(rowMutation);

    String query = newLookForValuesQuery(cdcTable, rowkey, column, null);
    waitForQueryToReturnRows(query, 1, true);

    TableResult tableResult = bigQueryResourceManager.runQuery(query);
    tableResult
        .iterateAll()
        .forEach(
            fvl -> {
              String value =
                  fvl.get(ChangelogColumn.VALUE_STRING.getBqColumnName()).getStringValue();
              // BigEndianTimestampTransformer formats as "yyyy-MM-dd HH:mm:ss.SSSSSS"
              assertEquals("2024-01-01 00:00:00.000000", value);
              assertFalse(fvl.get(ChangelogColumn.IS_GC.getBqColumnName()).getBooleanValue());
              assertEquals(
                  table, fvl.get(ChangelogColumn.SOURCE_TABLE.getBqColumnName()).getStringValue());
              assertEquals(
                  bigtableResourceManager.getInstanceId(),
                  fvl.get(ChangelogColumn.SOURCE_INSTANCE.getBqColumnName()).getStringValue());
            });
  }

  @Test
  public void testBigtableChangeStreamsToBigQueryProtoDecodeViaTransform() throws Exception {
    String appProfileId = generateAppProfileId();

    BigtableTableSpec cdcTableSpec = new BigtableTableSpec();
    cdcTableSpec.setCdcEnabled(true);
    cdcTableSpec.setColumnFamilies(Lists.asList(SOURCE_COLUMN_FAMILY, new String[] {}));

    String table = BigtableResourceManagerUtils.generateTableId("proto-transform");
    String cdcTable = BigtableResourceManagerUtils.generateTableId("cdc-proto-transform");
    bigtableResourceManager.createTable(table, cdcTableSpec);

    bigtableResourceManager.createAppProfile(
        appProfileId, true, bigtableResourceManager.getClusterNames());
    bigQueryResourceManager.createDataset(REGION);

    // Build a simple proto descriptor programmatically and upload as a .pb file to GCS
    FileDescriptorProto fileProto =
        FileDescriptorProto.newBuilder()
            .setName("test.proto")
            .setPackage("test")
            .addMessageType(
                DescriptorProto.newBuilder()
                    .setName("SimpleMessage")
                    .addField(
                        FieldDescriptorProto.newBuilder()
                            .setName("user_name")
                            .setNumber(1)
                            .setType(FieldDescriptorProto.Type.TYPE_STRING)
                            .build())
                    .addField(
                        FieldDescriptorProto.newBuilder()
                            .setName("id")
                            .setNumber(2)
                            .setType(FieldDescriptorProto.Type.TYPE_INT32)
                            .build())
                    .build())
            .build();

    FileDescriptorSet descriptorSet = FileDescriptorSet.newBuilder().addFile(fileProto).build();

    gcsClient.createArtifact("proto-schema.pb", descriptorSet.toByteArray());
    String protoSchemaGcsPath = getGcsPath("proto-schema.pb");

    // Build a protobuf message matching the descriptor
    FileDescriptor fileDescriptor = FileDescriptor.buildFrom(fileProto, new FileDescriptor[] {});
    Descriptor descriptor = fileDescriptor.findMessageTypeByName("SimpleMessage");

    DynamicMessage protoMessage =
        DynamicMessage.newBuilder(descriptor)
            .setField(descriptor.findFieldByName("user_name"), "test_user")
            .setField(descriptor.findFieldByName("id"), 42)
            .build();
    byte[] protoBytes = protoMessage.toByteArray();

    String rowkey = UUID.randomUUID().toString();
    String column = "proto_col";

    Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder = Function.identity();
    launchInfo =
        launchTemplate(
            paramsAdder.apply(
                LaunchConfig.builder(testName, specPath)
                    .addParameter("bigtableReadTableId", table)
                    .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
                    .addParameter("bigtableChangeStreamAppProfile", appProfileId)
                    .addParameter("bigQueryDataset", bigQueryResourceManager.getDatasetId())
                    .addParameter("bigQueryChangelogTableName", cdcTable)
                    .addParameter(
                        "columnTransforms",
                        SOURCE_COLUMN_FAMILY + ":" + column + ":PROTO_DECODE(test.SimpleMessage)")
                    .addParameter("protoSchemaPath", protoSchemaGcsPath)));

    assertThatPipeline(launchInfo).isRunning();

    RowMutation rowMutation =
        RowMutation.create(table, rowkey)
            .setCell(
                SOURCE_COLUMN_FAMILY,
                ByteString.copyFromUtf8(column),
                ByteString.copyFrom(protoBytes));
    bigtableResourceManager.write(rowMutation);

    String query = newLookForValuesQuery(cdcTable, rowkey, column, null);
    waitForQueryToReturnRows(query, 1, true);

    TableResult tableResult = bigQueryResourceManager.runQuery(query);
    tableResult
        .iterateAll()
        .forEach(
            fvl -> {
              String value =
                  fvl.get(ChangelogColumn.VALUE_STRING.getBqColumnName()).getStringValue();
              assertNotNull("Expected non-null decoded proto value", value);
              // JsonFormat.printer() uses camelCase by default: "userName" not "user_name"
              assertTrue(
                  "Expected decoded JSON to contain 'userName', got: " + value,
                  value.contains("userName"));
              assertTrue(
                  "Expected decoded JSON to contain 'test_user', got: " + value,
                  value.contains("test_user"));
              assertTrue(
                  "Expected decoded JSON to contain '42', got: " + value, value.contains("42"));
              assertFalse(fvl.get(ChangelogColumn.IS_GC.getBqColumnName()).getBooleanValue());
              assertEquals(
                  table, fvl.get(ChangelogColumn.SOURCE_TABLE.getBqColumnName()).getStringValue());
              assertEquals(
                  bigtableResourceManager.getInstanceId(),
                  fvl.get(ChangelogColumn.SOURCE_INSTANCE.getBqColumnName()).getStringValue());
            });
  }

  @Test
  public void testBigtableChangeStreamsToBigQueryProtoDecodeOversizedRoutesToDlq()
      throws Exception {
    String appProfileId = generateAppProfileId();

    BigtableTableSpec cdcTableSpec = new BigtableTableSpec();
    cdcTableSpec.setCdcEnabled(true);
    cdcTableSpec.setColumnFamilies(Lists.asList(SOURCE_COLUMN_FAMILY, new String[] {}));

    String table = BigtableResourceManagerUtils.generateTableId("proto-oversized");
    String cdcTable = BigtableResourceManagerUtils.generateTableId("cdc-proto-oversized");
    bigtableResourceManager.createTable(table, cdcTableSpec);

    bigtableResourceManager.createAppProfile(
        appProfileId, true, bigtableResourceManager.getClusterNames());
    bigQueryResourceManager.createDataset(REGION);

    FileDescriptorProto fileProto =
        FileDescriptorProto.newBuilder()
            .setName("test.proto")
            .setPackage("test")
            .addMessageType(
                DescriptorProto.newBuilder()
                    .setName("SimpleMessage")
                    .addField(
                        FieldDescriptorProto.newBuilder()
                            .setName("user_name")
                            .setNumber(1)
                            .setType(FieldDescriptorProto.Type.TYPE_STRING)
                            .build())
                    .addField(
                        FieldDescriptorProto.newBuilder()
                            .setName("id")
                            .setNumber(2)
                            .setType(FieldDescriptorProto.Type.TYPE_INT32)
                            .build())
                    .build())
            .build();

    FileDescriptorSet descriptorSet = FileDescriptorSet.newBuilder().addFile(fileProto).build();
    gcsClient.createArtifact("proto-schema-oversized.pb", descriptorSet.toByteArray());
    String protoSchemaGcsPath = getGcsPath("proto-schema-oversized.pb");

    FileDescriptor fileDescriptor = FileDescriptor.buildFrom(fileProto, new FileDescriptor[] {});
    Descriptor descriptor = fileDescriptor.findMessageTypeByName("SimpleMessage");

    // Small cell — must land in BigQuery.
    byte[] smallProtoBytes =
        DynamicMessage.newBuilder(descriptor)
            .setField(descriptor.findFieldByName("user_name"), "ok_user")
            .setField(descriptor.findFieldByName("id"), 1)
            .build()
            .toByteArray();

    // Big cell — must be routed to the severe DLQ, not BigQuery. Set user_name to ~50KB so the
    // decoded JSON comfortably exceeds the 10_000 byte cap configured below.
    String hugeName = org.apache.commons.lang3.StringUtils.repeat('x', 50_000);
    byte[] hugeProtoBytes =
        DynamicMessage.newBuilder(descriptor)
            .setField(descriptor.findFieldByName("user_name"), hugeName)
            .setField(descriptor.findFieldByName("id"), 2)
            .build()
            .toByteArray();

    String okRow = UUID.randomUUID().toString();
    String hugeRow = UUID.randomUUID().toString();
    String column = "proto_col";

    Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder = Function.identity();
    launchInfo =
        launchTemplate(
            paramsAdder.apply(
                LaunchConfig.builder(testName, specPath)
                    .addParameter("bigtableReadTableId", table)
                    .addParameter("bigtableReadInstanceId", bigtableResourceManager.getInstanceId())
                    .addParameter("bigtableChangeStreamAppProfile", appProfileId)
                    .addParameter("bigQueryDataset", bigQueryResourceManager.getDatasetId())
                    .addParameter("bigQueryChangelogTableName", cdcTable)
                    .addParameter(
                        "columnTransforms",
                        SOURCE_COLUMN_FAMILY + ":" + column + ":PROTO_DECODE(test.SimpleMessage)")
                    .addParameter("protoSchemaPath", protoSchemaGcsPath)
                    .addParameter("maxDecodedValueBytes", "10000")
                    .addParameter("dlqDirectory", getGcsPath("dlq"))));

    assertThatPipeline(launchInfo).isRunning();

    bigtableResourceManager.write(
        RowMutation.create(table, okRow)
            .setCell(
                SOURCE_COLUMN_FAMILY,
                ByteString.copyFromUtf8(column),
                ByteString.copyFrom(smallProtoBytes)));
    bigtableResourceManager.write(
        RowMutation.create(table, hugeRow)
            .setCell(
                SOURCE_COLUMN_FAMILY,
                ByteString.copyFromUtf8(column),
                ByteString.copyFrom(hugeProtoBytes)));

    // The small row should land in BigQuery.
    String okQuery = newLookForValuesQuery(cdcTable, okRow, column, null);
    waitForQueryToReturnRows(okQuery, 1, true);

    // The huge row should NOT land in BigQuery.
    String hugeQuery = newLookForValuesQuery(cdcTable, hugeRow, column, null);
    TableResult tableResult = bigQueryResourceManager.runQuery(hugeQuery);
    assertEquals("Oversized row must not reach BigQuery", 0L, tableResult.getTotalRows());

    // The huge row's metadata should appear in the severe DLQ.
    Storage storage = StorageOptions.newBuilder().setProjectId(PROJECT).build().getService();
    String filterPrefix =
        String.join("/", getClass().getSimpleName(), gcsClient.runId(), "dlq", "severe");
    LOG.info("Looking for oversized DLQ files with prefix: {}", filterPrefix);
    await("The oversized decoded value was not found in DLQ")
        .atMost(Duration.ofMinutes(30))
        .pollInterval(Duration.ofSeconds(1))
        .until(
            () -> {
              Page<Blob> blobs =
                  storage.list(artifactBucketName, BlobListOption.prefix(filterPrefix));
              for (Blob blob : blobs.iterateAll()) {
                if (blob.getName().contains(".temp-beam/")) {
                  continue;
                }
                String content = new String(storage.readAllBytes(blob.getBlobId()));
                if (content.contains("decoded_value_exceeds_max_bytes")
                    && content.contains(hugeRow)) {
                  ObjectMapper om = new ObjectMapper();
                  for (String line : content.split("\n")) {
                    if (line.isEmpty()) {
                      continue;
                    }
                    JsonNode record = om.readTree(line);
                    if (record.has("row_key") && hugeRow.equals(record.get("row_key").asText())) {
                      assertEquals(
                          "decoded_value_exceeds_max_bytes", record.get("reason").asText());
                      assertEquals(10_000L, record.get("max_bytes").asLong());
                      assertEquals(column, record.get("column").asText());
                      assertEquals(SOURCE_COLUMN_FAMILY, record.get("column_family").asText());
                      return true;
                    }
                  }
                }
              }
              return false;
            });
  }
}
