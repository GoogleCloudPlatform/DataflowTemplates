/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.templates.python;

import static com.google.common.truth.Truth.assertThat;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.common.collect.ImmutableMap;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigquery.matchers.BigQueryAsserts;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.pubsub.conditions.PubsubMessagesCheck;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for {@link BigQueryAnomalyDetection}.
 *
 * <p>Inserts normal baseline data into a BQ table, then injects an anomalous spike, and verifies
 * that the pipeline detects the anomaly and publishes it to Pub/Sub.
 */
@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(BigQueryAnomalyDetection.class)
@RunWith(JUnit4.class)
public final class BigQueryAnomalyDetectionIT extends TemplateTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryAnomalyDetectionIT.class);

  private static final String TABLE_NAME = "anomaly_test";
  private static final String SINK_TABLE_NAME = "anomaly_results";

  // Window size used in all metric specs (seconds).
  private static final int WINDOW_SIZE_SEC = 1;

  // Baseline: insert 100 rows every second for ~6 minutes (360 batches).
  // Each batch lands in one 1-second window, giving the detector many data
  // points to warm up before the anomaly spike.
  private static final int BASELINE_BATCHES = 360;
  private static final int ROWS_PER_BATCH = 100;
  private static final long BATCH_INTERVAL_MS = 1000;
  private static final double NORMAL_AMOUNT = 10.0;

  // Anomaly: 100 rows with amount=10000.0 → window MEAN ≈ 10000.0 (1000x spike).
  private static final double ANOMALY_AMOUNT = 10000.0;

  private BigQueryResourceManager bigQueryResourceManager;
  private PubsubResourceManager pubsubResourceManager;

  @Before
  public void setUp() throws IOException {
    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, PROJECT, credentials).build();
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(bigQueryResourceManager, pubsubResourceManager);
  }

  @Test
  public void testDetectsAnomalyAndPublishesToPubSub() throws IOException, InterruptedException {
    testSimpleSumMetric();
  }

  @Test
  public void testGroupedRatioMetric() throws IOException, InterruptedException {
    testGroupedRatioMetricImpl("sharded");
  }

  @Test
  public void testGroupedRatioMetricWithPrecombine() throws IOException, InterruptedException {
    testGroupedRatioMetricImpl("precombine");
  }

  @Test
  public void testThresholdDetector() throws IOException, InterruptedException {
    testThresholdDetectorImpl();
  }

  // -------------------------------------------------------------------------
  // Test implementations
  // -------------------------------------------------------------------------

  private void testSimpleSumMetric() throws IOException, InterruptedException {
    // --- Arrange ---

    // Create BQ table. APPENDS() does not require change tracking.
    Schema schema =
        Schema.of(
            Field.of("id", StandardSQLTypeName.INT64),
            Field.of("amount", StandardSQLTypeName.FLOAT64));
    bigQueryResourceManager.createDataset(REGION);
    bigQueryResourceManager.createTable(TABLE_NAME, schema);

    // Create Pub/Sub topic and subscription to verify anomaly output.
    TopicName outputTopic = pubsubResourceManager.createTopic("anomaly-output");
    SubscriptionName outputSubscription =
        pubsubResourceManager.createSubscription(outputTopic, "anomaly-output-sub");

    // 1-second fixed windows, MEAN of amount, RobustZScore detector.
    String metricSpec =
        "{\"aggregation\":{\"window\":{\"type\":\"fixed\","
            + "\"size_seconds\":"
            + WINDOW_SIZE_SEC
            + "},\"measures\":[{\"field\":\"amount\","
            + "\"agg\":\"MEAN\",\"alias\":\"avg_amount\"}]}}";
    String detectorSpec = "{\"type\":\"RobustZScore\"}";

    String tableRef =
        String.format(
            "%s:%s.%s",
            bigQueryResourceManager.getProjectId(),
            bigQueryResourceManager.getDatasetId(),
            TABLE_NAME);

    String sinkTableRef =
        String.format(
            "%s:%s.%s",
            bigQueryResourceManager.getProjectId(),
            bigQueryResourceManager.getDatasetId(),
            SINK_TABLE_NAME);

    // --- Act ---

    // Launch the pipeline first.
    // start_offset_sec=300 ensures it reads data inserted before and after launch.
    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("table", tableRef)
            .addParameter("metric_spec", metricSpec)
            .addParameter("detector_spec", detectorSpec)
            .addParameter("topic", outputTopic.toString())
            .addParameter("poll_interval_sec", "15")
            .addParameter("start_offset_sec", "300")
            .addParameter("duration_sec", "600")
            .addParameter("log_all_results", "true")
            .addParameter("sink_table", sinkTableRef);

    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    // Insert baseline data: 360 batches x 100 rows, one batch every second (~6 min).
    // This gives the pipeline time to start workers and the detector time to warm up
    // on many 1-second windows of normal data before the anomaly arrives.
    LOG.info(
        "Inserting {} batches of {} rows every {}ms (amount={})",
        BASELINE_BATCHES,
        ROWS_PER_BATCH,
        BATCH_INTERVAL_MS,
        NORMAL_AMOUNT);
    Random rng = new Random(42);
    int rowId = 0;
    for (int batch = 0; batch < BASELINE_BATCHES; batch++) {
      List<RowToInsert> rows = new ArrayList<>();
      for (int i = 0; i < ROWS_PER_BATCH; i++) {
        // Tiny variance (stdev ~0.5) so MAD/stdev > 0 for the detector.
        double amount = NORMAL_AMOUNT + rng.nextGaussian() * 0.5;
        rows.add(RowToInsert.of(ImmutableMap.of("id", ++rowId, "amount", amount)));
      }
      bigQueryResourceManager.write(TABLE_NAME, rows);
      if (batch < BASELINE_BATCHES - 1) {
        TimeUnit.MILLISECONDS.sleep(BATCH_INTERVAL_MS);
      }
      if ((batch + 1) % 60 == 0) {
        LOG.info("Inserted batch {}/{} ({} rows so far)", batch + 1, BASELINE_BATCHES, rowId);
      }
    }
    LOG.info("Inserted {} baseline rows total", rowId);

    // Pause to ensure the anomaly lands in a clean window, not mixed with baseline.
    TimeUnit.SECONDS.sleep(2);

    // Insert anomalous spike: same batch size but 1000x the amount.
    LOG.info("Inserting anomalous batch ({} rows, amount={})", ROWS_PER_BATCH, ANOMALY_AMOUNT);
    List<RowToInsert> anomalyRows = new ArrayList<>();
    for (int i = 0; i < ROWS_PER_BATCH; i++) {
      anomalyRows.add(RowToInsert.of(ImmutableMap.of("id", ++rowId, "amount", ANOMALY_AMOUNT)));
    }
    bigQueryResourceManager.write(TABLE_NAME, anomalyRows);
    LOG.info("Inserted {} anomalous rows", anomalyRows.size());

    // --- Assert ---

    // Wait for at least 1 anomaly message on the Pub/Sub subscription.
    PubsubMessagesCheck pubsubCheck =
        PubsubMessagesCheck.builder(pubsubResourceManager, outputSubscription)
            .setMinMessages(1)
            .build();

    Result result = pipelineOperator().waitForConditionAndCancel(createConfig(info), pubsubCheck);
    assertThatResult(result).meetsConditions();

    // Verify the anomaly message content.
    List<ReceivedMessage> messages = pubsubCheck.getReceivedMessageList();
    assertThat(messages).isNotEmpty();

    String messageData = messages.get(0).getMessage().getData().toStringUtf8();
    LOG.info("Received anomaly message: {}", messageData);
    JSONObject payload = new JSONObject(messageData);

    assertThat(payload.getString("event_description")).contains("Anomaly detected");
    assertThat(payload.getString("agent_id")).isEqualTo("RobustZScore");

    // --- Verify BQ sink table ---
    verifySinkTable(SINK_TABLE_NAME, WINDOW_SIZE_SEC, null /* no keys expected */);
  }

  /**
   * Tests a grouped ratio metric (CTR = clicks / impressions) with anomaly detection.
   *
   * <p>Inserts baseline data with a stable click-through rate (~10%) for two campaign types, then
   * injects an anomalous spike in one campaign's CTR (~90%), and verifies the pipeline detects the
   * anomaly and publishes it to Pub/Sub with the correct key.
   */
  private void testGroupedRatioMetricImpl(String fanoutStrategy)
      throws IOException, InterruptedException {
    // --- Arrange ---

    String tableName = "ctr_test";

    Schema schema =
        Schema.of(
            Field.of("id", StandardSQLTypeName.INT64),
            Field.of("campaign_type", StandardSQLTypeName.STRING),
            Field.of("is_click", StandardSQLTypeName.INT64));
    bigQueryResourceManager.createDataset(REGION);
    bigQueryResourceManager.createTable(tableName, schema);

    TopicName outputTopic = pubsubResourceManager.createTopic("ctr-anomaly-output");
    SubscriptionName outputSubscription =
        pubsubResourceManager.createSubscription(outputTopic, "ctr-anomaly-output-sub");

    String sinkTableName = "ctr_results";

    // Grouped ratio: CTR = clicks / impressions per campaign_type.
    String metricSpec =
        "{\"aggregation\":{\"window\":{\"type\":\"fixed\","
            + "\"size_seconds\":"
            + WINDOW_SIZE_SEC
            + "},\"group_by\":[\"campaign_type\"],"
            + "\"measures\":[{\"field\":\"is_click\",\"agg\":\"SUM\",\"alias\":\"clicks\"},"
            + "{\"field\":\"is_click\",\"agg\":\"COUNT\",\"alias\":\"impressions\"}]},"
            + "\"measure_combiner\":{\"expression\":\"clicks / impressions\"}}";
    String detectorSpec = "{\"type\":\"RobustZScore\"}";

    String tableRef =
        String.format(
            "%s:%s.%s",
            bigQueryResourceManager.getProjectId(),
            bigQueryResourceManager.getDatasetId(),
            tableName);

    String sinkTableRef =
        String.format(
            "%s:%s.%s",
            bigQueryResourceManager.getProjectId(),
            bigQueryResourceManager.getDatasetId(),
            sinkTableName);

    // --- Act ---

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("table", tableRef)
            .addParameter("metric_spec", metricSpec)
            .addParameter("detector_spec", detectorSpec)
            .addParameter("topic", outputTopic.toString())
            .addParameter("poll_interval_sec", "15")
            .addParameter("start_offset_sec", "300")
            .addParameter("duration_sec", "600")
            .addParameter("log_all_results", "true")
            .addParameter("sink_table", sinkTableRef);

    options.addParameter("fanout_strategy", fanoutStrategy);

    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    // Insert baseline data: CTR ~10% for both campaigns.
    // Campaigns are round-robin (deterministic split), clicks are random
    // to provide natural per-window variance needed by RobustZScore (MAD > 0).
    // 500 rows per batch (250 per key) keeps per-window CTR tight (~0.08-0.12).
    // 360 batches, one batch every second (~6 min).
    int ctrRowsPerBatch = 500;
    LOG.info(
        "Inserting {} batches of {} rows every {}ms (CTR ~10%%)",
        BASELINE_BATCHES, ctrRowsPerBatch, BATCH_INTERVAL_MS);
    Random rng = new Random(42);
    String[] campaigns = {"search", "display"};
    int rowId = 0;
    for (int batch = 0; batch < BASELINE_BATCHES; batch++) {
      List<RowToInsert> rows = new ArrayList<>();
      for (int i = 0; i < ctrRowsPerBatch; i++) {
        String campaign = campaigns[i % campaigns.length];
        int isClick = rng.nextDouble() < 0.10 ? 1 : 0;
        rows.add(
            RowToInsert.of(
                ImmutableMap.of("id", ++rowId, "campaign_type", campaign, "is_click", isClick)));
      }
      bigQueryResourceManager.write(tableName, rows);
      if (batch < BASELINE_BATCHES - 1) {
        TimeUnit.MILLISECONDS.sleep(BATCH_INTERVAL_MS);
      }
      if ((batch + 1) % 60 == 0) {
        LOG.info("Inserted batch {}/{} ({} rows so far)", batch + 1, BASELINE_BATCHES, rowId);
      }
    }
    LOG.info("Inserted {} baseline rows total", rowId);

    TimeUnit.SECONDS.sleep(2);

    // Inject anomaly: "search" campaign CTR spikes to ~90%.
    LOG.info("Inserting anomalous batch ({} rows, search CTR ~90%%)", ctrRowsPerBatch);
    List<RowToInsert> anomalyRows = new ArrayList<>();
    for (int i = 0; i < ctrRowsPerBatch; i++) {
      int isClick = rng.nextDouble() < 0.90 ? 1 : 0;
      anomalyRows.add(
          RowToInsert.of(
              ImmutableMap.of("id", ++rowId, "campaign_type", "search", "is_click", isClick)));
    }
    bigQueryResourceManager.write(tableName, anomalyRows);
    LOG.info("Inserted {} anomalous rows", anomalyRows.size());

    // --- Assert ---

    PubsubMessagesCheck pubsubCheck =
        PubsubMessagesCheck.builder(pubsubResourceManager, outputSubscription)
            .setMinMessages(1)
            .build();

    Result result = pipelineOperator().waitForConditionAndCancel(createConfig(info), pubsubCheck);
    assertThatResult(result).meetsConditions();

    List<ReceivedMessage> messages = pubsubCheck.getReceivedMessageList();
    assertThat(messages).isNotEmpty();

    String messageData = messages.get(0).getMessage().getData().toStringUtf8();
    LOG.info("Received CTR anomaly message: {}", messageData);
    JSONObject payload = new JSONObject(messageData);

    assertThat(payload.getString("event_description")).contains("Anomaly detected");
    assertThat(payload.getString("agent_id")).isEqualTo("RobustZScore");
    assertThat(payload.has("key")).isTrue();

    // --- Verify BQ sink table ---
    Set<String> expectedKeys = Set.of("('search',)", "('display',)");
    verifySinkTable(sinkTableName, WINDOW_SIZE_SEC, expectedKeys);
  }

  /**
   * Tests the Threshold detector with a sliding window and sharded fanout.
   *
   * <p>Uses a sliding-window MEAN metric with expression (value >= 100). Inserts rows with
   * amount=10 (below threshold), then a batch with amount=500 (above threshold). No warmup period
   * is needed — the threshold fires immediately on any value that satisfies the expression.
   */
  private void testThresholdDetectorImpl() throws IOException, InterruptedException {
    // --- Arrange ---

    String tableName = "threshold_test";

    Schema schema =
        Schema.of(
            Field.of("id", StandardSQLTypeName.INT64),
            Field.of("amount", StandardSQLTypeName.FLOAT64));
    bigQueryResourceManager.createDataset(REGION);
    bigQueryResourceManager.createTable(tableName, schema);

    TopicName outputTopic = pubsubResourceManager.createTopic("threshold-output");
    SubscriptionName outputSubscription =
        pubsubResourceManager.createSubscription(outputTopic, "threshold-output-sub");

    String sinkTableName = "threshold_results";

    // Sliding-window MEAN metric with Threshold detector and sharded fanout.
    String metricSpec =
        "{\"aggregation\":{\"window\":{\"type\":\"sliding\","
            + "\"size_seconds\":2,\"period_seconds\":1"
            + "},\"measures\":[{\"field\":\"amount\","
            + "\"agg\":\"MEAN\",\"alias\":\"avg_amount\"}]}}";
    String detectorSpec = "{\"type\":\"Threshold\",\"expression\":\"value >= 100\"}";

    String tableRef =
        String.format(
            "%s:%s.%s",
            bigQueryResourceManager.getProjectId(),
            bigQueryResourceManager.getDatasetId(),
            tableName);

    String sinkTableRef =
        String.format(
            "%s:%s.%s",
            bigQueryResourceManager.getProjectId(),
            bigQueryResourceManager.getDatasetId(),
            sinkTableName);

    // --- Act ---

    LaunchConfig.Builder options =
        LaunchConfig.builder(testName, specPath)
            .addParameter("table", tableRef)
            .addParameter("metric_spec", metricSpec)
            .addParameter("detector_spec", detectorSpec)
            .addParameter("topic", outputTopic.toString())
            .addParameter("poll_interval_sec", "15")
            .addParameter("start_offset_sec", "300")
            .addParameter("duration_sec", "600")
            .addParameter("log_all_results", "true")
            .addParameter("sink_table", sinkTableRef)
            .addParameter("fanout_strategy", "sharded");

    LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    // Insert baseline data: amount=10, well below the threshold of 100.
    // Fewer batches needed since no warmup — just enough for the pipeline to start.
    int baselineBatches = 120;
    LOG.info(
        "Inserting {} batches of {} rows every {}ms (amount=10, below threshold)",
        baselineBatches,
        ROWS_PER_BATCH,
        BATCH_INTERVAL_MS);
    int rowId = 0;
    for (int batch = 0; batch < baselineBatches; batch++) {
      List<RowToInsert> rows = new ArrayList<>();
      for (int i = 0; i < ROWS_PER_BATCH; i++) {
        rows.add(RowToInsert.of(ImmutableMap.of("id", ++rowId, "amount", 10.0)));
      }
      bigQueryResourceManager.write(tableName, rows);
      if (batch < baselineBatches - 1) {
        TimeUnit.MILLISECONDS.sleep(BATCH_INTERVAL_MS);
      }
      if ((batch + 1) % 60 == 0) {
        LOG.info("Inserted batch {}/{} ({} rows so far)", batch + 1, baselineBatches, rowId);
      }
    }
    LOG.info("Inserted {} baseline rows total", rowId);

    TimeUnit.SECONDS.sleep(2);

    // Insert above-threshold batch: amount=500, well above threshold of 100.
    LOG.info("Inserting above-threshold batch ({} rows, amount=500)", ROWS_PER_BATCH);
    List<RowToInsert> alertRows = new ArrayList<>();
    for (int i = 0; i < ROWS_PER_BATCH; i++) {
      alertRows.add(RowToInsert.of(ImmutableMap.of("id", ++rowId, "amount", 500.0)));
    }
    bigQueryResourceManager.write(tableName, alertRows);
    LOG.info("Inserted {} above-threshold rows", alertRows.size());

    // --- Assert ---

    PubsubMessagesCheck pubsubCheck =
        PubsubMessagesCheck.builder(pubsubResourceManager, outputSubscription)
            .setMinMessages(1)
            .build();

    Result result = pipelineOperator().waitForConditionAndCancel(createConfig(info), pubsubCheck);
    assertThatResult(result).meetsConditions();

    List<ReceivedMessage> messages = pubsubCheck.getReceivedMessageList();
    assertThat(messages).isNotEmpty();

    String messageData = messages.get(0).getMessage().getData().toStringUtf8();
    LOG.info("Received threshold alert message: {}", messageData);
    JSONObject payload = new JSONObject(messageData);

    assertThat(payload.getString("event_description")).contains("Anomaly detected");
    assertThat(payload.getString("agent_id")).isEqualTo("Threshold(value >= 100)");

    // --- Verify BQ sink table ---
    verifySinkTable(sinkTableName, 2, null /* no keys expected */);
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  /**
   * Verifies the BQ sink table written by the pipeline.
   *
   * @param tableName sink table name
   * @param windowSizeSec expected window duration in seconds
   * @param expectedKeys if non-null, the set of expected key values; if null, key must be absent
   */
  private void verifySinkTable(String tableName, int windowSizeSec, Set<String> expectedKeys) {
    TableResult tableResult = bigQueryResourceManager.readTable(tableName);
    List<Map<String, Object>> rows = BigQueryAsserts.tableResultToRecords(tableResult);

    assertThat(rows).isNotEmpty();
    LOG.info("Sink table '{}' has {} rows", tableName, rows.size());

    boolean hasOutlier = false;
    Set<String> observedKeys = new HashSet<>();
    Set<Integer> validLabels = Set.of(-2, 0, 1);

    for (Map<String, Object> row : rows) {
      // All expected columns exist.
      assertThat(row).containsKey("window_start");
      assertThat(row).containsKey("window_end");
      assertThat(row).containsKey("value");
      assertThat(row).containsKey("label");

      // Window timestamps parse as valid ISO-8601 UTC.
      String windowStart = row.get("window_start").toString();
      String windowEnd = row.get("window_end").toString();
      Instant start = Instant.parse(windowStart);
      Instant end = Instant.parse(windowEnd);

      // Window duration matches expected size.
      long durationSec = end.getEpochSecond() - start.getEpochSecond();
      assertThat(durationSec).isEqualTo(windowSizeSec);

      // Value is a valid number.
      assertThat(row.get("value")).isNotNull();
      double value = ((Number) row.get("value")).doubleValue();
      assertThat(Double.isNaN(value)).isFalse();

      // Label is valid.
      int label = ((Number) row.get("label")).intValue();
      assertThat(validLabels).contains(label);

      if (label == 1) {
        hasOutlier = true;
      }

      // Track keys.
      if (row.containsKey("key") && row.get("key") != null) {
        observedKeys.add(row.get("key").toString());
      }
    }

    // At least one outlier was detected.
    assertThat(hasOutlier).isTrue();

    // Verify keys.
    if (expectedKeys != null) {
      assertThat(observedKeys).isEqualTo(expectedKeys);
    } else {
      assertThat(observedKeys).isEmpty();
    }

    LOG.info(
        "Sink table '{}' verification passed ({} rows, outlier found)", tableName, rows.size());
  }
}
