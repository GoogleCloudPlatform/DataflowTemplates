/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.it.gcp;

import static com.google.cloud.teleport.it.common.logging.LogStrings.formatForLogging;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.services.bigquery.model.TableRow;
import com.google.auth.Credentials;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.teleport.it.common.PipelineLauncher;
import com.google.cloud.teleport.it.common.PipelineLauncher.LaunchInfo;
import com.google.cloud.teleport.it.common.PipelineOperator;
import com.google.cloud.teleport.it.common.TestProperties;
import com.google.cloud.teleport.it.gcp.bigquery.BigQueryResourceManager;
import com.google.cloud.teleport.it.gcp.bigquery.DefaultBigQueryResourceManager;
import com.google.cloud.teleport.it.gcp.monitoring.MonitoringClient;
import com.google.common.base.MoreObjects;
import com.google.protobuf.util.Timestamps;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Base class for performance tests. It provides helper methods for common operations. */
@RunWith(JUnit4.class)
public abstract class LoadTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(LoadTestBase.class);
  // Dataflow resources cost factors (showing us-central-1 pricing).
  // See https://cloud.google.com/dataflow/pricing#pricing-details
  private static final double VCPU_PER_HR_BATCH = 0.056;
  private static final double VCPU_PER_HR_STREAMING = 0.069;
  private static final double MEM_PER_GB_HR_BATCH = 0.003557;
  private static final double MEM_PER_GB_HR_STREAMING = 0.0035557;
  private static final double PD_PER_GB_HR = 0.000054;
  private static final double PD_SSD_PER_GB_HR = 0.000298;
  private static final double SHUFFLE_PER_GB_BATCH = 0.011;
  private static final double SHUFFLE_PER_GB_STREAMING = 0.018;
  protected static final Credentials CREDENTIALS = TestProperties.googleCredentials();
  protected static final CredentialsProvider CREDENTIALS_PROVIDER =
      FixedCredentialsProvider.create(CREDENTIALS);
  protected static String project;
  protected static String region;
  protected static String hostIp;
  protected static MonitoringClient monitoringClient;
  protected static PipelineLauncher pipelineLauncher;
  protected static PipelineOperator pipelineOperator;

  protected String testName;

  @Rule
  public TestRule watcher =
      new TestWatcher() {
        protected void starting(Description description) {
          LOG.info(
              "Starting load test {}.{}", description.getClassName(), description.getMethodName());
          testName = description.getMethodName();
        }
      };

  @BeforeClass
  public static void setUpLoadTestBase() {
    project = TestProperties.project();
    region = TestProperties.region();
    hostIp = TestProperties.hostIp();
  }

  @Before
  public void setUp() throws IOException {
    monitoringClient =
        MonitoringClient.builder().setCredentialsProvider(CREDENTIALS_PROVIDER).build();
    pipelineLauncher = launcher();
    pipelineOperator = new PipelineOperator(pipelineLauncher);
  }

  @After
  public void tearDownLoadTestBase() {
    monitoringClient.cleanupAll();
  }

  abstract PipelineLauncher launcher();

  /**
   * Exports the metrics of given dataflow job to BigQuery.
   *
   * @param launchInfo Job info of the job
   * @param metrics metrics to export
   */
  public void exportMetricsToBigQuery(LaunchInfo launchInfo, Map<String, Double> metrics) {
    LOG.info("Exporting metrics:\n{}", formatForLogging(metrics));
    try {
      // either use the user specified project for exporting, or the same project
      String exportProject = MoreObjects.firstNonNull(TestProperties.exportProject(), project);
      BigQueryResourceManager bigQueryResourceManager =
          DefaultBigQueryResourceManager.builder(testName, exportProject)
              .setDatasetId(TestProperties.exportDataset())
              .setCredentials(CREDENTIALS)
              .build();
      // exporting metrics to bigQuery table
      Map<String, Object> rowContent = new HashMap<>();
      rowContent.put("timestamp", launchInfo.createTime());
      rowContent.put("sdk", launchInfo.sdk());
      rowContent.put("version", launchInfo.version());
      rowContent.put("job_type", launchInfo.jobType());
      rowContent.put("template_name", launchInfo.templateName());
      rowContent.put("template_version", launchInfo.templateVersion());
      rowContent.put("template_type", launchInfo.templateType());
      rowContent.put("test_name", testName);
      // Convert parameters map to list of table row since it's a repeated record
      List<TableRow> parameterRows = new ArrayList<>();
      for (String parameter : launchInfo.parameters().keySet()) {
        TableRow row =
            new TableRow()
                .set("name", parameter)
                .set("value", launchInfo.parameters().get(parameter));
        parameterRows.add(row);
      }
      rowContent.put("parameters", parameterRows);
      // Convert metrics map to list of table row since it's a repeated record
      List<TableRow> metricRows = new ArrayList<>();
      for (String metric : metrics.keySet()) {
        TableRow row = new TableRow().set("name", metric).set("value", metrics.get(metric));
        metricRows.add(row);
      }
      rowContent.put("metrics", metricRows);
      bigQueryResourceManager.write(
          TestProperties.exportTable(), RowToInsert.of("rowId", rowContent));
    } catch (IllegalStateException e) {
      LOG.error("Unable to export results to datastore. ", e);
    }
  }

  /**
   * Checks if the input PCollection has the expected number of messages.
   *
   * @param jobId JobId of the job
   * @param pcollection the input pcollection name
   * @param expectedElements expected number of messages
   * @return whether the input pcollection has the expected number of messages.
   */
  public boolean waitForNumMessages(String jobId, String pcollection, Long expectedElements) {
    try {
      // the element count metric always follows the pattern <pcollection name>-ElementCount
      String metricName = pcollection.replace(".", "-") + "-ElementCount";
      Double metric = pipelineLauncher.getMetric(project, region, jobId, metricName);
      if ((metric != null) && (metric >= (double) expectedElements)) {
        return true;
      }
      LOG.info(
          "Expected {} messages in input PCollection, but found {}.", expectedElements, metric);
      return false;
    } catch (Exception e) {
      LOG.warn("Encountered error when trying to measure input elements. ", e);
      return false;
    }
  }

  /**
   * Compute metrics of a Dataflow runner job.
   *
   * @param metrics a map of raw metrics. The results are also appened in the map.
   * @param launchInfo Job info of the job
   * @param inputPcollection input pcollection of the dataflow job to query additional metrics. If
   *     not provided, the metrics will not be calculated.
   * @param outputPcollection output pcollection of the dataflow job to query additional metrics. If
   *     not provided, the metrics will not be calculated.
   */
  private void computeDataflowMetrics(
      Map<String, Double> metrics,
      LaunchInfo launchInfo,
      @Nullable String inputPcollection,
      @Nullable String outputPcollection)
      throws ParseException {
    // cost info
    LOG.info("Calculating approximate cost for {} under {}", launchInfo.jobId(), project);
    double cost = 0;
    if (launchInfo.jobType().equals("JOB_TYPE_STREAMING")) {
      cost += metrics.get("TotalVcpuTime") / 3600 * VCPU_PER_HR_STREAMING;
      cost += (metrics.get("TotalMemoryUsage") / 1000) / 3600 * MEM_PER_GB_HR_STREAMING;
      cost += metrics.get("TotalShuffleDataProcessed") * SHUFFLE_PER_GB_STREAMING;
      // Also, add other streaming metrics
      metrics.putAll(getDataFreshnessMetrics(launchInfo));
      metrics.putAll(getSystemLatencyMetrics(launchInfo));
    } else {
      cost += metrics.get("TotalVcpuTime") / 3600 * VCPU_PER_HR_BATCH;
      cost += (metrics.get("TotalMemoryUsage") / 1000) / 3600 * MEM_PER_GB_HR_BATCH;
      cost += metrics.get("TotalShuffleDataProcessed") * SHUFFLE_PER_GB_BATCH;
    }
    cost += metrics.get("TotalPdUsage") / 3600 * PD_PER_GB_HR;
    cost += metrics.get("TotalSsdUsage") / 3600 * PD_SSD_PER_GB_HR;
    metrics.put("EstimatedCost", cost);
    metrics.put("ElapsedTime", monitoringClient.getElapsedTime(project, launchInfo));

    Double dataProcessed = monitoringClient.getDataProcessed(project, launchInfo, inputPcollection);
    if (dataProcessed != null) {
      metrics.put("EstimatedDataProcessedGB", dataProcessed / 1e9d);
    }
    metrics.putAll(getCpuUtilizationMetrics(launchInfo));
    metrics.putAll(
        getThroughputMetricsOfPcollection(launchInfo, inputPcollection, outputPcollection));
  }

  /**
   * Compute metrics of a Direct runner job.
   *
   * @param metrics a map of raw metrics. The results are also appened in the map.
   * @param launchInfo Job info of the job
   * @param inputPcollection input pcollection of the job to query additional metrics. Currently
   *     unused.
   * @param outputPcollection output pcollection of the job to query additional metrics. Currently
   *     unused.
   */
  private void computeDirectMetrics(
      Map<String, Double> metrics,
      LaunchInfo launchInfo,
      @Nullable String inputPcollection,
      @Nullable String outputPcollection)
      throws ParseException {
    // TODO: determine elapsed time more accurately if Direct runner supports do so.
    metrics.put(
        "ElapsedTime",
        0.001
            * (System.currentTimeMillis()
                - Timestamps.toMillis(Timestamps.parse(launchInfo.createTime()))));
  }

  /**
   * Computes the metrics of the given job using dataflow and monitoring clients.
   *
   * @param launchInfo Job info of the job
   * @param inputPcollection input pcollection of the dataflow job to query additional metrics. If
   *     not provided, the metrics will not be calculated.
   * @param outputPcollection output pcollection of the dataflow job to query additional metrics. If
   *     not provided, the metrics will not be calculated.
   * @return metrics
   * @throws IOException if there is an issue sending the request
   * @throws ParseException if timestamp is inaccurate
   * @throws InterruptedException thrown if thread is interrupted
   */
  protected Map<String, Double> getMetrics(
      LaunchInfo launchInfo, @Nullable String inputPcollection, @Nullable String outputPcollection)
      throws InterruptedException, IOException, ParseException {
    if ("DataflowRunner".equalsIgnoreCase(launchInfo.runner())) {
      // Dataflow runner specific metrics
      // In Dataflow runner, metrics take up to 3 minutes to show up
      // TODO(pranavbhandari): We should use a library like http://awaitility.org/ to poll for
      // metrics
      // instead of hard coding X minutes.
      LOG.info("Sleeping for 4 minutes to query Dataflow runner metrics.");
      Thread.sleep(Duration.ofMinutes(4).toMillis());
    }
    Map<String, Double> metrics = pipelineLauncher.getMetrics(project, region, launchInfo.jobId());
    if ("DataflowRunner".equalsIgnoreCase(launchInfo.runner())) {
      computeDataflowMetrics(metrics, launchInfo, inputPcollection, outputPcollection);
    } else if ("DirectRunner".equalsIgnoreCase(launchInfo.runner())) {
      computeDirectMetrics(metrics, launchInfo, inputPcollection, outputPcollection);
    }

    return metrics;
  }

  /**
   * Computes the metrics of the given job using dataflow and monitoring clients.
   *
   * @param launchInfo Job info of the job
   * @param inputPcollection input pcollection of the dataflow job to query additional metrics. If
   *     not provided, the metrics will not be calculated.
   * @return metrics
   * @throws IOException if there is an issue sending the request
   * @throws ParseException if timestamp is inaccurate
   * @throws InterruptedException thrown if thread is interrupted
   */
  protected Map<String, Double> getMetrics(LaunchInfo launchInfo, String inputPcollection)
      throws ParseException, InterruptedException, IOException {
    return getMetrics(launchInfo, inputPcollection, null);
  }

  /**
   * Computes the metrics of the given job using dataflow and monitoring clients.
   *
   * @param launchInfo Job info of the job
   * @return metrics
   * @throws IOException if there is an issue sending the request
   * @throws ParseException if timestamp is inaccurate
   * @throws InterruptedException thrown if thread is interrupted
   */
  protected Map<String, Double> getMetrics(LaunchInfo launchInfo)
      throws ParseException, InterruptedException, IOException {
    return getMetrics(launchInfo, null, null);
  }

  /**
   * Computes CPU Utilization metrics of the given job.
   *
   * @param launchInfo Job info of the job
   * @return CPU Utilization metrics of the job
   * @throws ParseException if timestamp is inaccurate
   */
  protected Map<String, Double> getCpuUtilizationMetrics(LaunchInfo launchInfo)
      throws ParseException {
    List<Double> cpuUtilization = monitoringClient.getCpuUtilization(project, launchInfo);
    Map<String, Double> cpuUtilizationMetrics = new HashMap<>();
    if (cpuUtilization == null) {
      return cpuUtilizationMetrics;
    }
    cpuUtilizationMetrics.put("AvgCpuUtilization", calculateAverage(cpuUtilization));
    cpuUtilizationMetrics.put("MaxCpuUtilization", Collections.max(cpuUtilization));
    return cpuUtilizationMetrics;
  }

  /**
   * Computes throughput metrics of the given ptransform in dataflow job.
   *
   * @param launchInfo Job info of the job
   * @param inputPtransform input ptransform of the dataflow job to query additional metrics
   * @param outputPtransform output ptransform of the dataflow job to query additional metrics
   * @return throughput metrics of the ptransform
   * @throws ParseException if timestamp is inaccurate
   */
  protected Map<String, Double> getThroughputMetricsOfPtransform(
      LaunchInfo launchInfo, @Nullable String inputPtransform, @Nullable String outputPtransform)
      throws ParseException {
    List<Double> inputThroughput =
        monitoringClient.getThroughputOfPtransform(project, launchInfo, inputPtransform);
    List<Double> outputThroughput =
        monitoringClient.getThroughputOfPtransform(project, launchInfo, outputPtransform);
    return getThroughputMetrics(inputThroughput, outputThroughput);
  }

  /**
   * Computes throughput metrics of the given pcollection in dataflow job.
   *
   * @param launchInfo Job info of the job
   * @param inputPcollection input pcollection of the dataflow job to query additional metrics
   * @param outputPcollection output pcollection of the dataflow job to query additional metrics
   * @return throughput metrics of the pcollection
   * @throws ParseException if timestamp is inaccurate
   */
  protected Map<String, Double> getThroughputMetricsOfPcollection(
      LaunchInfo launchInfo, @Nullable String inputPcollection, @Nullable String outputPcollection)
      throws ParseException {
    List<Double> inputThroughput =
        monitoringClient.getThroughputOfPcollection(project, launchInfo, inputPcollection);
    List<Double> outputThroughput =
        monitoringClient.getThroughputOfPcollection(project, launchInfo, outputPcollection);
    return getThroughputMetrics(inputThroughput, outputThroughput);
  }

  /**
   * Computes Data freshness metrics of the given job.
   *
   * @param launchInfo Job info of the job
   * @return Data freshness metrics of the job
   * @throws ParseException if timestamp is inaccurate
   */
  protected Map<String, Double> getDataFreshnessMetrics(LaunchInfo launchInfo)
      throws ParseException {
    List<Double> dataFreshness = monitoringClient.getDataFreshness(project, launchInfo);
    Map<String, Double> dataFreshnessMetrics = new HashMap<>();
    if (dataFreshness == null) {
      return dataFreshnessMetrics;
    }
    dataFreshnessMetrics.put("AvgDataFreshness", calculateAverage(dataFreshness));
    dataFreshnessMetrics.put("MaxDataFreshness", Collections.max(dataFreshness));
    return dataFreshnessMetrics;
  }

  /**
   * Computes System latency metrics of the given job.
   *
   * @param launchInfo Job info of the job
   * @return System latency metrics of the job
   * @throws ParseException if timestamp is inaccurate
   */
  protected Map<String, Double> getSystemLatencyMetrics(LaunchInfo launchInfo)
      throws ParseException {
    List<Double> systemLatency = monitoringClient.getSystemLatency(project, launchInfo);
    Map<String, Double> systemLatencyMetrics = new HashMap<>();
    if (systemLatency == null) {
      return systemLatencyMetrics;
    }
    systemLatencyMetrics.put("AvgSystemLatency", calculateAverage(systemLatency));
    systemLatencyMetrics.put("MaxSystemLatency", Collections.max(systemLatency));
    return systemLatencyMetrics;
  }

  private Map<String, Double> getThroughputMetrics(
      List<Double> inputThroughput, List<Double> outputThroughput) {
    Map<String, Double> throughputMetrics = new HashMap<>();
    if (inputThroughput != null) {
      throughputMetrics.put("AvgInputThroughput", calculateAverage(inputThroughput));
      throughputMetrics.put("MaxInputThroughput", Collections.max(inputThroughput));
    }
    if (outputThroughput != null) {
      throughputMetrics.put("AvgOutputThroughput", calculateAverage(outputThroughput));
      throughputMetrics.put("MaxOutputThroughput", Collections.max(outputThroughput));
    }
    return throughputMetrics;
  }

  private Double calculateAverage(List<Double> values) {
    return values.stream().mapToDouble(d -> d).average().orElse(0.0);
  }

  public static PipelineOperator.Config createConfig(LaunchInfo info, Duration timeout) {
    return PipelineOperator.Config.builder()
        .setJobId(info.jobId())
        .setProject(project)
        .setRegion(region)
        .setTimeoutAfter(timeout)
        .build();
  }
}
