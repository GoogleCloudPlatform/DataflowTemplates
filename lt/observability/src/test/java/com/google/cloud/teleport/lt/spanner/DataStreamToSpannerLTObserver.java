package com.google.cloud.teleport.lt.spanner;

import static org.junit.Assert.assertTrue;

import com.google.auth.Credentials;
import com.google.cloud.teleport.lt.AssertAll;
import com.google.cloud.teleport.lt.dataset.bigquery.BigQueryPerfDataset;
import com.google.cloud.teleport.lt.dataset.bigquery.BigQueryPerfDatasetFetcher;
import com.google.cloud.teleport.lt.rules.DetectMetricDeviationConditionCheck;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.model.MultipleFailureException;

@RunWith(JUnit4.class)
public class DataStreamToSpannerLTObserver {

  protected static final Credentials CREDENTIALS = TestProperties.googleCredentials();

  private BigQueryResourceManager bigQueryResourceManager;

  @Before
  public void setup() {
    String projectId = TestProperties.exportProject();
    bigQueryResourceManager =
        BigQueryResourceManager.builder("Load-test-analyzer", projectId, CREDENTIALS)
            .setDatasetId(TestProperties.exportDataset())
            .build();
    // *DO NOT CALL cleanup on this bigQueryResourceManager.* It would delete the tables in the
    // dataset.
  }

  @Test
  public void observeDSToSpanner100GBLT() throws MultipleFailureException {
    // Fetch data
    BigQueryPerfDatasetFetcher datasetFetcher =
        BigQueryPerfDatasetFetcher.builder()
            .setTemplateName("cloud_datastream_to_spanner")
            .setTestName("backfill100Gb")
            .setNumRows(11)
            .setBigQueryResourceManager(bigQueryResourceManager)
            .build();
    BigQueryPerfDataset dataset = datasetFetcher.fetch();

    // Assert using condition checks
    DetectMetricDeviationConditionCheck totalTimeCheck =
        DetectMetricDeviationConditionCheck.builder()
            .setDataset(dataset)
            .setMetricName("RunTime")
            .setPercentageDeviation(25)
            .build();

    DetectMetricDeviationConditionCheck totalCostCheck =
        DetectMetricDeviationConditionCheck.builder()
            .setDataset(dataset)
            .setMetricName("EstimatedCost")
            .setPercentageDeviation(25)
            .build();

    AssertAll asserts = new AssertAll();
    asserts.assertAll(
        () -> assertTrue(totalTimeCheck.get()), () -> assertTrue(totalCostCheck.get()));
  }
}
