package com.google.cloud.teleport.lt.spanner;

import com.google.auth.Credentials;
import com.google.cloud.teleport.lt.AssertAll;
import com.google.cloud.teleport.lt.dataset.bigquery.BigQueryPerfDataset;
import com.google.cloud.teleport.lt.dataset.bigquery.BigQueryPerfDatasetFetcher;
import com.google.cloud.teleport.lt.dataset.bigquery.PerfResultRow;
import com.google.cloud.teleport.lt.rules.DetectMetricDeviationConditionCheck;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.model.MultipleFailureException;

/** Load test observer for all the spanner templates. */
@RunWith(Parameterized.class)
public class GenericTemplateLTObserver {

  private static final Credentials CREDENTIALS = TestProperties.googleCredentials();

  private static BigQueryResourceManager bigQueryResourceManager;

  @Parameters
  public static Collection<String[]> getParameters() {
    String projectId = TestProperties.exportProject();
    bigQueryResourceManager =
        BigQueryResourceManager.builder("Load-test-analyzer", projectId, CREDENTIALS)
            .setDatasetId(TestProperties.exportDataset())
            .build();
    // *DO NOT CALL cleanup on this bigQueryResourceManager.* It would delete the tables in the
    // dataset.
    return getSpannerTemplateAndTestNames().stream()
        .map(x -> x.toArray(new String[0]))
        .collect(Collectors.toList());
  }

  @Parameter(0)
  public String templateName;

  @Parameter(1)
  public String testName;

  @Test
  public void observeLoadTest() throws MultipleFailureException {
    String query = DetectMetricDeviationConditionCheck.constructQuery(templateName, testName, "11");
    // Fetch data
    BigQueryPerfDatasetFetcher datasetFetcher =
        BigQueryPerfDatasetFetcher.builder()
            .setQuery(query)
            .setBigQueryResourceManager(bigQueryResourceManager)
            .build();
    BigQueryPerfDataset dataset = datasetFetcher.fetch();

    // Assert condition checks
    List<Runnable> assertions = new ArrayList<>();
    DetectMetricDeviationConditionCheck totalTimeCheck =
        createMetricDeviationConditionCheck("RunTime", dataset, templateName, testName);
    assertions.add(totalTimeCheck::assertTrue);

    AssertAll asserts = new AssertAll();
    asserts.assertAll(assertions.toArray(new Runnable[0]));
  }

  private static List<List<String>> getSpannerTemplateAndTestNames() {
    List<List<String>> allTemplates = getAllTemplateAndTestNames();
    // relies on the template_name to contain "spanner" if it is a spanner template.
    return allTemplates.stream()
        .filter(x -> x.get(0).toLowerCase().contains("spanner"))
        .collect(Collectors.toList());
  }

  private static List<List<String>> getAllTemplateAndTestNames() {
    String query =
        "SELECT template_name, test_name "
            + "FROM `%s.%s.%s` "
            + " GROUP BY template_name, test_name";
    query =
        String.format(
            query,
            TestProperties.exportProject(),
            TestProperties.exportDataset(),
            TestProperties.exportTable());

    BigQueryPerfDatasetFetcher datasetFetcher =
        BigQueryPerfDatasetFetcher.builder()
            .setQuery(query)
            .setBigQueryResourceManager(bigQueryResourceManager)
            .build();
    BigQueryPerfDataset dataset = datasetFetcher.fetch();

    List<List<String>> result = new ArrayList<>();

    for (PerfResultRow resultRow : dataset.rows()) {
      Object templateName = resultRow.row.get("template_name");
      Object testName = resultRow.row.get("test_name");
      if (templateName != null && testName != null) {
        result.add(List.of(templateName.toString(), testName.toString()));
      }
    }
    return result;
  }

  private DetectMetricDeviationConditionCheck createMetricDeviationConditionCheck(
      String metricName, BigQueryPerfDataset dataset, String templateName, String testName) {
    return DetectMetricDeviationConditionCheck.builder()
        .setDataset(dataset)
        .setMetricName(metricName)
        .setPercentageDeviation(25)
        .setTestCaseDescription(
            String.format("templateName=%s testName=%s ", templateName, testName))
        .build();
  }
}
