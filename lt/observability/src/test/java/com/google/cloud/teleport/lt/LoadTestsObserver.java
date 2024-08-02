package com.google.cloud.teleport.lt;

import com.google.auth.Credentials;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.teleport.lt.config.ObservabilityConfig;
import com.google.cloud.teleport.lt.rules.RegressionRule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class LoadTestsObserver {

  protected static final Credentials CREDENTIALS = TestProperties.googleCredentials();

  private BigQueryResourceManager bigQueryResourceManager;

  @Before
  public void setup() {
    // Your Google Cloud Project ID
    String projectId = TestProperties.exportProject();
    bigQueryResourceManager =
        BigQueryResourceManager.builder("Load-test-analyzer", projectId, CREDENTIALS)
            .setDatasetId(TestProperties.exportDataset())
            .build();
  }

  @After
  public void cleanup() {
  }

  @Test
  public void test() throws IOException {

    List<ObservabilityConfig> observabilityConfigs = ObservabilityConfig.parseConfig();
    for (ObservabilityConfig config: observabilityConfigs) {
      // execute Query
      List<PerfResultRow> results = runQuery(config);

      // get FirstRow and other rows
      PerfResultRow latestRow = results.get(0);
      results.remove(0);

      List<Violation> violations = new ArrayList<>();
      // call RegressionRules and collect violations
      for (RegressionRule rule: config.getRegressionRules()) {
        violations.addAll(rule.getViolations(latestRow, results));
      }

      // create github issue
      if (violations.size() > 0) {
        // create github issue
      }
    }
  }

  public List<PerfResultRow> runQuery(ObservabilityConfig config) {
    String query = String.format("SELECT `timestamp`, template_name, test_name, metrics "
        + "FROM `cloud-teleport-testing.performance_tests.template_performance_metrics` "
        + "WHERE template_name = \"%s\" and test_name = \"%s\" order by `timestamp` desc LIMIT %s",
        config.getTemplateName(), config.getTestName(), String.valueOf(config.getNumPreviousRunsToObserve()));

    TableResult result = bigQueryResourceManager.runQuery(query);
    List<PerfResultRow> perfResultRows = new ArrayList<>();
    for (FieldValueList row : result.iterateAll()) {
      String templateName = row.get("templateName").getStringValue();
      String testName = row.get("testName").getStringValue();
      String timestamp = row.get("timestamp").getStringValue();
      Map<String, Double> metrics = new HashMap<>();
      for (FieldValue structField: row.get("metrics").getRepeatedValue()) {
        String metricName = structField.getRecordValue().get("name").getStringValue();
        Double metricValue = structField.getRecordValue().get("value").getDoubleValue();
        metrics.put(metricName, metricValue);
      }
      perfResultRows.add(new PerfResultRow(
          templateName, testName, timestamp, metrics
      ));
    }
    return perfResultRows;
  }
}
