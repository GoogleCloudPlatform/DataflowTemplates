package com.google.cloud.teleport.lt.dataset.bigquery;

import com.google.auto.value.AutoValue;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;

@AutoValue
public abstract class BigQueryPerfDatasetFetcher {
  abstract String templateName();

  abstract String testName();

  abstract int numRows();

  abstract BigQueryResourceManager bigQueryResourceManager();

  private static final String QUERY =
      "SELECT `timestamp`, template_name, test_name, metrics "
          + "FROM `%s.%s.%s` "
          + "WHERE template_name = \"%s\" and test_name = \"%s\" order by `timestamp` desc LIMIT %s";

  public static BigQueryPerfDatasetFetcher.Builder builder() {
    return new AutoValue_BigQueryPerfDatasetFetcher.Builder();
  }

  public BigQueryPerfDataset fetch() {
    String query =
        String.format(
            QUERY,
            TestProperties.exportProject(),
            TestProperties.exportDataset(),
            TestProperties.exportTable(),
            templateName(),
            testName(),
            String.valueOf(numRows()));

    TableResult result = bigQueryResourceManager().runQuery(query);
    List<PerfResultRow> perfResultRows = new ArrayList<>();
    for (FieldValueList row : result.iterateAll()) {
      String timestamp = row.get(0).getStringValue();
      String templateName = row.get(1).getStringValue();
      String testName = row.get(2).getStringValue();
      Map<String, Double> metrics = new HashMap<>();
      for (FieldValue structField : row.get(3).getRepeatedValue()) {
        String metricName = structField.getRecordValue().get(0).getStringValue();
        Double metricValue = structField.getRecordValue().get(1).getDoubleValue();
        metrics.put(metricName, metricValue);
      }
      perfResultRows.add(new PerfResultRow(templateName, testName, timestamp, metrics));
    }
    PerfResultRow latestRow = null;
    if (perfResultRows.size() > 0) {
      latestRow = perfResultRows.get(0);
      perfResultRows.remove(0);
    }
    return BigQueryPerfDataset.builder()
        .setLatestRow(latestRow)
        .setPreviousRows(perfResultRows)
        .build();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract BigQueryPerfDatasetFetcher.Builder setTemplateName(String templateName);

    public abstract BigQueryPerfDatasetFetcher.Builder setTestName(String testName);

    public abstract BigQueryPerfDatasetFetcher.Builder setNumRows(int numRows);

    public abstract BigQueryPerfDatasetFetcher.Builder setBigQueryResourceManager(
        BigQueryResourceManager bigQueryResourceManager);

    abstract BigQueryPerfDatasetFetcher autoBuild();

    public BigQueryPerfDatasetFetcher build() {
      return autoBuild();
    }
  }
}
