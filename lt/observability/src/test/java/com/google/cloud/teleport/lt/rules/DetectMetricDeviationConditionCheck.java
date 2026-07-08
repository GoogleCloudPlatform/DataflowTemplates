package com.google.cloud.teleport.lt.rules;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.lt.dataset.bigquery.BigQueryPerfDataset;
import com.google.cloud.teleport.lt.dataset.bigquery.PerfResultRow;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.conditions.ConditionCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The DetectMetricDeviationRule class provides a ConditionCheck which checks if a Metric in the
 * observed record in a given Dataset is deviating more than the given % from the average of
 * previous records.
 */
@AutoValue
public abstract class DetectMetricDeviationConditionCheck extends ConditionCheck {

  private static final Logger LOG =
      LoggerFactory.getLogger(DetectMetricDeviationConditionCheck.class);

  abstract BigQueryPerfDataset dataset();

  abstract String metricName();

  abstract double percentageDeviation();

  abstract String testCaseDescription();

  @Override
  public String getDescription() {
    return String.format("DetectMetricDeviation for %s", metricName());
  }

  @Override
  protected CheckResult check() {
    double totalMetricValue = 0.0;
    PerfResultRow latestRow = null;
    try {
      for (PerfResultRow perfResultRow : dataset().rows()) {
        if (latestRow == null) {
          latestRow = perfResultRow;
        } else {
          totalMetricValue += getMetricValue(perfResultRow, metricName());
        }
      }
      double average = totalMetricValue / (dataset().rows().size() - 1.0);

      // Deviating more than x% of average
      if ((getMetricValue(latestRow, metricName())
              > average * (100.0 + percentageDeviation()) / 100.0)
          || (getMetricValue(latestRow, metricName())
              < average * (100.0 - percentageDeviation()) / 100.0)) {
        return new ConditionCheck.CheckResult(
            false,
            String.format(
                "Metric %s is deviating more than %.2f %% than the average. %s=%f, Average=%f\n",
                metricName(),
                percentageDeviation(),
                metricName(),
                getMetricValue(latestRow, metricName()),
                average));
      }
    } catch (MetricNotFoundException e) {
      LOG.warn("Metric " + metricName() + " not found in Dataset! Ignoring the check Condition");
    }

    return new ConditionCheck.CheckResult(true);
  }

  public double getMetricValue(PerfResultRow resultRow, String metricName)
      throws MetricNotFoundException {
    if (resultRow.row.containsKey("metrics")) {
      List<Object> metrics = (List<Object>) resultRow.row.get("metrics");
      for (Object metric : metrics) {
        Map<String, Object> metricMap = (Map<String, Object>) metric;
        if (metricName.equals(metricMap.get("name"))) {
          return (double) metricMap.get("value");
        }
      }
    }
    throw new MetricNotFoundException("Metric " + metricName + " not found!");
  }

  public static Builder builder() {
    return new AutoValue_DetectMetricDeviationConditionCheck.Builder();
  }

  public static String constructQuery(String templateName, String testName, String numRows) {
    String query =
        "SELECT `timestamp`, template_name, test_name, metrics "
            + "FROM `%s.%s.%s` "
            + "WHERE template_name = \"%s\" and test_name = \"%s\" order by `timestamp` desc LIMIT %s";
    return String.format(
        query,
        TestProperties.exportProject(),
        TestProperties.exportDataset(),
        TestProperties.exportTable(),
        templateName,
        testName,
        numRows);
  }

  public void assertTrue() {
    if (!this.get()) {
      throw new AssertionError(
          String.format(
              "Metric %s is deviating more than %.2f %% than the average for %s\n",
              metricName(), percentageDeviation(), testCaseDescription()));
    }
  }

  public static class MetricNotFoundException extends Exception {
    public MetricNotFoundException() {}

    public MetricNotFoundException(String message) {
      super(message);
    }
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setDataset(BigQueryPerfDataset bigQueryPerfDataset);

    public abstract Builder setMetricName(String metricName);

    public abstract Builder setPercentageDeviation(double percentageDeviation);

    public abstract Builder setTestCaseDescription(String description);

    abstract DetectMetricDeviationConditionCheck autoBuild();

    public DetectMetricDeviationConditionCheck build() {
      return autoBuild();
    }
  }
}
