package com.google.cloud.teleport.lt.rules;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.lt.dataset.bigquery.BigQueryPerfDataset;
import com.google.cloud.teleport.lt.dataset.bigquery.PerfResultRow;
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

  @Override
  public String getDescription() {
    return String.format("DetectMetricDeviation ConditionCheck for %s", metricName());
  }

  @Override
  protected CheckResult check() {
    double totalMetricValue = 0.0;
    for (PerfResultRow perfResultRow : dataset().previousRows()) {
      totalMetricValue += perfResultRow.metrics.get(metricName());
    }
    double average = totalMetricValue / dataset().previousRows().size();

    // Deviating more than x% of average
    if ((dataset().latestRow().metrics.get(metricName())
            > average * (100.0 + percentageDeviation()) / 100.0)
        || (dataset().latestRow().metrics.get(metricName())
            < average * (100.0 - percentageDeviation()) / 100.0)) {
      return new ConditionCheck.CheckResult(
          false,
          String.format(
              "Metric %s is deviating more than %.2f %% than the average. %s=%f, Average=%f\n",
              metricName(),
              percentageDeviation(),
              metricName(),
              dataset().latestRow().metrics.get(metricName()),
              average));
    }

    return new ConditionCheck.CheckResult(true);
  }

  public static Builder builder() {
    return new AutoValue_DetectMetricDeviationConditionCheck.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setDataset(BigQueryPerfDataset bigQueryPerfDataset);

    public abstract Builder setMetricName(String metricName);

    public abstract Builder setPercentageDeviation(double percentageDeviation);

    abstract DetectMetricDeviationConditionCheck autoBuild();

    public DetectMetricDeviationConditionCheck build() {
      return autoBuild();
    }
  }
}
