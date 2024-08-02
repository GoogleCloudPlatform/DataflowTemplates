package com.google.cloud.teleport.lt.rules;

import com.google.cloud.teleport.lt.PerfResultRow;
import com.google.cloud.teleport.lt.Violation;
import java.util.ArrayList;
import java.util.List;

public class TotalRunTimeAnomaly implements RegressionRule {

  private static TotalRunTimeAnomaly instance = new TotalRunTimeAnomaly();

  public static TotalRunTimeAnomaly getInstance() {
    return instance;
  }

  /**
   * Creates a violation if the total time taken by the row is deviating more than 25% from the
   * average of previous rows.
   *
   * @param row
   * @param previousRows
   * @return
   */
  public List<Violation> getViolations(PerfResultRow row, List<PerfResultRow> previousRows) {
    double totalTime = 0.0;
    List<Violation> violations = new ArrayList<>();
    for (PerfResultRow perfResultRow : previousRows) {
      totalTime += perfResultRow.metrics.get("RunTime");
    }
    double average = totalTime / previousRows.size();

    // Greater than 125% of average
    if (row.metrics.get("RunTime") > average * 1.25) {
      violations.add(
          new Violation(
              String.format(
                  "Total run time is greater than 125% than the average. RunTime=%d, AverageRunTime=%d\n",
                  row.metrics.get("RunTime"), average)));
    }
    // Less than 75% of average
    if (row.metrics.get("RunTime") < average * 0.75) {
      violations.add(
          new Violation(
              String.format(
                  "Total run time is less than 75% than the average. RunTime=%d, AverageRunTime=%d\n",
                  row.metrics.get("RunTime"), average)));
    }
    return violations;
  }
}
