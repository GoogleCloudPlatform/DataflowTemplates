package com.google.cloud.teleport.lt.dataset.bigquery;

import java.util.Map;

public class PerfResultRow {
  public String templateName;
  public String testName;
  public String timestamp;
  public Map<String, Double> metrics;

  public PerfResultRow(
      String templateName, String testName, String timestamp, Map<String, Double> metrics) {
    this.templateName = templateName;
    this.testName = testName;
    this.timestamp = timestamp;
    this.metrics = metrics;
  }
}
