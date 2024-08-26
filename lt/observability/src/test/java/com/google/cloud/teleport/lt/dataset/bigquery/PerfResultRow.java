package com.google.cloud.teleport.lt.dataset.bigquery;

import java.util.Map;

public class PerfResultRow {
  public Map<String, Object> row;

  public PerfResultRow(Map<String, Object> row) {
    this.row = row;
  }
}
