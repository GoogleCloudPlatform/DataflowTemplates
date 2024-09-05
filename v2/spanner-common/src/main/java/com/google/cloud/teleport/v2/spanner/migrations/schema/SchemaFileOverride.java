package com.google.cloud.teleport.v2.spanner.migrations.schema;

import java.io.Serializable;
import java.util.Map;

public class SchemaFileOverride implements Serializable {

  private final Map<String, String> renamedTables;

  private final Map<String, Map<String, String>> renamedColumns;

  public SchemaFileOverride(Map<String, String> renamedTables, Map<String, Map<String, String>> renamedColumns) {
    this.renamedTables = renamedTables;
    this.renamedColumns = renamedColumns;
  }

  public Map<String, Map<String, String>>  getRenamedColumns() {
    return renamedColumns;
  }

  public Map<String, String> getRenamedTables() {
    return renamedTables;
  }

  @Override
  public String toString() {
    return "SchemaFileOverride{" +
        "renamedTables=" + renamedTables +
        ", renamedColumns=" + renamedColumns +
        '}';
  }
}
