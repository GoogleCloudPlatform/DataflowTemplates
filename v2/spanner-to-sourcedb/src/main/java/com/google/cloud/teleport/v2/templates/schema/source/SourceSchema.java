package com.google.cloud.teleport.v2.templates.schema.source;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class SourceSchema implements Serializable {

  private final Map<String, Map<String, SourceColumn>> srcSchema;

  public SourceSchema() {
    this.srcSchema = new HashMap<>();
  }

  public SourceSchema(Map<String, Map<String, SourceColumn>> srcSchema) {
    this.srcSchema = srcSchema;
  }

  public Map<String, Map<String, SourceColumn>> getSrcSchema() {
    return srcSchema;
  }
}