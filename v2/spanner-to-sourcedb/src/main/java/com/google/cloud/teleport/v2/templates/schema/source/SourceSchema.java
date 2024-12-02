package com.google.cloud.teleport.v2.templates.schema.source;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class SourceSchema implements Serializable {

  private final Map<String, SourceTable> srcSchema;

  public SourceSchema() {
    this.srcSchema = new HashMap<String, SourceTable>();
  }

  public SourceSchema(Map<String, SourceTable> srcSchema) {
    this.srcSchema = srcSchema;
  }

  public Map<String, SourceTable> getSrcSchema() {
    return srcSchema;
  }
}
