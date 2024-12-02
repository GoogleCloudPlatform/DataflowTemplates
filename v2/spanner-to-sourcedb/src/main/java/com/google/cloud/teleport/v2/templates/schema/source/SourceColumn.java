package com.google.cloud.teleport.v2.templates.schema.source;

import java.io.Serializable;

public class SourceColumn implements Serializable {
  private String sourceType;
  private boolean isPrimaryKey;

  public SourceColumn(String sourceType, boolean isPrimaryKey) {
    this.sourceType = sourceType;
    this.isPrimaryKey = isPrimaryKey;
  }

  public String getSourceType() {
    return sourceType;
  }

  public void setSourceType(String sourceType) {
    this.sourceType = sourceType;
  }

  public boolean isPrimaryKey() {
    return isPrimaryKey;
  }

  public void setPrimaryKey(boolean primaryKey) {
    isPrimaryKey = primaryKey;
  }
}