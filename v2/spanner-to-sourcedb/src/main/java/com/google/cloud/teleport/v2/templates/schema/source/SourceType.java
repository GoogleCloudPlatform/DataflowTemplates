package com.google.cloud.teleport.v2.templates.schema.source;

import java.io.Serializable;

public class SourceType implements Serializable {

  /** represents the data type of the source column. */
  private final String sourceType;
  /** represents is column is primary key. */
  private final Boolean isPrimaryKey;

  public SourceType(String type, Boolean isPrimaryKey) {
    this.sourceType = type;
    this.isPrimaryKey = isPrimaryKey;
  }

  public String getSourceType() {
    return sourceType;
  }

  public Boolean getIsPrimaryKey() {
    return isPrimaryKey;
  }
}
