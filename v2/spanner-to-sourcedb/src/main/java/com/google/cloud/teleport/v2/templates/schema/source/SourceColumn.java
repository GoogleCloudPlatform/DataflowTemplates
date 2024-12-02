package com.google.cloud.teleport.v2.templates.schema.source;

import com.google.cloud.teleport.v2.spanner.migrations.schema.SourceColumnType;

import java.io.Serializable;

public class SourceColumn implements Serializable {
  /** represents name of a column in a source table */
  private final String name;
  /** represents the data type of the source column. */
  private final SourceType type;

  public SourceColumn(String name, SourceType type) {
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public SourceType getType() {
    return type;
  }

  public String toString() {
    return String.format("{ 'name': '%s' , 'type': '%s'}", name, type);
  }

}
