package com.google.cloud.teleport.v2.templates.schema.source;

import java.io.Serializable;

public class SourceTable implements Serializable {

  private String name;
  private SourceColumn colDefs;

  public SourceTable(String name,  SourceColumn colDefs) {
    this.name = name;
    this.colDefs = colDefs;
  }

  public String getName() {
    return name;
  }

  public SourceColumn getColDefs() {
    return colDefs;
  }
}
