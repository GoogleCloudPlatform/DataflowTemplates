package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model;

public class ColumnIdentifier {
  
  private String tableName;
  private String tableSchema;
  private Integer ordinalPosition;

  public ColumnIdentifier(String tableName, String tableSchema, Integer originalPosition) {
    this.tableName = tableName;
    this.tableSchema = tableSchema;
    this.ordinalPosition = originalPosition;
  }

  public Integer getOrdinalPosition() {
    return ordinalPosition;
  }

  public String getTableName() {
    return tableName;
  }

  public String getTableSchema() {
    return tableSchema;
  }
}
