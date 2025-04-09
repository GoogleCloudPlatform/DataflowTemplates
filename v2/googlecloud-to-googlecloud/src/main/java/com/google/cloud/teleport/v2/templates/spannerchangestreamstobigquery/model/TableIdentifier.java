package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model;

import com.google.cloud.spanner.Dialect;

public class TableIdentifier {
  private String tableName;
  private String tableSchema;

  public TableIdentifier(String tableName, String tableSchema) {
    this.tableName = tableName;
    this.tableSchema = tableSchema;
  }

  public String getTableName() {
    return tableName;
  }

  public String getTableSchema() {
    return tableSchema;
  }

  @Override
  public String toString() {
    return "TableIdentifier{" + "tableName='" + tableName + '\'' + ", tableSchema='" + tableSchema + '\'' + '}';
  } 

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TableIdentifier that = (TableIdentifier) o;
    return tableName.equals(that.tableName) && tableSchema.equals(that.tableSchema);
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(tableName, tableSchema);
  }

  /**
   * Creates a TableIdentifier object from a qualified name. Table names from Spanner change
   * streams are fully qualified with the format `${schema}.${table}`. For the default schema
   * there is no schema name included.
   * 
   * @param qualifiedName
   * @return
   */
  public static TableIdentifier create(String qualifiedName, Dialect dialect) {
    if (qualifiedName.indexOf('.') > 0) {
      String[] parts = qualifiedName.split("\\.");
      String tableName = parts[1];
      String tableSchema = parts[0];
      return new TableIdentifier(tableName, tableSchema);
    }
    if (dialect == Dialect.POSTGRESQL) {
      return new TableIdentifier(qualifiedName, "public");
    }
    return new TableIdentifier(qualifiedName, null);
  }
}
