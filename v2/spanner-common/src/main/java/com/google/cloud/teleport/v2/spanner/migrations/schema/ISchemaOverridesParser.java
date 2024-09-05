package com.google.cloud.teleport.v2.spanner.migrations.schema;

import org.apache.commons.lang3.tuple.Pair;

public interface ISchemaOverridesParser {

  /**
   * Gets the spanner table name given the source table name, or source table name if no override is
   * configured.
   *
   * @param sourceTableName The source table name
   * @return The overridden spanner table name
   */
  String getTableOverrideOrDefault(String sourceTableName);

  /**
   * Gets the spanner column name given the source table name, or the source column name if override
   * is configured.
   *
   * @param sourceTableName the source table name for which column name is overridden
   * @param sourceColumnName the source column name being overridden
   * @return A pair of spannerTableName and spannerColumnName
   */
  Pair<String, String> getColumnOverrideOrDefault(String sourceTableName,
      String sourceColumnName);

}
