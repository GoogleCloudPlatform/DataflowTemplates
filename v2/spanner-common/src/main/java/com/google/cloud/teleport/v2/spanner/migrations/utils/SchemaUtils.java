package com.google.cloud.teleport.v2.spanner.migrations.utils;

import com.google.cloud.teleport.v2.spanner.migrations.exceptions.DroppedTableException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;

public class SchemaUtils {
  public static void verifyTableInSession(Schema schema, String tableName)
      throws IllegalArgumentException, DroppedTableException {
    if (!schema.getSrcToID().containsKey(tableName)) {
      throw new IllegalArgumentException(
          "Missing entry for " + tableName + " in srcToId map, provide a valid session file.");
    }
    if (!schema.getToSpanner().containsKey(tableName)) {
      throw new DroppedTableException(
          "Cannot find entry for "
              + tableName
              + " in toSpanner map, it is likely this table was dropped");
    }
    String tableId = schema.getSrcToID().get(tableName).getName();
    if (!schema.getSpSchema().containsKey(tableId)) {
      throw new IllegalArgumentException(
          "Missing entry for " + tableId + " in spSchema, provide a valid session file.");
    }
  }
}
