package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.Mod;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.ModColumnType;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.TrackedSpannerColumn;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.TrackedSpannerTable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ValueCaptureType;
import org.json.JSONObject;

/**
 * The {@link SchemaUpdateUtils} provides methods that detects schema updates and updates stored
 * schema information.
 */
public class SchemaUpdateUtils {

  // Detect if there's a table/column difference between the mod and the stored map.
  public static boolean detectDiffColumnInMod(
      Mod mod, Map<String, TrackedSpannerTable> spannerTableByName) {
    TrackedSpannerTable spannerTable = spannerTableByName.get(mod.getTableName());
    Set<String> keySetOfNewValuesJsonObject =
        mod.getNewValuesJson() == ""
            ? new JSONObject("{}").keySet()
            : new JSONObject(mod.getNewValuesJson()).keySet();
    // At this mod's spannerCommitTimestamp, one column is added/dropped.
    if (spannerTable.getNonPkColumns().size() != keySetOfNewValuesJsonObject.size()) {
      return true;
    }
    Set<String> nonPkColumnsNamesSet = spannerTable.getNonPkColumnsNamesSet();
    // Returns true if the stored schema doesn't contain a column in the mod
    return !nonPkColumnsNamesSet.containsAll(keySetOfNewValuesJsonObject);
  }

  // Update the stored schema information (spannerTableByName) by fetching from the mod.
  public static void updateStoredSchemaNewRow(
      Mod mod, Map<String, TrackedSpannerTable> spannerTableByName, Dialect dialect) {
    JSONObject keysJsonObject = new JSONObject(mod.getKeysJson());
    JSONObject newValuesJsonObject =
        mod.getNewValuesJson() == ""
            ? new JSONObject("{}")
            : new JSONObject(mod.getNewValuesJson());
    String spannerTableName = mod.getTableName();
    Map<String, ModColumnType> modColumnTypeMap = mod.getRowTypeAsMap();
    // Create a new table for spannerTableName if it's not in spannerTableByName.
    if (!spannerTableByName.containsKey(spannerTableName)) {
      // Create an empty list of pk columns for the new table.
      List<TrackedSpannerColumn> pkColumns = new ArrayList<>();
      // Create an empty list of non-pk columns for the new table.
      List<TrackedSpannerColumn> nonPkColumns = new ArrayList<>();
      // Introduce the new table into spannerTableByName.
      TrackedSpannerTable spannerTableObj =
          new TrackedSpannerTable(spannerTableName, pkColumns, nonPkColumns);
      spannerTableByName.put(spannerTableName, spannerTableObj);
      // Populate pk columns from Mod to TrackedSpannerTable.
      for (String pkColumnName : keysJsonObject.keySet()) {
        ModColumnType spannerColumn = modColumnTypeMap.get(pkColumnName);
        String typeStr =
            TypesUtils.extractTypeFromTypeCode(new JSONObject(spannerColumn.getType().getCode()));
        spannerTableByName
            .get(spannerTableName)
            .addTrackedSpannerColumn(
                pkColumnName, typeStr, -1, (int) spannerColumn.getOrdinalPosition(), dialect);
      }
    }

    // Populate nonPkColumns from Mod to TrackedSpannerTable.
    Set<String> nonPkColumnsSet =
        spannerTableByName.get(spannerTableName).getNonPkColumnsNamesSet();
    for (String nonPkColumnName : newValuesJsonObject.keySet()) {
      if (!nonPkColumnsSet.contains(nonPkColumnName)) {
        ModColumnType spannerColumn = modColumnTypeMap.get(nonPkColumnName);
        String typeStr =
            TypesUtils.extractTypeFromTypeCode(new JSONObject(spannerColumn.getType().getCode()));
        spannerTableByName
            .get(spannerTableName)
            .addTrackedSpannerColumn(
                spannerColumn.getName(),
                typeStr,
                (int) spannerColumn.getOrdinalPosition(),
                -1,
                dialect);
      }
    }
  }

  // For NEW_VALUES and OLD_AND_NEW_VALUES, update the stored schema information by looking up
  // INFORMATION_SCHEMA at the mod's commit timestamp.
  // For NEW_ROW, update the stored schema information by fetching from the mod.
  public static Map<String, TrackedSpannerTable> updateStoredSchemaIfNeeded(
      SpannerAccessor spannerAccessor,
      String spannerChangeStream,
      Dialect dialect,
      Mod mod,
      Map<String, TrackedSpannerTable> spannerTableByName) {
    if (!spannerTableByName.containsKey(mod.getTableName())
        || SchemaUpdateUtils.detectDiffColumnInMod(mod, spannerTableByName)) {
      if (mod.getValueCaptureType() != ValueCaptureType.NEW_ROW) {
        com.google.cloud.Timestamp spannerCommitTimestamp =
            com.google.cloud.Timestamp.ofTimeSecondsAndNanos(
                mod.getCommitTimestampSeconds(), mod.getCommitTimestampNanos());
        // TODO:b/322630434 Consider updating the schema only for one table at a time.
        spannerTableByName =
            new SpannerChangeStreamsUtils(
                    spannerAccessor.getDatabaseClient(),
                    spannerChangeStream,
                    dialect,
                    spannerCommitTimestamp)
                .getSpannerTableByName();
      } else {
        updateStoredSchemaNewRow(mod, spannerTableByName, dialect);
      }
    }
    return spannerTableByName;
  }
}
