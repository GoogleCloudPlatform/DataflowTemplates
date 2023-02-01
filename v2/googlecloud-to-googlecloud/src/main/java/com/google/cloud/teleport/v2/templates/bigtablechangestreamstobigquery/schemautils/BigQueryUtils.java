/*
 * Copyright (C) 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.schemautils;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.BigQueryDestination;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.BigtableSource;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.ChangelogColumn;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.Mod;
import com.google.cloud.teleport.v2.templates.bigtablechangestreamstobigquery.model.TransientColumn;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.sdk.io.gcp.bigquery.DynamicDestinations;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.json.JSONObject;

/**
 * {@link BigQueryUtils} provides utils for processing BigQuery schema and generating BigQuery rows.
 */
public class BigQueryUtils implements Serializable {

  public static final String ANY_COLUMN_FAMILY = "*";

  private static final EnumMap<ChangelogColumn, BigQueryValueFormatter> FORMATTERS =
      new EnumMap<>(ChangelogColumn.class);

  static {
    FORMATTERS.put(
        ChangelogColumn.ROW_KEY_STRING,
        (bq, chg) -> {
          String rowkeyEncoded = chg.getString(ChangelogColumn.ROW_KEY_BYTES.name());
          return bq.convertBase64ToString(rowkeyEncoded);
        });
    FORMATTERS.put(
        ChangelogColumn.ROW_KEY_BYTES,
        (bq, chg) -> {
          String rowkeyEncoded = chg.getString(ChangelogColumn.ROW_KEY_BYTES.name());
          return bq.convertBase64ToBytes(rowkeyEncoded);
        });
    FORMATTERS.put(
        ChangelogColumn.MOD_TYPE, (bq, chg) -> chg.getString(ChangelogColumn.MOD_TYPE.name()));
    FORMATTERS.put(
        ChangelogColumn.COMMIT_TIMESTAMP,
        (bq, chg) -> chg.getString(ChangelogColumn.COMMIT_TIMESTAMP.name()));
    FORMATTERS.put(
        ChangelogColumn.COLUMN_FAMILY,
        (bq, chg) -> chg.getString(ChangelogColumn.COLUMN_FAMILY.name()));
    FORMATTERS.put(
        ChangelogColumn.COLUMN,
        (bq, chg) -> {
          if (!chg.has(ChangelogColumn.COLUMN.name())) {
            return null;
          }
          String qualifierEncoded = chg.getString(ChangelogColumn.COLUMN.name());
          return bq.convertBase64ToString(qualifierEncoded);
        });
    FORMATTERS.put(
        ChangelogColumn.TIMESTAMP,
        (bq, chg) -> {
          if (!chg.has(ChangelogColumn.TIMESTAMP.name())) {
            return null;
          }
          return chg.getString(ChangelogColumn.TIMESTAMP.name());
        });
    FORMATTERS.put(
        ChangelogColumn.TIMESTAMP_NUM,
        (bq, chg) -> {
          if (!chg.has(ChangelogColumn.TIMESTAMP_NUM.name())) {
            return null;
          }
          return chg.getString(ChangelogColumn.TIMESTAMP_NUM.name());
        });
    FORMATTERS.put(
        ChangelogColumn.VALUE_STRING,
        (bq, chg) -> {
          if (!chg.has(ChangelogColumn.VALUE_BYTES.name())) {
            return null;
          }

          String valueEncoded = chg.getString(ChangelogColumn.VALUE_BYTES.name());
          return bq.convertBase64ToString(valueEncoded);
        });
    FORMATTERS.put(
        ChangelogColumn.VALUE_BYTES,
        (bq, chg) -> {
          if (!chg.has(ChangelogColumn.VALUE_BYTES.name())) {
            return null;
          }

          String valueEncoded = chg.getString(ChangelogColumn.VALUE_BYTES.name());
          return bq.convertBase64ToBytes(valueEncoded);
        });
    FORMATTERS.put(
        ChangelogColumn.TIMESTAMP_FROM,
        (bq, chg) -> {
          if (!chg.has(ChangelogColumn.TIMESTAMP_FROM.name())) {
            return null;
          }
          return chg.getString(ChangelogColumn.TIMESTAMP_FROM.name());
        });
    FORMATTERS.put(
        ChangelogColumn.TIMESTAMP_FROM_NUM,
        (bq, chg) -> {
          if (!chg.has(ChangelogColumn.TIMESTAMP_FROM_NUM.name())) {
            return null;
          }
          return chg.getString(ChangelogColumn.TIMESTAMP_FROM_NUM.name());
        });
    FORMATTERS.put(
        ChangelogColumn.TIMESTAMP_TO,
        (bq, chg) -> {
          if (!chg.has(ChangelogColumn.TIMESTAMP_TO.name())) {
            return null;
          }
          return chg.getString(ChangelogColumn.TIMESTAMP_TO.name());
        });
    FORMATTERS.put(
        ChangelogColumn.TIMESTAMP_TO_NUM,
        (bq, chg) -> {
          if (!chg.has(ChangelogColumn.TIMESTAMP_TO_NUM.name())) {
            return null;
          }
          return chg.getString(ChangelogColumn.TIMESTAMP_TO_NUM.name());
        });
    FORMATTERS.put(
        ChangelogColumn.IS_GC,
        (bq, chg) -> Boolean.toString(chg.getBoolean(ChangelogColumn.IS_GC.name())));
    FORMATTERS.put(
        ChangelogColumn.SOURCE_INSTANCE,
        (bq, chg) -> chg.getString(ChangelogColumn.SOURCE_INSTANCE.name()));
    FORMATTERS.put(
        ChangelogColumn.SOURCE_CLUSTER,
        (bq, chg) -> chg.getString(ChangelogColumn.SOURCE_CLUSTER.name()));
    FORMATTERS.put(
        ChangelogColumn.SOURCE_TABLE,
        (bq, chg) -> chg.getString(ChangelogColumn.SOURCE_TABLE.name()));
    FORMATTERS.put(
        ChangelogColumn.TIEBREAKER,
        (bq, chg) -> Long.toString(chg.getLong(ChangelogColumn.TIEBREAKER.name())));
    FORMATTERS.put(ChangelogColumn.BQ_COMMIT_TIMESTAMP, (bq, chg) -> "AUTO");

    // Just in case, validate that every column in the enum has a formatter
    for (ChangelogColumn column : ChangelogColumn.values()) {
      Validate.notNull(FORMATTERS.get(column));
    }
  }

  private final BigtableSource source;
  private final BigQueryDestination destination;
  private final List<ChangelogColumn> configuredChangelogColumns;
  private final Map<String, Set<String>> ignoredColumnsMap;

  private transient Charset charsetObj;

  public BigQueryUtils(BigtableSource sourceInfo, BigQueryDestination destinationInfo) {
    this.source = sourceInfo;
    this.destination = destinationInfo;
    this.charsetObj = Charset.forName(sourceInfo.getCharset());
    this.configuredChangelogColumns = new ArrayList<>();
    for (ChangelogColumn column : ChangelogColumn.values()) {
      if (destinationInfo.isColumnEnabled(column)) {
        this.configuredChangelogColumns.add(column);
      }
    }

    ignoredColumnsMap = new HashMap<>();
    for (String columnFamilyAndColumn : sourceInfo.getColumnsToIgnore()) {
      int indexOfColon = columnFamilyAndColumn.indexOf(':');
      String columnFamily = ANY_COLUMN_FAMILY;
      String columnName = columnFamilyAndColumn;
      if (indexOfColon > 0) {
        columnFamily = columnFamilyAndColumn.substring(0, indexOfColon);
        if (StringUtils.isBlank(columnFamily)) {
          columnFamily = ANY_COLUMN_FAMILY;
        }
        columnName = columnFamilyAndColumn.substring(indexOfColon + 1);
      }

      Set<String> appliedToColumnFamilies =
          ignoredColumnsMap.computeIfAbsent(columnName, k -> new HashSet<>());
      appliedToColumnFamilies.add(columnFamily);
    }
  }

  public boolean hasIgnoredColumnFamilies() {
    return this.source.getColumnFamiliesToIgnore().size() > 0;
  }

  public boolean isIgnoredColumnFamily(String columnFamily) {
    return this.source.getColumnFamiliesToIgnore().contains(columnFamily);
  }

  public boolean hasIgnoredColumns() {
    return this.source.getColumnsToIgnore().size() > 0;
  }

  public boolean isIgnoredColumn(String columnFamily, String column) {
    Set<String> columnFamilies = ignoredColumnsMap.get(column);
    if (columnFamilies == null) {
      return false;
    }
    return columnFamilies.contains(columnFamily) || columnFamilies.contains(ANY_COLUMN_FAMILY);
  }

  /** @return true if modification should be written to BigQuery, false otherwise */
  public boolean setTableRowFields(Mod mod, String modJsonString, TableRow tableRow)
      throws Exception {
    // Metadata columns, not written to BQ
    tableRow.set(
        TransientColumn.BQ_CHANGELOG_FIELD_NAME_ORIGINAL_PAYLOAD_JSON.getColumnName(),
        modJsonString);

    JSONObject changeJsonParsed = new JSONObject(mod.getChangeJson());

    String columnFamily = null;
    if (hasIgnoredColumnFamilies() && changeJsonParsed.has(ChangelogColumn.COLUMN_FAMILY.name())) {
      columnFamily = Objects.toString(changeJsonParsed.get(ChangelogColumn.COLUMN_FAMILY.name()));
      if (isIgnoredColumnFamily(columnFamily)) {
        return false;
      }
    }

    if (hasIgnoredColumns() && changeJsonParsed.has(ChangelogColumn.COLUMN.name())) {
      String columnEncoded = Objects.toString(changeJsonParsed.get(ChangelogColumn.COLUMN.name()));
      if (!StringUtils.isBlank(columnEncoded)) {
        String column = convertBase64ToString(columnEncoded);
        if (isIgnoredColumn(columnFamily, column)) {
          return false;
        }
      }
    }

    for (ChangelogColumn column : configuredChangelogColumns) {
      BigQueryValueFormatter formatter = FORMATTERS.get(column);
      // .format might throw RuntimeException
      Object value = formatter.format(this, changeJsonParsed);

      if (value == null && column.isRequired()) {
        throw new IllegalArgumentException(
            "Cannot find value for column " + column.getBqColumnName());
      }
      tableRow.set(column.getBqColumnName(), value);
    }

    return true;
  }

  public BigQueryDynamicDestinations getDynamicDestinations() {
    return new BigQueryDynamicDestinations();
  }

  private TableSchema getDestinationTableSchema() {
    return new TableSchema().setFields(getDestinationTableFields());
  }

  private List<TableFieldSchema> getDestinationTableFields() {
    List<TableFieldSchema> fields = new ArrayList<>();

    for (ChangelogColumn column : configuredChangelogColumns) {
      fields.add(
          new TableFieldSchema()
              .setName(column.getBqColumnName())
              .setType(column.getBqType())
              .setMode(
                  (column.isRequired() ? Field.Mode.REQUIRED.name() : Field.Mode.NULLABLE.name())));
    }

    return fields;
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    charsetObj = Charset.forName(source.getCharset());
  }

  private String convertBase64ToString(String base64String) {
    return new String(Base64.getDecoder().decode(base64String), charsetObj);
  }

  private byte[] convertBase64ToBytes(String base64String) {
    return Base64.getDecoder().decode(base64String);
  }

  /**
   * The {@link BigQueryDynamicDestinations} loads into BigQuery tables in a dynamic fashion. The
   * destination table is always the same, but we'll be using DynamicDestinations to enable
   * partitioning / clustering and other BigQuery table features if necessary which otherwise are
   * not present in TableSchema
   */
  public final class BigQueryDynamicDestinations
      extends DynamicDestinations<TableRow, KV<TableId, TableRow>> {

    private BigQueryDynamicDestinations() {}

    @Override
    public KV<TableId, TableRow> getDestination(ValueInSingleWindow<TableRow> element) {
      TableRow tableRow = element.getValue();
      return KV.of(BigQueryUtils.this.destination.getBigQueryTableId(), tableRow);
    }

    @Override
    public TableDestination getTable(KV<TableId, TableRow> destination) {
      TableId tableId = BigQueryUtils.this.destination.getBigQueryTableId();
      String tableName =
          String.format("%s:%s.%s", tableId.getProject(), tableId.getDataset(), tableId.getTable());

      TimePartitioning timePartitioning = null;
      if (BigQueryUtils.this.destination.isPartitioned()) {
        timePartitioning = new TimePartitioning();
        timePartitioning.setType(
            BigQueryUtils.this.destination.getBigQueryChangelogTablePartitionType());
        timePartitioning.setExpirationMs(
            BigQueryUtils.this.destination.getBigQueryChangelogTablePartitionExpirationMs());
        timePartitioning.setField(BigQueryUtils.this.destination.getPartitionByColumnName());
      }

      return new TableDestination(tableName, "BigQuery changelog table.", timePartitioning);
    }

    @Override
    public TableSchema getSchema(KV<TableId, TableRow> destination) {
      return BigQueryUtils.this.getDestinationTableSchema();
    }
  }
}
