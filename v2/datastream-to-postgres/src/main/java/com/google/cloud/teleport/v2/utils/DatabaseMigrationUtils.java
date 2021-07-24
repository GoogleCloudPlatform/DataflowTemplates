/*
 * Copyright (C) 2021 Google LLC
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
package com.google.cloud.teleport.v2.utils;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.teleport.v2.io.CdcJdbcIO.DataSourceConfiguration;
import com.google.cloud.teleport.v2.values.DmlInfo;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A set of Database Migration utilities to convert JSON data to DML. */
public class DatabaseMigrationUtils implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(DatabaseMigrationUtils.class);

  private static Set<String> ignoreFields =
      new HashSet<String>(
          Arrays.asList(
              "_metadata_stream",
              "_metadata_timestamp",
              "_metadata_read_timestamp",
              "_metadata_deleted",
              "_metadata_schema",
              "_metadata_table",
              "_metadata_change_type",
              "_metadata_scn",
              "_metadata_ssn",
              "_metadata_rs_id",
              "_metadata_tx_id",
              "_metadata_source",
              "_metadata_row_id",
              "_metadata_read_method",
              "_metadata_source_type"));
  private static String rowIdColumnName = "rowid";
  private static List<String> defaultPrimaryKeys;
  private DataSourceConfiguration dataSourceConfiguration;
  private DataSource dataSource;
  private static MappedObjectCache<List<String>, Map<String, String>> tableCache;
  private static MappedObjectCache<List<String>, List<String>> primaryKeyCache;

  private DatabaseMigrationUtils(DataSourceConfiguration config) {
    checkArgument(config != null, "DataSourceConfiguration can not be null");

    this.dataSourceConfiguration = config;
  }

  public static DatabaseMigrationUtils of(DataSourceConfiguration config) {
    return new DatabaseMigrationUtils(config);
  }

  public DatabaseMigrationUtils withIgnoreFields(Set<String> fields) {
    this.ignoreFields = fields;
    return this;
  }

  public List<String> getDefaultPrimaryKeys() {
    if (this.defaultPrimaryKeys == null) {
      this.defaultPrimaryKeys = Arrays.asList(this.rowIdColumnName);
    }
    return this.defaultPrimaryKeys;
  }

  public DataSource getDataSource() {
    if (this.dataSource == null) {
      this.dataSource = this.dataSourceConfiguration.buildDatasource();
    }
    return this.dataSource;
  }

  private synchronized void setUpTableCache() {
    if (tableCache == null) {
      tableCache = new JdbcTableCache(this.getDataSource()).withCacheResetTimeUnitValue(1440);
    }
  }

  private synchronized void setUpPrimaryKeyCache() {
    if (primaryKeyCache == null) {
      primaryKeyCache =
          new JdbcPrimaryKeyCache(this.getDataSource()).withCacheResetTimeUnitValue(1440);
    }
  }

  public Map<String, String> getTableSchema(String schemaName, String tableName) {
    List<String> searchKey = ImmutableList.of(schemaName, tableName);

    if (this.tableCache == null) {
      setUpTableCache();
    }

    return this.tableCache.get(searchKey);
  }

  public List<String> getPrimaryKeys(String schemaName, String tableName, JsonNode rowObj) {
    List<String> searchKey = ImmutableList.of(schemaName, tableName);

    if (this.primaryKeyCache == null) {
      setUpPrimaryKeyCache();
    }

    List<String> primaryKeys = this.primaryKeyCache.get(searchKey);
    // If primary keys exist, but are not in the data
    // than we assume this is a rolled back delete.
    for (String primaryKey : primaryKeys) {
      if (!rowObj.has(primaryKey)) {
        return this.getDefaultPrimaryKeys();
      }
    }

    return primaryKeys;
  }

  public KV<String, DmlInfo> convertJsonToDmlInfo(FailsafeElement<String, String> element) {
    String jsonString = element.getPayload();

    ObjectMapper mapper = new ObjectMapper();
    JsonNode rowObj;

    try {
      rowObj = mapper.readTree(jsonString);
    } catch (IOException e) {
      LOG.error("IOException: {} :: {}", jsonString, e.toString());
      DmlInfo dmlInfo =
          DmlInfo.of(
              element.getOriginalPayload(),
              "",
              "",
              "",
              new ArrayList<String>(),
              new ArrayList<String>(),
              new ArrayList<String>(),
              new ArrayList<String>());

      // TODO(dhercher): how should we handle bad data?
      return KV.of(jsonString, dmlInfo);
    }

    try {
      // Oracle uses upper case while Postgres uses all lowercase.
      // We lowercase the values of these metadata fields to align with
      // our schema conversion rules.
      String schemaName = this.getPostgresSchemaName(rowObj);
      String tableName = this.getPostgresTableName(rowObj);

      Map<String, String> tableSchema = this.getTableSchema(schemaName, tableName);
      List<String> primaryKeys = this.getPrimaryKeys(schemaName, tableName, rowObj);
      List<String> orderByFields = Arrays.asList("_metadata_timestamp", "_metadata_scn");
      List<String> primaryKeyValues = getFieldValues(rowObj, primaryKeys);
      List<String> orderByValues = getFieldValues(rowObj, orderByFields);
      if (tableSchema.isEmpty()) {
        // If the table DNE we supply an empty SQL value (NOOP)
        DmlInfo dmlInfo =
            DmlInfo.of(
                element.getOriginalPayload(),
                "",
                schemaName,
                tableName,
                primaryKeys,
                orderByFields,
                primaryKeyValues,
                orderByValues);
        return KV.of(jsonString, dmlInfo);
      }

      String dmlSql;
      if (rowObj.get("_metadata_deleted").asBoolean()) {
        dmlSql = convertJsonToDeleteSql(rowObj, tableSchema, schemaName, tableName, primaryKeys);
      } else if (primaryKeys.size() == 0) {
        // TODO(dhercher): Do we choose to support this case?
        dmlSql = convertJsonToInsertSql(rowObj, tableSchema, schemaName, tableName);
      } else {
        dmlSql = convertJsonToUpsertSql(rowObj, tableSchema, schemaName, tableName, primaryKeys);
      }

      DmlInfo dmlInfo =
          DmlInfo.of(
              element.getOriginalPayload(),
              dmlSql,
              schemaName,
              tableName,
              primaryKeys,
              orderByFields,
              primaryKeyValues,
              orderByValues);

      return KV.of(dmlInfo.getStateWindowKey(), dmlInfo);
    } catch (Exception e) {
      LOG.error("Value Error: {} :: {}", rowObj.toString(), e.toString());
      DmlInfo dmlInfo =
          DmlInfo.of(
              element.getOriginalPayload(),
              "",
              "",
              "",
              new ArrayList<String>(),
              new ArrayList<String>(),
              new ArrayList<String>(),
              new ArrayList<String>());
      // TODO(dhercher): how should we handle bad data?
      return KV.of(jsonString, dmlInfo);
    }
  }

  public static String getPostgresSchemaName(JsonNode rowObj) {
    String oracleSchemaName = rowObj.get("_metadata_schema").getTextValue();
    return oracleSchemaName.toLowerCase();
  }

  public static String getPostgresTableName(JsonNode rowObj) {
    String oracleTableName = rowObj.get("_metadata_table").getTextValue();
    return oracleTableName.toLowerCase();
  }

  public static String getValueSql(
      JsonNode rowObj, String columnName, Map<String, String> tableSchema) {
    String columnValue;

    JsonNode columnObj = rowObj.get(columnName);
    if (columnObj == null) {
      LOG.warn("Missing Required Value: {} in {}", columnName, rowObj.toString());
      return "";
    }

    if (columnObj.isTextual()) {
      columnValue = "\'" + cleanSql(columnObj.getTextValue()) + "\'";
    } else {
      columnValue = columnObj.toString();
    }

    return columnValue;
  }

  private static String cleanSql(String str) {
    if (str == null) {
      return null;
    }
    String cleanedNullBytes = StringUtils.replace(str, "\u0000", "");

    return escapeSql(cleanedNullBytes);
  }

  private static String quoteColumn(String columnName) {
    return "\"" + columnName + "\"";
  }

  private static String escapeSql(String str) {
    return StringUtils.replace(str, "'", "''");
  }

  public List<String> getFieldValues(JsonNode rowObj, List<String> fieldNames) {
    List<String> fieldValues = new ArrayList<String>();

    for (String fieldName : fieldNames) {
      fieldValues.add(getValueSql(rowObj, fieldName, null));
    }

    return fieldValues;
  }

  public String getColumnsListSql(JsonNode rowObj) {
    String columnsListSql = "";

    for (Iterator<String> fieldNames = rowObj.getFieldNames(); fieldNames.hasNext(); ) {
      String columnName = fieldNames.next();
      if (ignoreFields.contains(columnName)) {
        continue;
      }

      // Add column name
      String quotedColumnName = quoteColumn(columnName);
      if (columnsListSql == "") {
        columnsListSql = quotedColumnName;
      } else {
        columnsListSql = columnsListSql + "," + quotedColumnName;
      }
    }

    return columnsListSql;
  }

  public String getColumnsValuesSql(JsonNode rowObj, Map<String, String> tableSchema) {
    String valuesInsertSql = "";

    for (Iterator<String> fieldNames = rowObj.getFieldNames(); fieldNames.hasNext(); ) {
      String columnName = fieldNames.next();
      if (ignoreFields.contains(columnName)) {
        continue;
      }

      String columnValue = getValueSql(rowObj, columnName, tableSchema);
      if (valuesInsertSql == "") {
        valuesInsertSql = columnValue;
      } else {
        valuesInsertSql = valuesInsertSql + "," + columnValue;
      }
    }

    return valuesInsertSql;
  }

  public String getColumnsUpdateSql(JsonNode rowObj, Map<String, String> tableSchema) {
    String onUpdateSql = "";
    for (Iterator<String> fieldNames = rowObj.getFieldNames(); fieldNames.hasNext(); ) {
      String columnName = fieldNames.next();
      if (ignoreFields.contains(columnName)) {
        continue;
      }

      String quotedColumnName = quoteColumn(columnName);
      String columnValue = getValueSql(rowObj, columnName, tableSchema);

      if (onUpdateSql == "") {
        onUpdateSql = quotedColumnName + "=" + columnValue;
      } else {
        onUpdateSql = onUpdateSql + "," + quotedColumnName + "=" + columnValue;
      }
    }

    return onUpdateSql;
  }

  public String getPrimaryKeyToValueFilterSql(
      JsonNode rowObj, List<String> primaryKeys, Map<String, String> tableSchema) {
    String pkToValueSql = "";

    for (String columnName : primaryKeys) {
      if (ignoreFields.contains(columnName)) {
        continue;
      }

      String columnValue = getValueSql(rowObj, columnName, tableSchema);
      if (pkToValueSql == "") {
        pkToValueSql = columnName + "=" + columnValue;
      } else {
        pkToValueSql = pkToValueSql + " AND " + columnName + "=" + columnValue;
      }
    }

    return pkToValueSql;
  }

  public String convertJsonToInsertSql(
      JsonNode rowObj, Map<String, String> tableSchema, String schemaName, String tableName) {
    String dmlInsertTemplate = "INSERT INTO " + "%s.%s (%s) VALUES (%s);";

    String columnsListSql = getColumnsListSql(rowObj);
    String valuesInsertSql = getColumnsValuesSql(rowObj, tableSchema);

    String insertStatement =
        String.format(dmlInsertTemplate, schemaName, tableName, columnsListSql, valuesInsertSql);

    return insertStatement;
  }

  public String convertJsonToUpsertSql(
      JsonNode rowObj,
      Map<String, String> tableSchema,
      String schemaName,
      String tableName,
      List<String> primaryKeys) {
    String dmlUpsertTemplate =
        "INSERT INTO " + "%s.%s (%s) VALUES (%s) " + "ON CONFLICT (%s) DO UPDATE " + "SET %s;";

    String columnsListSql = getColumnsListSql(rowObj);
    String valuesInsertSql = getColumnsValuesSql(rowObj, tableSchema);
    String onUpdateSql = getColumnsUpdateSql(rowObj, tableSchema);
    String primaryKeySql = String.join(",", primaryKeys);

    String upsertStatement =
        String.format(
            dmlUpsertTemplate,
            schemaName,
            tableName,
            columnsListSql,
            valuesInsertSql,
            primaryKeySql,
            onUpdateSql);

    return upsertStatement;
  }

  public String convertJsonToDeleteSql(
      JsonNode rowObj,
      Map<String, String> tableSchema,
      String schemaName,
      String tableName,
      List<String> primaryKeys) {
    // TODO(dhercher): How should we handle Deletes if PKs are empty?
    if (primaryKeys.size() == 0) {
      LOG.warn(
          "Primary Keys DNE for Delete: {}.{} --> {}", schemaName, tableName, rowObj.toString());
    }
    String dmlDeleteTemplate = "DELETE FROM %s.%s WHERE %s;";

    String pkFilterSql = getPrimaryKeyToValueFilterSql(rowObj, primaryKeys, tableSchema);
    String dmlDeleteStatement =
        String.format(dmlDeleteTemplate, schemaName, tableName, pkFilterSql);

    return dmlDeleteStatement;
  }

  private static Connection getConnection(
      DataSource dataSource, int retriesRemaining, int maxRetries) {
    Connection connection = null;
    try {
      connection = dataSource.getConnection();
    } catch (SQLException e) {
      if (retriesRemaining > 0) {
        int sleepSecs = (maxRetries - retriesRemaining + 1) * 10;
        LOG.info(
            "SQLException: Will retry after {} seconds: Connection Error: {}",
            sleepSecs,
            e.toString());
        try {
          Thread.sleep(sleepSecs * 1000);
          return getConnection(dataSource, retriesRemaining - 1, maxRetries);
        } catch (InterruptedException i) {
        }
      }
      LOG.error("SQLException: Connection Error: {}", e.toString());
    }

    return connection;
  }

  /**
   * The {@link JdbcTableCache} manages safely getting and setting JDBC Table objects from a local
   * cache for each worker thread.
   *
   * <p>The key factors addressed are ensuring expiration of cached tables, consistent update
   * behavior to ensure reliabillity, and easy cache reloads. Open Question: Does the class require
   * thread-safe behaviors? Currently it does not since there is no iteration and get/set are not
   * continuous.
   */
  public static class JdbcTableCache extends MappedObjectCache<List<String>, Map<String, String>> {

    private DataSource dataSource;
    private static final int MAX_RETRIES = 5;

    /**
     * Create an instance of a {@link JdbcTableCache} to track table schemas.
     *
     * @param dataSource A DataSource instance used to extract Table objects.
     */
    public JdbcTableCache(DataSource dataSource) {
      this.dataSource = dataSource;
    }

    private Map<String, String> getTableSchema(
        String schemaName, String tableName, int retriesRemaining) {
      Map<String, String> tableSchema = new HashMap<String, String>();

      try (Connection connection = getConnection(this.dataSource, MAX_RETRIES, MAX_RETRIES)) {
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet columns = metaData.getColumns(null, schemaName, tableName, null)) {
          while (columns.next()) {
            tableSchema.put(columns.getString("COLUMN_NAME"), columns.getString("TYPE_NAME"));
          }
        }
      } catch (SQLException e) {
        if (retriesRemaining > 0) {
          int sleepSecs = (MAX_RETRIES - retriesRemaining + 1) * 10;
          LOG.info(
              "SQLException Occurred, will retry after {} seconds: Failed to Retrieve Schema: {}.{} : {}",
              sleepSecs,
              schemaName,
              tableName,
              e.toString());
          try {
            Thread.sleep(sleepSecs * 1000);
            return getTableSchema(schemaName, tableName, retriesRemaining - 1);
          } catch (InterruptedException i) {
          }
        }
        LOG.error(
            "SQLException Occurred: Failed to Retrieve Schema: {}.{} : {}",
            schemaName,
            tableName,
            e.toString());
      }

      return tableSchema;
    }

    @Override
    public Map<String, String> getObjectValue(List<String> key) {
      String schemaName = key.get(0);
      String tableName = key.get(1);

      Map<String, String> tableSchema = getTableSchema(schemaName, tableName, MAX_RETRIES);

      return tableSchema;
    }
  }

  /**
   * The {@link JdbcPrimaryKeyCache} manages safely getting and setting JDBC Table PKs from a local
   * cache for each worker thread.
   *
   * <p>The key factors addressed are ensuring expiration of cached tables, consistent update
   * behavior to ensure reliabillity, and easy cache reloads. Open Question: Does the class require
   * thread-safe behaviors? Currently it does not since there is no iteration and get/set are not
   * continuous.
   */
  public static class JdbcPrimaryKeyCache extends MappedObjectCache<List<String>, List<String>> {

    private DataSource dataSource;
    private static final int MAX_RETRIES = 5;

    /**
     * Create an instance of a {@link JdbcPrimaryKeyCache} to track table primary keys.
     *
     * @param dataSource A DataSource instance used to extract Table objects.
     */
    public JdbcPrimaryKeyCache(DataSource dataSource) {
      this.dataSource = dataSource;
    }

    private List<String> getTablePrimaryKeys(
        String schemaName, String tableName, int retriesRemaining) {
      List<String> primaryKeys = new ArrayList<String>();
      try (Connection connection = getConnection(this.dataSource, MAX_RETRIES, MAX_RETRIES)) {
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet jdbcPrimaryKeys = metaData.getPrimaryKeys(null, schemaName, tableName)) {
          while (jdbcPrimaryKeys.next()) {
            primaryKeys.add(jdbcPrimaryKeys.getString("COLUMN_NAME"));
          }
        }
      } catch (SQLException e) {
        if (retriesRemaining > 0) {
          int sleepSecs = (MAX_RETRIES - retriesRemaining + 1) * 10;
          LOG.info(
              "SQLException Occurred, will retry after {} seconds: Failed to Retrieve Primary Keys: {}.{} : {}",
              sleepSecs,
              schemaName,
              tableName,
              e.toString());
          try {
            Thread.sleep(sleepSecs * 1000);
            return getTablePrimaryKeys(schemaName, tableName, retriesRemaining - 1);
          } catch (InterruptedException i) {
          }
        }
        LOG.error(
            "SQLException Occurred: Failed to Retrieve Primary Keys: {}.{} : {}",
            schemaName,
            tableName,
            e.toString());
      }

      return primaryKeys;
    }

    @Override
    public List<String> getObjectValue(List<String> key) {
      String schemaName = key.get(0);
      String tableName = key.get(1);

      List<String> primaryKeys = getTablePrimaryKeys(schemaName, tableName, MAX_RETRIES);

      return primaryKeys;
    }
  }
}
