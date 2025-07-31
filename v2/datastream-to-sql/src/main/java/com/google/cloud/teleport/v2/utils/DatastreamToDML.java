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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.teleport.v2.datastream.io.CdcJdbcIO;
import com.google.cloud.teleport.v2.datastream.values.DatastreamRow;
import com.google.cloud.teleport.v2.datastream.values.DmlInfo;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A set of Database Migration utilities to convert JSON data to DML. */
public abstract class DatastreamToDML
    extends DoFn<FailsafeElement<String, String>, KV<String, DmlInfo>> {

  private static final Logger LOG = LoggerFactory.getLogger(DatastreamToDML.class);

  private static String rowIdColumnName = "rowid";
  private static List<String> defaultPrimaryKeys;
  private static MappedObjectCache<List<String>, Map<String, String>> tableCache;
  private static MappedObjectCache<List<String>, List<String>> primaryKeyCache;
  private CdcJdbcIO.DataSourceConfiguration dataSourceConfiguration;
  private DataSource dataSource;
  public String quoteCharacter;
  protected Map<String, String> schemaMap = new HashMap<String, String>();
  protected Boolean orderByIncludesIsDeleted = false;

  public abstract String getDefaultQuoteCharacter();

  public abstract String getDeleteDmlStatement();

  public abstract String getUpsertDmlStatement();

  public abstract String getInsertDmlStatement();

  public abstract String getTargetCatalogName(DatastreamRow row);

  public abstract String getTargetSchemaName(DatastreamRow row);

  public abstract String getTargetTableName(DatastreamRow row);

  /* An exception for delete DML without a primary key */
  private class DeletedWithoutPrimaryKey extends RuntimeException {
    public DeletedWithoutPrimaryKey(String errorMessage) {
      super(errorMessage);
    }
  }

  public DatastreamToDML(CdcJdbcIO.DataSourceConfiguration config) {
    this.dataSourceConfiguration = config;
    this.quoteCharacter = getDefaultQuoteCharacter();
  }

  public DatastreamToDML withQuoteCharacter(String quoteChar) {
    this.quoteCharacter = quoteChar;
    return this;
  }

  public DatastreamToDML withSchemaMap(Map<String, String> schemaMap) {
    this.schemaMap = schemaMap;
    return this;
  }

  public DatastreamToDML withOrderByIncludesIsDeleted(Boolean orderByIncludesIsDeleted) {
    this.orderByIncludesIsDeleted = orderByIncludesIsDeleted;
    return this;
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    FailsafeElement<String, String> element = context.element();
    String jsonString = element.getPayload();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode rowObj;

    try {
      rowObj = mapper.readTree(jsonString);
      DmlInfo dmlInfo = convertJsonToDmlInfo(rowObj, element.getOriginalPayload());

      // Null rows suggest no DML is required.
      if (dmlInfo != null) {
        LOG.debug("Output Data: {}", jsonString);
        context.output(KV.of(dmlInfo.getStateWindowKey(), dmlInfo));
      } else {
        LOG.debug("Skipping Null DmlInfo: {}", jsonString);
      }
    } catch (IOException e) {
      // TODO(dhercher): Push failure to DLQ collection
      LOG.error("IOException: {} :: {}", jsonString, e.toString());
    }
  }

  protected String cleanTableName(String tableName) {
    return tableName;
  }

  protected String cleanSchemaName(String schemaName) {
    schemaName = applySchemaMap(schemaName);
    return schemaName;
  }

  protected String applySchemaMap(String sourceSchema) {
    return schemaMap.getOrDefault(sourceSchema, sourceSchema);
  }

  // TODO(dhercher): Only if source is oracle, pull from DatastreamRow
  public List<String> getDefaultPrimaryKeys() {
    if (this.defaultPrimaryKeys == null) {
      this.defaultPrimaryKeys = Arrays.asList(this.rowIdColumnName);
    }
    return this.defaultPrimaryKeys;
  }

  public DataSource getDataSource() {
    if (this.dataSource == null) {
      // This can be null in unit tests that don't need a real connection.
      if (this.dataSourceConfiguration == null) {
        return null;
      }
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

  public Map<String, String> getTableSchema(
      String catalogName, String schemaName, String tableName) {
    List<String> searchKey = ImmutableList.of(catalogName, schemaName, tableName);

    if (this.tableCache == null) {
      setUpTableCache();
    }

    return this.tableCache.get(searchKey);
  }

  public List<String> getPrimaryKeys(
      String catalogName, String schemaName, String tableName, JsonNode rowObj) {
    List<String> searchKey = ImmutableList.of(catalogName, schemaName, tableName);

    if (this.primaryKeyCache == null) {
      setUpPrimaryKeyCache();
    }

    // This correctly fetches ["ROW_ID"] from the target DB
    List<String> primaryKeys = this.primaryKeyCache.get(searchKey);

    for (String primaryKey : primaryKeys) { // primaryKey is "ROW_ID"

      // --- CORRECTED LOGIC ---
      // Instead of a case-sensitive .has() check, use our case-insensitive helper.
      if (findMatchingSourceKey(rowObj, primaryKey) == null) {
        // If no case-insensitive match is found, then we can fall back.
        return this.getDefaultPrimaryKeys();
      }
      // --- END CORRECTION ---
    }

    return primaryKeys; // This will now correctly return ["ROW_ID"]
  }

  // Ensure this helper method is available in your class
  private String findMatchingSourceKey(JsonNode rowObj, String targetKeyName) {
    for (Iterator<String> it = rowObj.fieldNames(); it.hasNext(); ) {
      String sourceKeyName = it.next();
      if (sourceKeyName.equalsIgnoreCase(targetKeyName)) {
        return sourceKeyName;
      }
    }
    return null;
  }

  public String quote(String name) {
    return quoteCharacter + name + quoteCharacter;
  }

  public DmlInfo convertJsonToDmlInfo(JsonNode rowObj, String failsafeValue) {
    DatastreamRow row = DatastreamRow.of(rowObj);
    try {
      // Get the target names which will be correctly cased by the subclass implementations
      // The original source names are passed to the cache for lookup
      String catalogName = this.getTargetCatalogName(row);
      String schemaName = this.getTargetSchemaName(row);
      String tableName = this.getTargetTableName(row);

      // CORRECTED CALL
      Map<String, String> tableSchema = this.getTableSchema(catalogName, schemaName, tableName);

      if (tableSchema.isEmpty()) {
        // If the table DNE we return null (NOOP).
        return null;
      }

      // Use the correctly cased names from the cache, falling back to original names if needed.
      String correctedSchemaName =
          tableSchema.getOrDefault("__correctly_cased_schema_name__", schemaName);
      String correctedTableName =
          tableSchema.getOrDefault("__correctly_cased_table_name__", tableName);

      List<String> primaryKeys =
          this.getPrimaryKeys(catalogName, correctedSchemaName, correctedTableName, rowObj);
      List<String> orderByFields = row.getSortFields(orderByIncludesIsDeleted);
      List<String> primaryKeyValues = getFieldValues(rowObj, primaryKeys, tableSchema, false);
      List<String> orderByValues =
          getFieldValues(rowObj, orderByFields, tableSchema, orderByIncludesIsDeleted);

      String dmlSqlTemplate = getDmlTemplate(rowObj, primaryKeys);
      Map<String, String> sqlTemplateValues =
          getSqlTemplateValues(
              rowObj,
              catalogName,
              correctedSchemaName,
              correctedTableName,
              primaryKeys,
              tableSchema);

      StringSubstitutor stringSubstitutor = new StringSubstitutor(sqlTemplateValues, "{", "}");
      String dmlSql =
          stringSubstitutor.setDisableSubstitutionInValues(true).replace(dmlSqlTemplate);
      return DmlInfo.of(
          failsafeValue,
          dmlSql,
          correctedSchemaName,
          correctedTableName,
          primaryKeys,
          orderByFields,
          primaryKeyValues,
          orderByValues);
    } catch (DeletedWithoutPrimaryKey e) {
      LOG.error("CDC Error: {} :: {}", rowObj.toString(), e.toString());
      return null;
    } catch (Exception e) {
      // TODO(dhercher): Consider raising an error and pushing to DLQ
      LOG.error("Value Error for: " + rowObj.toString(), e);
      return null;
    }
  }

  public String getDmlTemplate(JsonNode rowObj, List<String> primaryKeys) {
    Boolean isDelete = rowObj.get("_metadata_deleted").asBoolean();
    Boolean hasPrimaryKeys = primaryKeys.size() != 0;
    if (isDelete && !hasPrimaryKeys) {
      throw new DeletedWithoutPrimaryKey("Delete DML without primary keys cannot be applied");
    } else if (isDelete) {
      return getDeleteDmlStatement();
    } else if (hasPrimaryKeys) {
      return getUpsertDmlStatement();
    } else {
      return getInsertDmlStatement();
    }
  }

  public Map<String, String> getSqlTemplateValues(
      JsonNode rowObj,
      String catalogName,
      String schemaName,
      String tableName,
      List<String> primaryKeys,
      Map<String, String> tableSchema) {
    Map<String, String> sqlTemplateValues = new HashMap<>();

    sqlTemplateValues.put("quoted_catalog_name", quote(catalogName));
    sqlTemplateValues.put("quoted_schema_name", quote(schemaName));
    sqlTemplateValues.put("quoted_table_name", quote(tableName));
    sqlTemplateValues.put(
        "primary_key_kv_sql", getPrimaryKeyToValueFilterSql(rowObj, primaryKeys, tableSchema));
    sqlTemplateValues.put("quoted_column_names", getColumnsListSql(rowObj, tableSchema));
    sqlTemplateValues.put("column_value_sql", getColumnsValuesSql(rowObj, tableSchema));

    List<String> quotedPrimaryKeys = new ArrayList<>();
    for (String pk : primaryKeys) {
      quotedPrimaryKeys.add(quote(pk));
    }
    sqlTemplateValues.put("primary_key_names_sql", String.join(",", quotedPrimaryKeys));

    sqlTemplateValues.put("column_kv_sql", getColumnsUpdateSql(rowObj, tableSchema, primaryKeys));

    return sqlTemplateValues;
  }

  public String getValueSql(JsonNode rowObj, String columnName, Map<String, String> tableSchema) {
    String columnValue;
    JsonNode columnObj = rowObj.get(columnName);
    if (columnObj == null) {
      LOG.warn("Missing Required Value: {} in {}", columnName, rowObj.toString());
      return "";
    }
    if (columnObj.isTextual()) {
      columnValue = "\'" + cleanSql(columnObj.textValue()) + "\'";
    } else {
      columnValue = columnObj.toString();
    }
    return cleanDataTypeValueSql(columnValue, columnName, tableSchema);
  }

  public String cleanDataTypeValueSql(
      String columnValue, String columnName, Map<String, String> tableSchema) {
    return columnValue;
  }

  public String getNullValueSql() {
    return "NULL";
  }

  public static String cleanSql(String str) {
    if (str == null) {
      return null;
    }
    String cleanedNullBytes = StringUtils.replace(str, "\u0000", "");

    return escapeSql(cleanedNullBytes);
  }

  public static String escapeSql(String str) {
    return StringUtils.replace(str, "'", "''");
  }

  public List<String> getFieldValues(
      JsonNode rowObj,
      List<String> targetFieldNames, // Renamed for clarity, e.g., ["ROW_ID"]
      Map<String, String> tableSchema,
      Boolean overrideIsDeleted) {
    List<String> fieldValues = new ArrayList<String>();

    for (String targetFieldName : targetFieldNames) {
      // Find the matching key in the source JSON, ignoring case.
      String sourceFieldName = findMatchingSourceKey(rowObj, targetFieldName);

      if (sourceFieldName != null) {
        // SUCCESS: Use the correctly-cased source key ("row_id") to get the value.
        if (overrideIsDeleted && sourceFieldName.equals("_metadata_deleted")) {
          String val = getValueSql(rowObj, sourceFieldName, tableSchema);
          fieldValues.add(val == "true" ? "1" : "0");
        } else {
          fieldValues.add(getValueSql(rowObj, sourceFieldName, tableSchema));
        }
      } else {
        LOG.warn(
            "Could not find a case-insensitive match for target field '{}' in JSON: {}",
            targetFieldName,
            rowObj.toString());
      }
    }
    return fieldValues;
  }

  public String getColumnsListSql(JsonNode rowObj, Map<String, String> tableSchema) {
    List<String> quotedTargetColumns = new ArrayList<>();

    for (Iterator<String> it = rowObj.fieldNames(); it.hasNext(); ) {
      String sourceColumnName = it.next();

      // Find the correctly-cased target column that matches the source column
      String matchingTargetColumn = findMatchingTargetColumn(sourceColumnName, tableSchema);

      if (matchingTargetColumn != null) {
        // Add the correctly-cased and quoted TARGET column name to our list
        quotedTargetColumns.add(quote(matchingTargetColumn));
      }
    }
    return String.join(",", quotedTargetColumns);
  }

  // You need this helper method (or one like it)
  private String findMatchingTargetColumn(
      String sourceColumnName, Map<String, String> tableSchema) {
    for (String targetColumnName : tableSchema.keySet()) {
      if (targetColumnName.equalsIgnoreCase(sourceColumnName)) {
        return targetColumnName; // Return the correctly-cased target name
      }
    }
    return null;
  }

  public String getColumnsValuesSql(JsonNode rowObj, Map<String, String> tableSchema) {
    List<String> values = new ArrayList<>();
    for (Iterator<String> it = rowObj.fieldNames(); it.hasNext(); ) {
      String sourceColumnName = it.next();

      // CORRECTED: Use the pattern to check if a match exists in the target
      if (findMatchingTargetColumn(sourceColumnName, tableSchema) != null) {
        // If it exists, get the VALUE from the source JSON using the source name
        String columnValue = getValueSql(rowObj, sourceColumnName, tableSchema);
        values.add(columnValue);
      }
    }
    return String.join(",", values);
  }

  public String getColumnsUpdateSql(
      JsonNode rowObj, Map<String, String> tableSchema, List<String> primaryKeys) {
    List<String> updatePairs = new ArrayList<>();

    for (Iterator<String> it = rowObj.fieldNames(); it.hasNext(); ) {
      String sourceColumnName = it.next();
      String matchingTargetColumn = findMatchingTargetColumn(sourceColumnName, tableSchema);

      if (matchingTargetColumn != null) {
        // --- NEW CHECK: Skip this column if it's a primary key ---
        boolean isPrimaryKey = false;
        for (String pk : primaryKeys) {
          if (pk.equalsIgnoreCase(matchingTargetColumn)) {
            isPrimaryKey = true;
            break;
          }
        }

        if (isPrimaryKey) {
          continue; // Skip primary keys in the SET clause
        }
        // --- END NEW CHECK ---

        String columnValue = getValueSql(rowObj, sourceColumnName, tableSchema);
        String updatePair = quote(matchingTargetColumn) + "=" + columnValue;
        updatePairs.add(updatePair);
      }
    }
    return String.join(",", updatePairs);
  }

  public String getPrimaryKeyToValueFilterSql(
      JsonNode rowObj, List<String> primaryKeys, Map<String, String> tableSchema) {
    List<String> pkFilters = new ArrayList<>();

    // The primaryKeys list contains the correctly-cased names from the TARGET DB (e.g., "ROW_ID")
    for (String targetPkColumnName : primaryKeys) {
      // Find the matching column name in the SOURCE JSON (e.g., "row_id")
      String sourcePkColumnName = findMatchingSourceKey(rowObj, targetPkColumnName);

      if (sourcePkColumnName != null) {
        // Use the SOURCE name to get the value from the JSON
        String columnValue = getValueSql(rowObj, sourcePkColumnName, tableSchema);

        // Use the TARGET name to build the SQL filter
        pkFilters.add(quote(targetPkColumnName) + "=" + columnValue);
      } else {
        LOG.warn(
            "Primary key '{}' from target schema not found in source JSON payload.",
            targetPkColumnName);
      }
    }

    return String.join(" AND ", pkFilters);
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
          LOG.info("InterruptedException retrieving connection");
        }
      }
      LOG.error("SQLException: Connection Error: {}", e.toString());
    }

    return connection;
  }

  /**
   * The {@link JdbcTableCache} manages safely getting and setting JDBC Table objects from a local
   * cache for each worker thread.
   */
  public static class JdbcTableCache extends MappedObjectCache<List<String>, Map<String, String>> {

    private DataSource dataSource;
    private static final int MAX_RETRIES = 5;

    public JdbcTableCache(DataSource dataSource) {
      this.dataSource = dataSource;
    }

    private Map<String, String> getTableSchema(
        String catalogName, String schemaName, String tableName, int retriesRemaining) {
      Map<String, String> tableSchema = new HashMap<>();

      try (Connection connection = getConnection(this.dataSource, MAX_RETRIES, MAX_RETRIES)) {
        DatabaseMetaData metaData = connection.getMetaData();
        String correctlyCasedSchema = null;
        String correctlyCasedTable = null;

        try (ResultSet tables =
            metaData.getTables(catalogName, schemaName, null, new String[] {"TABLE"})) {
          while (tables.next()) {
            String dbTableName = tables.getString("TABLE_NAME");
            if (tableName.equalsIgnoreCase(dbTableName)) {
              String dbSchemaName = tables.getString("TABLE_SCHEM");
              correctlyCasedSchema = (dbSchemaName == null) ? "" : dbSchemaName;
              correctlyCasedTable = dbTableName;
              break; // Found the exact match.
            }
          }
        }

        if (correctlyCasedTable != null) {
          try (ResultSet columns =
              metaData.getColumns(catalogName, correctlyCasedSchema, correctlyCasedTable, null)) {
            while (columns.next()) {
              tableSchema.put(columns.getString("COLUMN_NAME"), columns.getString("TYPE_NAME"));
            }
          }

          tableSchema.put("__correctly_cased_schema_name__", correctlyCasedSchema);
          tableSchema.put("__correctly_cased_table_name__", correctlyCasedTable);
        }
      } catch (SQLException e) {
        if (retriesRemaining > 0) {
          int sleepSecs = (MAX_RETRIES - retriesRemaining + 1) * 10;
          LOG.info(
              "SQLException, will retry after {} seconds: Failed to Retrieve Schema: {}.{} : {}",
              sleepSecs,
              schemaName,
              tableName,
              e.toString());
          try {
            Thread.sleep(sleepSecs * 1000);
            return getTableSchema(catalogName, schemaName, tableName, retriesRemaining - 1);
          } catch (InterruptedException i) {
            LOG.info("InterruptedException retrieving schema: {}.{}", schemaName, tableName);
          }
        }
        LOG.error(
            "SQLException: Failed to Retrieve Schema: {}.{} : {}",
            schemaName,
            tableName,
            e.toString());
      }

      if (tableSchema.isEmpty()) {
        LOG.warn(
            "Table Not Found (case-insensitive search): Catalog: {}, Schema: {}, Table: {}",
            catalogName,
            schemaName,
            tableName);
      }
      return tableSchema;
    }

    @Override
    public Map<String, String> getObjectValue(List<String> key) {
      String catalogName = key.get(0);
      String schemaName = key.get(1);
      String tableName = key.get(2);

      return getTableSchema(catalogName, schemaName, tableName, MAX_RETRIES);
    }
  }

  /**
   * The {@link JdbcPrimaryKeyCache} manages safely getting and setting JDBC Table PKs from a local
   * cache for each worker thread.
   */
  public static class JdbcPrimaryKeyCache extends MappedObjectCache<List<String>, List<String>> {

    private DataSource dataSource;
    private static final int MAX_RETRIES = 5;

    public JdbcPrimaryKeyCache(DataSource dataSource) {
      this.dataSource = dataSource;
    }

    private List<String> getTablePrimaryKeys(
        String catalogName, String schemaName, String tableName, int retriesRemaining) {
      List<String> primaryKeys = new ArrayList<String>();
      try (Connection connection = getConnection(this.dataSource, MAX_RETRIES, MAX_RETRIES)) {
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet jdbcPrimaryKeys =
            metaData.getPrimaryKeys(catalogName, schemaName, tableName)) {
          while (jdbcPrimaryKeys.next()) {
            primaryKeys.add(jdbcPrimaryKeys.getString("COLUMN_NAME"));
          }
        }
      } catch (SQLException e) {
        if (retriesRemaining > 0) {
          int sleepSecs = (MAX_RETRIES - retriesRemaining + 1) * 10;
          LOG.info(
              "SQLException, will retry after {} seconds: Failed to Retrieve Primary Key: {}.{} :"
                  + " {}",
              sleepSecs,
              schemaName,
              tableName,
              e.toString());
          try {
            Thread.sleep(sleepSecs * 1000);
            return getTablePrimaryKeys(catalogName, schemaName, tableName, retriesRemaining - 1);
          } catch (InterruptedException i) {
            LOG.info("InterruptedException retrieving pk: {}.{}", schemaName, tableName);
          }
        }
        LOG.error(
            "SQLException: Failed to Retrieve Primary Key: {}.{} : {}",
            schemaName,
            tableName,
            e.toString());
      }

      return primaryKeys;
    }

    @Override
    public List<String> getObjectValue(List<String> key) {
      String catalogName = key.get(0);
      String schemaName = key.get(1);
      String tableName = key.get(2);

      return getTablePrimaryKeys(catalogName, schemaName, tableName, MAX_RETRIES);
    }
  }
}
