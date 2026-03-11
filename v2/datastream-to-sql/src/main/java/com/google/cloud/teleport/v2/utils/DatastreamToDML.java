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
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.teleport.v2.datastream.io.CdcJdbcIO;
import com.google.cloud.teleport.v2.datastream.utils.DataStreamClient;
import com.google.cloud.teleport.v2.datastream.values.DatastreamRow;
import com.google.cloud.teleport.v2.datastream.values.DmlInfo;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.CaseFormat;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A set of Database Migration utilities to convert JSON data to DML. */
public abstract class DatastreamToDML
    extends DoFn<FailsafeElement<String, String>, KV<String, DmlInfo>> {

  private static final Logger LOG = LoggerFactory.getLogger(DatastreamToDML.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Tag for records that failed DML conversion and should be sent to the DLQ. */
  public static final TupleTag<FailsafeElement<String, String>> ERROR_TAG =
      new TupleTag<FailsafeElement<String, String>>() {};

  private static String rowIdColumnName = "rowid";
  private static List<String> defaultPrimaryKeys;

  // These must be transient or Serializable. MappedObjectCache is usually Serializable.
  private transient MappedObjectCache<List<String>, Map<String, String>> tableCache;
  private transient MappedObjectCache<List<String>, List<String>> primaryKeyCache;
  private static final Cache<List<String>, List<String>> tableLockMap =
      CacheBuilder.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build();

  private CdcJdbcIO.DataSourceConfiguration dataSourceConfiguration;
  private transient DataSource dataSource;
  private transient DataStreamClient datastreamClient;
  private String datastreamRootUrl = "https://datastream.googleapis.com/";
  private String streamName = "{_metadata_stream}";

  public String quoteCharacter;
  protected String defaultCasing = "LOWERCASE";
  protected String columnCasing = "LOWERCASE";
  protected Map<String, String> schemaMappings = new HashMap<>();
  protected Map<String, String> tableMappings = new HashMap<>();
  protected Boolean orderByIncludesIsDeleted = false;
  protected Integer schemaCacheRefreshMinutes = 1440;

  public abstract String getDefaultQuoteCharacter();

  public abstract String getDeleteDmlStatement();

  public abstract String getUpsertDmlStatement();

  public abstract String getInsertDmlStatement();

  public abstract String getTargetCatalogName(DatastreamRow row);

  public abstract String getTargetSchemaName(DatastreamRow row);

  public abstract String getCreateTableSql(
      String catalogName,
      String schemaName,
      String tableName,
      List<String> primaryKeys,
      Map<String, String> sourceSchema);

  public abstract String getAddColumnSql(
      String catalogName,
      String schemaName,
      String tableName,
      String columnName,
      String columnType);

  public abstract String getDestinationType(String sourceType, Long precision, Long scale);

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

  public DatastreamToDML withDefaultCasing(String casing) {
    if (casing != null) {
      this.defaultCasing = casing;
    }
    return this;
  }

  public DatastreamToDML withColumnCasing(String casing) {
    if (casing != null) {
      this.columnCasing = casing;
    }
    return this;
  }

  public DatastreamToDML withSchemaCacheRefreshMinutes(Integer cacheMinutes) {
    if (cacheMinutes != null) {
      this.schemaCacheRefreshMinutes = cacheMinutes;
    }
    return this;
  }

  protected String applyCasing(String name) {
    return applyCasingLogic(name, this.defaultCasing);
  }

  protected String applyCasingLogic(String name, String casingOption) {
    if (name == null || name.isEmpty()) {
      return name;
    }

    switch (casingOption.toUpperCase()) {
      case "UPPERCASE":
        return name.toUpperCase();
      case "CAMEL":
        if (name.contains("_")) {
          return CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name);
        }
        return name;
      case "SNAKE":
        return CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name);
      case "LOWERCASE":
      default:
        return name.toLowerCase();
    }
  }

  public DatastreamToDML withSchemaMap(Map<String, String> combinedMap) {
    for (Map.Entry<String, String> entry : combinedMap.entrySet()) {
      if (entry.getKey().contains(".")) {
        this.tableMappings.put(entry.getKey(), entry.getValue());
      } else {
        this.schemaMappings.put(entry.getKey(), entry.getValue());
      }
    }
    return this;
  }

  public DatastreamToDML withTableNameMap(Map<String, String> tableNameMap) {
    this.tableMappings = tableNameMap;
    return this;
  }

  public DatastreamToDML withOrderByIncludesIsDeleted(Boolean orderByIncludesIsDeleted) {
    this.orderByIncludesIsDeleted = orderByIncludesIsDeleted;
    return this;
  }

  public DatastreamToDML withDataStreamRootUrl(String url) {
    this.datastreamRootUrl = url;
    return this;
  }

  public DatastreamToDML withStreamName(String streamName) {
    if (streamName != null) {
      this.streamName = streamName;
    }
    return this;
  }

  @Setup
  public void setup(PipelineOptions options) throws IOException {
    LOG.info("Initializing DatastreamToDML worker. Root URL: {}", this.datastreamRootUrl);
    LOG.info("Schema Mappings: {}", this.schemaMappings);
    LOG.info("Table Mappings: {}", this.tableMappings);
    if (this.datastreamClient == null) {
      this.datastreamClient = new DataStreamClient(options.as(GcpOptions.class).getGcpCredential());
      this.datastreamClient.setRootUrl(this.datastreamRootUrl);
    }
    setUpTableCache();
    setUpPrimaryKeyCache();
  }

  @ProcessElement
  public void processElement(ProcessContext context) {
    FailsafeElement<String, String> element = context.element();
    String jsonString = element.getPayload();
    JsonNode rowObj;

    try {
      rowObj = MAPPER.readTree(jsonString);
      DmlInfo dmlInfo = convertJsonToDmlInfo(rowObj, element.getOriginalPayload());

      // Null rows suggest no DML is required.
      if (dmlInfo != null) {
        LOG.debug("Output Data: {}", jsonString);
        context.output(KV.of(dmlInfo.getStateWindowKey(), dmlInfo));
      } else {
        LOG.debug("Skipping Null DmlInfo: {}", jsonString);
      }
    } catch (Exception e) {
      LOG.error("Failed to process element: {}", jsonString, e);
      context.output(
          ERROR_TAG,
          FailsafeElement.of(element.getOriginalPayload(), jsonString)
              .setErrorMessage(e.getMessage())
              .setStacktrace(java.util.Arrays.toString(e.getStackTrace())));
    }
  }

  // TODO(dhercher): Only if source is oracle, pull from DatastreamRow
  public List<String> getDefaultPrimaryKeys() {
    if (defaultPrimaryKeys == null) {
      defaultPrimaryKeys = Arrays.asList(rowIdColumnName);
    }
    return defaultPrimaryKeys;
  }

  public DataSource getDataSource() {
    if (this.dataSource == null && this.dataSourceConfiguration != null) {
      this.dataSource = this.dataSourceConfiguration.buildDatasource();
    }
    return this.dataSource;
  }

  private synchronized void setUpTableCache() {
    if (this.tableCache == null) {
      this.tableCache =
          new JdbcTableCache(this).withCacheResetTimeUnitValue(this.schemaCacheRefreshMinutes);
    }
  }

  private synchronized void setUpPrimaryKeyCache() {
    if (this.primaryKeyCache == null) {
      this.primaryKeyCache =
          new JdbcPrimaryKeyCache(this).withCacheResetTimeUnitValue(this.schemaCacheRefreshMinutes);
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

  protected String getFullSourceTableName(DatastreamRow row) {
    return row.getSchemaName() + "." + row.getTableName();
  }

  public String getTargetTableName(DatastreamRow row) {
    String fullSourceTableName = getFullSourceTableName(row);
    if (tableMappings.containsKey(fullSourceTableName)) {
      return tableMappings.get(fullSourceTableName).split("\\.")[1];
    }
    return applyCasing(row.getTableName());
  }

  public List<String> getPrimaryKeys(
      String catalogName, String schemaName, String tableName, JsonNode rowObj) {
    List<String> searchKey = ImmutableList.of(catalogName, schemaName, tableName);

    if (this.primaryKeyCache == null) {
      setUpPrimaryKeyCache();
    }

    List<String> destinationPrimaryKeys = this.primaryKeyCache.get(searchKey);

    java.util.Set<String> casedSourceFieldNames = new java.util.HashSet<>();
    for (java.util.Iterator<String> it = rowObj.fieldNames(); it.hasNext(); ) {
      String sourceFieldName = it.next();
      casedSourceFieldNames.add(applyCasingLogic(sourceFieldName, this.columnCasing));
    }

    for (String destPk : destinationPrimaryKeys) {
      if (!casedSourceFieldNames.contains(destPk)) {
        return this.getDefaultPrimaryKeys();
      }
    }

    return destinationPrimaryKeys;
  }

  public String quote(String name) {
    if (name == null || name.isEmpty()) {
      return "";
    }
    return quoteCharacter + name + quoteCharacter;
  }

  private List<String> getTableLock(List<String> searchKey) {
    List<String> tableLock = tableLockMap.getIfPresent(searchKey);
    if (tableLock != null) {
      return tableLock;
    }

    synchronized (tableLockMap) {
      tableLock = tableLockMap.getIfPresent(searchKey);
      if (tableLock != null) {
        return tableLock;
      }

      tableLockMap.put(searchKey, searchKey);
      return searchKey;
    }
  }

  private void executeSql(String sql) throws SQLException {
    LOG.info("Executing DDL: {}", sql);
    try (Connection connection = getDataSource().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private void updateTableIfRequired(
      String catalogName, String schemaName, String tableName, JsonNode rowObj)
      throws IOException, SQLException {
    List<String> searchKey = ImmutableList.of(catalogName, schemaName, tableName);
    Map<String, String> tableSchema = this.getTableSchema(catalogName, schemaName, tableName);

    if (tableSchema.isEmpty()) {
      List<String> tableLock = getTableLock(searchKey);
      synchronized (tableLock) {
        if (this.tableCache == null) {
          setUpTableCache();
        }
        tableSchema = tableCache.reset(searchKey);
        if (tableSchema.isEmpty()) {
          DatastreamRow row = DatastreamRow.of(rowObj);
          String streamNameValue = row.formatStringTemplate(this.streamName);
          String sourceSchemaName = row.getSchemaName();
          String sourceTableName = row.getTableName();

          LOG.info(
              "Table Not Found. Attempting to create: {}.{} (stream: {})",
              schemaName,
              tableName,
              streamNameValue);
          if (this.datastreamClient == null) {
            throw new RuntimeException("DataStreamClient is null in updateTableIfRequired!");
          }
          Map<String, StandardSQLTypeName> sourceSchema =
              this.datastreamClient.getObjectSchema(
                  streamNameValue, sourceSchemaName, sourceTableName);
          List<String> primaryKeys =
              this.datastreamClient.getPrimaryKeys(
                  streamNameValue, sourceSchemaName, sourceTableName);

          // Convert BigQuery Types to Destination SQL Types
          Map<String, String> destinationSchema = new HashMap<>();
          for (Map.Entry<String, StandardSQLTypeName> entry : sourceSchema.entrySet()) {
            String casedSourceColName = applyCasingLogic(entry.getKey(), this.columnCasing);
            destinationSchema.put(
                casedSourceColName, getDestinationType(entry.getValue().name(), null, null));
          }

          // Add metadata columns if they are in the rowObj but not in sourceSchema
          for (Iterator<String> fieldNames = rowObj.fieldNames(); fieldNames.hasNext(); ) {
            String columnName = fieldNames.next();
            String casedColumnName = applyCasingLogic(columnName, this.columnCasing);
            if (!destinationSchema.containsKey(casedColumnName)) {
              destinationSchema.put(casedColumnName, getDestinationType("STRING", null, null));
            }
          }

          String createTableSql =
              getCreateTableSql(catalogName, schemaName, tableName, primaryKeys, destinationSchema);
          executeSql(createTableSql);

          tableCache.reset(searchKey);
          if (this.primaryKeyCache == null) {
            setUpPrimaryKeyCache();
          }
          primaryKeyCache.reset(searchKey);
          tableSchema = this.getTableSchema(catalogName, schemaName, tableName);
        }
      }
    }

    // Check for missing columns
    List<String> missingColumns = new ArrayList<>();
    for (Iterator<String> fieldNames = rowObj.fieldNames(); fieldNames.hasNext(); ) {
      String columnName = fieldNames.next();
      String casedColumnName = applyCasingLogic(columnName, this.columnCasing);
      if (!tableSchema.containsKey(casedColumnName)) {
        missingColumns.add(columnName);
      }
    }

    if (!missingColumns.isEmpty()) {
      List<String> tableLock = getTableLock(searchKey);
      synchronized (tableLock) {
        if (this.tableCache == null) {
          setUpTableCache();
        }
        tableSchema = tableCache.reset(searchKey);
        for (String columnName : missingColumns) {
          String casedColumnName = applyCasingLogic(columnName, this.columnCasing);
          if (!tableSchema.containsKey(casedColumnName)) {
            DatastreamRow row = DatastreamRow.of(rowObj);
            String streamNameValue = row.formatStringTemplate(this.streamName);
            String sourceSchemaName = row.getSchemaName();
            String sourceTableName = row.getTableName();

            LOG.info(
                "Column {} missing. Attempting to add to Table: {}.{} (stream: {})",
                casedColumnName,
                schemaName,
                tableName,
                streamNameValue);
            if (this.datastreamClient == null) {
              throw new RuntimeException("DataStreamClient is null when adding column!");
            }
            Map<String, StandardSQLTypeName> sourceSchema =
                this.datastreamClient.getObjectSchema(
                    streamNameValue, sourceSchemaName, sourceTableName);

            String sourceType = "STRING";
            if (sourceSchema.containsKey(columnName)) {
              sourceType = sourceSchema.get(columnName).name();
            }
            String destinationType = getDestinationType(sourceType, null, null);
            String addColumnSql =
                getAddColumnSql(
                    catalogName, schemaName, tableName, casedColumnName, destinationType);
            executeSql(addColumnSql);
            tableCache.reset(searchKey);
            tableSchema = this.getTableSchema(catalogName, schemaName, tableName);
          }
        }
      }
    }
  }

  public DmlInfo convertJsonToDmlInfo(JsonNode rowObj, String failsafeValue) {
    DatastreamRow row = DatastreamRow.of(rowObj);
    // Oracle uses upper case while Postgres uses all lowercase.
    // We lowercase the values of these metadata fields to align with
    // our schema conversion rules.
    String catalogName = this.getTargetCatalogName(row);
    String schemaName = this.getTargetSchemaName(row);
    String tableName = this.getTargetTableName(row);

    try {
      updateTableIfRequired(catalogName, schemaName, tableName, rowObj);
    } catch (Exception e) {
      LOG.error("Failed to update table schema: {}.{}", schemaName, tableName, e);
    }

    Map<String, String> tableSchema = this.getTableSchema(catalogName, schemaName, tableName);
    if (tableSchema.isEmpty()) {
      throw new RuntimeException(
          String.format(
              "Target table not found or schema retrieval failed: %s.%s (catalog: %s)",
              schemaName, tableName, catalogName));
    }

    List<String> primaryKeys = this.getPrimaryKeys(catalogName, schemaName, tableName, rowObj);
    List<String> orderByFields = row.getSortFields(orderByIncludesIsDeleted);

    // Calculate primaryKeyValues using destination primaryKeys to ensure per-PK state consistency
    List<String> primaryKeyValues = new ArrayList<>();
    for (String pk : primaryKeys) {
      boolean found = false;
      for (Iterator<String> it = rowObj.fieldNames(); it.hasNext(); ) {
        String sourceFieldName = it.next();
        if (applyCasingLogic(sourceFieldName, this.columnCasing).equals(pk)) {
          primaryKeyValues.add(getValueSql(rowObj, sourceFieldName, tableSchema));
          found = true;
          break;
        }
      }
      if (!found) {
        primaryKeyValues.add(getNullValueSql());
      }
    }

    List<String> orderByValues =
        getFieldValues(rowObj, orderByFields, tableSchema, orderByIncludesIsDeleted);

    String dmlSqlTemplate = getDmlTemplate(rowObj, primaryKeys);
    Map<String, String> sqlTemplateValues =
        getSqlTemplateValues(rowObj, catalogName, schemaName, tableName, primaryKeys, tableSchema);

    StringSubstitutor stringSubstitutor = new StringSubstitutor(sqlTemplateValues, "{", "}");
    String dmlSql = stringSubstitutor.setDisableSubstitutionInValues(true).replace(dmlSqlTemplate);
    return DmlInfo.of(
        failsafeValue,
        dmlSql,
        schemaName,
        tableName,
        primaryKeys,
        orderByFields,
        primaryKeyValues,
        orderByValues,
        failsafeValue);
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
    String casedAndQuotedPkNames =
        primaryKeys.stream()
            .map(pk -> applyCasingLogic(pk, this.columnCasing))
            .map(this::quote)
            .collect(java.util.stream.Collectors.joining(","));
    sqlTemplateValues.put("primary_key_names_sql", casedAndQuotedPkNames);
    sqlTemplateValues.put("column_kv_sql", getColumnsUpdateSql(rowObj, tableSchema));

    return sqlTemplateValues;
  }

  public String getValueSql(JsonNode rowObj, String columnName, Map<String, String> tableSchema) {
    String columnValue;
    JsonNode columnObj = rowObj.get(columnName);
    if (columnObj == null) {
      LOG.warn("Missing Required Value: {} in {}", columnName, rowObj.toString());
      return getNullValueSql();
    }
    if (columnObj.isTextual()) {
      columnValue = "\'" + cleanSql(columnObj.textValue()) + "\'";
    } else {
      columnValue = columnObj.toString();
    }
    // Apply casing logic to match the keys in tableSchema.
    String casedColumnName = applyCasingLogic(columnName, this.columnCasing);
    return cleanDataTypeValueSql(columnValue, casedColumnName, tableSchema);
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
      List<String> fieldNames,
      Map<String, String> tableSchema,
      Boolean overrideIsDeleted) {
    List<String> fieldValues = new ArrayList<String>();

    for (String fieldName : fieldNames) {
      if (overrideIsDeleted && "_metadata_deleted".equals(fieldName)) {
        String val = getValueSql(rowObj, fieldName, tableSchema);
        fieldValues.add(val.equals("true") ? "1" : "0");
      } else {
        fieldValues.add(getValueSql(rowObj, fieldName, tableSchema));
      }
    }

    return fieldValues;
  }

  public String getColumnsListSql(JsonNode rowObj, Map<String, String> tableSchema) {
    String columnsListSql = "";
    for (Iterator<String> fieldNames = rowObj.fieldNames(); fieldNames.hasNext(); ) {
      String columnName = fieldNames.next();
      // Apply casing logic FIRST to get the destination column name.
      String casedColumnName = applyCasingLogic(columnName, this.columnCasing);

      // Check against the destination schema using the CASED name.
      if (!tableSchema.containsKey(casedColumnName)) {
        continue;
      }

      String quotedColumnName = quote(casedColumnName);
      if (columnsListSql.isEmpty()) {
        columnsListSql = quotedColumnName;
      } else {
        columnsListSql = columnsListSql + "," + quotedColumnName;
      }
    }
    return columnsListSql;
  }

  public String getColumnsValuesSql(JsonNode rowObj, Map<String, String> tableSchema) {
    String valuesInsertSql = "";
    for (Iterator<String> fieldNames = rowObj.fieldNames(); fieldNames.hasNext(); ) {
      String columnName = fieldNames.next();
      String casedColumnName = applyCasingLogic(columnName, this.columnCasing);

      if (!tableSchema.containsKey(casedColumnName)) {
        continue;
      }

      String columnValue = getValueSql(rowObj, columnName, tableSchema);
      if (Objects.equals(valuesInsertSql, "")) {
        valuesInsertSql = columnValue;
      } else {
        valuesInsertSql = valuesInsertSql + "," + columnValue;
      }
    }
    return valuesInsertSql;
  }

  public String getColumnsUpdateSql(JsonNode rowObj, Map<String, String> tableSchema) {
    String onUpdateSql = "";
    for (Iterator<String> fieldNames = rowObj.fieldNames(); fieldNames.hasNext(); ) {
      String columnName = fieldNames.next();
      String casedColumnName = applyCasingLogic(columnName, this.columnCasing);

      if (!tableSchema.containsKey(casedColumnName)) {
        continue;
      }

      String quotedColumnName = quote(casedColumnName);
      String columnValue = getValueSql(rowObj, columnName, tableSchema);

      if (onUpdateSql.isEmpty()) {
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

    for (String pkName : primaryKeys) {
      // Find source field name for this PK
      boolean found = false;
      for (Iterator<String> it = rowObj.fieldNames(); it.hasNext(); ) {
        String sourceFieldName = it.next();
        if (applyCasingLogic(sourceFieldName, this.columnCasing).equals(pkName)) {
          String columnValue = getValueSql(rowObj, sourceFieldName, tableSchema);
          String quotedPkName = quote(pkName);

          if (pkToValueSql.isEmpty()) {
            pkToValueSql = quotedPkName + "=" + columnValue;
          } else {
            pkToValueSql = pkToValueSql + " AND " + quotedPkName + "=" + columnValue;
          }
          found = true;
          break;
        }
      }
      if (!found) {
        String quotedPkName = quote(pkName);
        if (pkToValueSql.isEmpty()) {
          pkToValueSql = quotedPkName + " IS NULL";
        } else {
          pkToValueSql = pkToValueSql + " AND " + quotedPkName + " IS NULL";
        }
      }
    }
    return pkToValueSql;
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

    private final DatastreamToDML dml;
    private static final int MAX_RETRIES = 5;

    public JdbcTableCache(DatastreamToDML dml) {
      this.dml = dml;
    }

    private Map<String, String> getTableSchema(
        String catalogName, String schemaName, String tableName, int retriesRemaining) {
      Map<String, String> tableSchema = new HashMap<String, String>();
      DataSource ds = dml.getDataSource();
      if (ds == null) {
        LOG.error("DataSource is null in JdbcTableCache!");
        return tableSchema;
      }

      String effectiveCatalog = (catalogName == null || catalogName.isEmpty()) ? null : catalogName;

      try (Connection connection = getConnection(ds, MAX_RETRIES, MAX_RETRIES)) {
        if (connection == null) {
          return tableSchema;
        }
        DatabaseMetaData metaData = connection.getMetaData();

        // Try exact match first
        try (ResultSet columns =
            metaData.getColumns(effectiveCatalog, schemaName, tableName, null)) {
          while (columns.next()) {
            String colName = columns.getString("COLUMN_NAME");
            String typeName = columns.getString("TYPE_NAME");
            tableSchema.put(dml.applyCasingLogic(colName, dml.columnCasing), typeName);
          }
        }

        // If not found, and it's Postgres, try lowercasing everything
        if (tableSchema.isEmpty() && metaData.getDatabaseProductName().equals("PostgreSQL")) {
          try (ResultSet columns =
              metaData.getColumns(
                  effectiveCatalog,
                  schemaName != null ? schemaName.toLowerCase() : null,
                  tableName.toLowerCase(),
                  null)) {
            while (columns.next()) {
              String colName = columns.getString("COLUMN_NAME");
              String typeName = columns.getString("TYPE_NAME");
              tableSchema.put(dml.applyCasingLogic(colName, dml.columnCasing), typeName);
            }
          }
        }
      } catch (SQLException e) {
        if (retriesRemaining > 0) {
          int sleepSecs = (MAX_RETRIES - retriesRemaining + 1) * 10;
          try {
            Thread.sleep(sleepSecs * 1000);
            return getTableSchema(catalogName, schemaName, tableName, retriesRemaining - 1);
          } catch (InterruptedException i) {
          }
        }
      }
      return tableSchema;
    }

    @Override
    public Map<String, String> getObjectValue(List<String> key) {
      return getTableSchema(key.get(0), key.get(1), key.get(2), MAX_RETRIES);
    }
  }

  /**
   * The {@link JdbcPrimaryKeyCache} manages safely getting and setting JDBC Table PKs from a local
   * cache for each worker thread.
   */
  public static class JdbcPrimaryKeyCache extends MappedObjectCache<List<String>, List<String>> {

    private final DatastreamToDML dml;
    private static final int MAX_RETRIES = 5;

    public JdbcPrimaryKeyCache(DatastreamToDML dml) {
      this.dml = dml;
    }

    private List<String> getTablePrimaryKeys(
        String catalogName, String schemaName, String tableName, int retriesRemaining) {
      List<String> primaryKeys = new ArrayList<String>();
      DataSource ds = dml.getDataSource();
      if (ds == null) {
        return primaryKeys;
      }

      String effectiveCatalog = (catalogName == null || catalogName.isEmpty()) ? null : catalogName;

      try (Connection connection = getConnection(ds, MAX_RETRIES, MAX_RETRIES)) {
        if (connection == null) {
          return primaryKeys;
        }
        DatabaseMetaData metaData = connection.getMetaData();

        try (ResultSet jdbcPrimaryKeys =
            metaData.getPrimaryKeys(effectiveCatalog, schemaName, tableName)) {
          while (jdbcPrimaryKeys.next()) {
            primaryKeys.add(
                dml.applyCasingLogic(jdbcPrimaryKeys.getString("COLUMN_NAME"), dml.columnCasing));
          }
        }

        if (primaryKeys.isEmpty() && metaData.getDatabaseProductName().equals("PostgreSQL")) {
          try (ResultSet jdbcPrimaryKeys =
              metaData.getPrimaryKeys(
                  effectiveCatalog,
                  schemaName != null ? schemaName.toLowerCase() : null,
                  tableName.toLowerCase())) {
            while (jdbcPrimaryKeys.next()) {
              primaryKeys.add(
                  dml.applyCasingLogic(jdbcPrimaryKeys.getString("COLUMN_NAME"), dml.columnCasing));
            }
          }
        }
      } catch (SQLException e) {
        if (retriesRemaining > 0) {
          int sleepSecs = (MAX_RETRIES - retriesRemaining + 1) * 10;
          try {
            Thread.sleep(sleepSecs * 1000);
            return getTablePrimaryKeys(catalogName, schemaName, tableName, retriesRemaining - 1);
          } catch (InterruptedException i) {
          }
        }
      }
      return primaryKeys;
    }

    @Override
    public List<String> getObjectValue(List<String> key) {
      return getTablePrimaryKeys(key.get(0), key.get(1), key.get(2), MAX_RETRIES);
    }
  }
}
