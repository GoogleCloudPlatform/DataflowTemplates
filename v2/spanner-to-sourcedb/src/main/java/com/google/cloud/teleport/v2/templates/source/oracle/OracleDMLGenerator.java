/*
 * Copyright (C) 2026 Google LLC
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
package com.google.cloud.teleport.v2.templates.source.oracle;

import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.templates.dbutils.dml.DMLGeneratorUtils;
import com.google.cloud.teleport.v2.templates.dbutils.dml.IDMLGenerator;
import com.google.cloud.teleport.v2.templates.exceptions.InvalidDMLGenerationException;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.source.oracle.OracleJdbcDao.OracleBindParameter;
import com.google.common.annotations.VisibleForTesting;
import java.math.BigDecimal;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates parameterized DML statements for Oracle.
 *
 * <p>Column values (including {@code NULL}) are written as {@code ?} placeholders with bind values.
 * Records of a table with the same set of columns therefore produce the same SQL text, which
 * Oracle's implicit statement cache ({@link
 * OracleSpToSrcSourceConnector#IMPLICIT_STATEMENT_CACHE_PROP}) can reuse. Custom transformation
 * outputs that are SQL expressions (rather than plain literals) are written to the SQL text as is.
 */
public class OracleDMLGenerator implements IDMLGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(OracleDMLGenerator.class);

  private static final Pattern PLAIN_NUMERIC_LITERAL = Pattern.compile("^[+-]?\\d+(\\.\\d+)?$");

  /**
   * Spanner type passed to {@link #getColumnValueByType} for custom transformation values. Custom
   * transformations return SQL text, so their values are never Spanner BYTES or DATE values.
   */
  private static final String CUSTOM_TRANSFORMATION_SPANNER_TYPE = "STRING";

  /**
   * Numeric types, bound as {@link BigDecimal} so that the value does not depend on the session
   * decimal character ({@code NLS_NUMERIC_CHARACTERS}).
   */
  private static final Set<String> NUMERIC_TYPES =
      Set.of(
          "number",
          "numeric",
          "decimal",
          "float",
          "double precision",
          "integer",
          "smallint",
          "int",
          "boolean",
          "bool");

  /** Character types for which custom transformation string literals ({@code '...'}) are bound. */
  private static final Set<String> CHARACTER_TYPES =
      Set.of(
          "varchar",
          "varchar2",
          "character varying",
          "text",
          "char",
          "character",
          "clob",
          "nvarchar2",
          "nchar",
          "nchar varying",
          "national char",
          "national char varying",
          "national character",
          "national character varying",
          "nclob");

  @Override
  public DMLGeneratorResponse getDMLStatement(DMLGeneratorRequest dmlGeneratorRequest) {
    if (dmlGeneratorRequest == null) {
      throw new InvalidDMLGenerationException(
          "DMLGeneratorRequest is null. Cannot process the request.");
    }
    String spannerTableName = dmlGeneratorRequest.getSpannerTableName();
    ISchemaMapper schemaMapper = dmlGeneratorRequest.getSchemaMapper();
    Ddl spannerDdl = dmlGeneratorRequest.getSpannerDdl();
    SourceSchema sourceSchema = dmlGeneratorRequest.getSourceSchema();

    if (schemaMapper == null) {
      throw new InvalidDMLGenerationException("Schema Mapper must be not null");
    }
    if (spannerDdl == null) {
      throw new InvalidDMLGenerationException("Spanner Ddl must be not null.");
    }
    if (sourceSchema == null) {
      throw new InvalidDMLGenerationException("SourceSchema must be not null.");
    }

    Table spannerTable = spannerDdl.table(spannerTableName);
    if (spannerTable == null) {
      throw new InvalidDMLGenerationException(
          String.format(
              "The spanner table %s was not found in ddl found on spanner", spannerTableName));
    }

    String sourceTableName = "";
    try {
      sourceTableName = schemaMapper.getSourceTableName("", spannerTableName);
    } catch (NoSuchElementException e) {
      throw new InvalidDMLGenerationException(
          "Could not find source table name for spanner table: " + spannerTableName, e);
    }
    SourceTable sourceTable = sourceSchema.table(sourceTableName);
    if (sourceTable == null) {
      throw new InvalidDMLGenerationException(
          String.format(
              "Equivalent table %s was not found in source for spanner table %s",
              sourceTableName, spannerTableName));
    }

    if (sourceTable.primaryKeyColumns() == null || sourceTable.primaryKeyColumns().size() == 0) {
      throw new InvalidDMLGenerationException(
          String.format(
              "Cannot reverse replicate for source table %s without primary key, skipping the record.",
              sourceTableName));
    }

    ColumnBinder binder =
        new ColumnBinder(
            schemaMapper,
            spannerTable,
            sourceTable,
            dmlGeneratorRequest.getSourceDbTimezoneOffset(),
            dmlGeneratorRequest.getCustomTransformationResponse());

    Map<String, String> pkcolumnNameValues =
        DMLGeneratorUtils.getPkColumnValues(
            schemaMapper,
            spannerTable,
            sourceTable,
            dmlGeneratorRequest.getNewValuesJson(),
            dmlGeneratorRequest.getKeyValuesJson(),
            dmlGeneratorRequest.getSourceDbTimezoneOffset(),
            dmlGeneratorRequest.getCustomTransformationResponse(),
            binder::mapColumnValue,
            null);
    if (pkcolumnNameValues == null || pkcolumnNameValues.isEmpty()) {
      throw new InvalidDMLGenerationException(
          String.format(
              "Cannot reverse replicate for table %s without primary key, skipping the record",
              sourceTableName));
    }

    if ("INSERT".equals(dmlGeneratorRequest.getModType())
        || "UPDATE".equals(dmlGeneratorRequest.getModType())) {
      return generateUpsertStatement(
          spannerTable, sourceTable, dmlGeneratorRequest, pkcolumnNameValues, binder);

    } else if ("DELETE".equals(dmlGeneratorRequest.getModType())) {
      return getDeleteStatement(sourceTable.name(), pkcolumnNameValues, binder);
    } else {
      throw new InvalidDMLGenerationException(
          String.format(
              "Unsupported modType: %s for table %s",
              dmlGeneratorRequest.getModType(), spannerTableName));
    }
  }

  private static DMLGeneratorResponse getUpsertStatement(
      String tableName,
      Map<String, String> allColumnNameValues,
      Map<String, String> generatedColumnValues,
      List<String> primaryKeys,
      ColumnBinder binder) {

    Map<String, String> queryColumns = new LinkedHashMap<>(allColumnNameValues);
    if (generatedColumnValues != null) {
      queryColumns.putAll(generatedColumnValues);
    }

    List<Object> bindParams = new ArrayList<>();
    StringBuilder usingSelect = new StringBuilder("SELECT ");
    int index = 0;
    for (Map.Entry<String, String> entry : queryColumns.entrySet()) {
      String colName = entry.getKey();
      String sqlValue = binder.bindColumn(colName, entry.getValue(), bindParams);
      usingSelect.append(sqlValue).append(" AS \"").append(colName).append("\"");
      if (index + 1 < queryColumns.size()) {
        usingSelect.append(", ");
      }
      index++;
    }
    usingSelect.append(" FROM DUAL");

    String onClause =
        primaryKeys.stream()
            .map(k -> "t.\"" + k + "\" = s.\"" + k + "\"")
            .collect(Collectors.joining(" AND "));

    List<String> nonPkCols =
        allColumnNameValues.keySet().stream()
            .filter(k -> !primaryKeys.contains(k))
            .collect(Collectors.toList());

    StringBuilder mergeQuery = new StringBuilder();
    mergeQuery.append("MERGE INTO \"").append(tableName).append("\" t ");
    mergeQuery.append("USING (").append(usingSelect).append(") s ");
    mergeQuery.append("ON (").append(onClause).append(") ");

    if (!nonPkCols.isEmpty()) {
      String updateSet =
          nonPkCols.stream()
              .map(k -> "t.\"" + k + "\" = s.\"" + k + "\"")
              .collect(Collectors.joining(", "));
      mergeQuery.append("WHEN MATCHED THEN UPDATE SET ").append(updateSet).append(" ");
    }

    String insertCols =
        allColumnNameValues.keySet().stream()
            .map(k -> "\"" + k + "\"")
            .collect(Collectors.joining(", "));
    String insertVals =
        allColumnNameValues.keySet().stream()
            .map(k -> "s.\"" + k + "\"")
            .collect(Collectors.joining(", "));
    mergeQuery
        .append("WHEN NOT MATCHED THEN INSERT (")
        .append(insertCols)
        .append(") VALUES (")
        .append(insertVals)
        .append(")");

    DMLGeneratorResponse response = new DMLGeneratorResponse(mergeQuery.toString());
    response.setPreparedStatementParameters(bindParams);
    return response;
  }

  private static DMLGeneratorResponse getDeleteStatement(
      String tableName, Map<String, String> pkcolumnNameValues, ColumnBinder binder) {
    String deleteValues = "";
    List<Object> bindParams = new ArrayList<>();

    int index = 0;
    for (Map.Entry<String, String> entry : pkcolumnNameValues.entrySet()) {
      String colName = entry.getKey();
      String colValue = binder.bindColumn(colName, entry.getValue(), bindParams);

      deleteValues += " \"" + colName + "\" = " + colValue;
      if (index + 1 < pkcolumnNameValues.size()) {
        deleteValues += " AND ";
      }
      index++;
    }
    String returnVal = "DELETE FROM \"" + tableName + "\" WHERE " + deleteValues;

    DMLGeneratorResponse response = new DMLGeneratorResponse(returnVal);
    response.setPreparedStatementParameters(bindParams);
    return response;
  }

  private static DMLGeneratorResponse generateUpsertStatement(
      Table spannerTable,
      SourceTable sourceTable,
      DMLGeneratorRequest dmlGeneratorRequest,
      Map<String, String> pkcolumnNameValues,
      ColumnBinder binder) {
    Map<String, String> columnNameValues =
        DMLGeneratorUtils.getColumnValues(
            dmlGeneratorRequest.getSchemaMapper(),
            spannerTable,
            sourceTable,
            dmlGeneratorRequest.getNewValuesJson(),
            dmlGeneratorRequest.getKeyValuesJson(),
            dmlGeneratorRequest.getSourceDbTimezoneOffset(),
            dmlGeneratorRequest.getCustomTransformationResponse(),
            binder::mapColumnValue,
            null);

    Map<String, String> orderedColumnNameValues = new LinkedHashMap<>();
    orderedColumnNameValues.putAll(pkcolumnNameValues);
    orderedColumnNameValues.putAll(columnNameValues);

    Map<String, String> generatedColumnValues = new LinkedHashMap<>();
    for (SourceColumn col : sourceTable.columns()) {
      // The shared DMLGeneratorUtils explicitly skips generated (virtual) columns. Oracle
      // physically requires
      // these columns to be mapped separately so they can be injected solely into the USING
      // (SELECT...) block
      // of the MERGE statement. (E.g. in case a generated column is part of a Primary Key
      // referenced in the ON block).
      if (col.isGenerated()) {
        String spannerColName =
            dmlGeneratorRequest
                .getSchemaMapper()
                .getSpannerColumnName("", sourceTable.name(), col.name());
        Column spannerColDef = spannerTable.column(spannerColName);
        if (dmlGeneratorRequest.getKeyValuesJson().has(spannerColName)
            && !dmlGeneratorRequest.getKeyValuesJson().isNull(spannerColName)) {
          generatedColumnValues.put(
              col.name(),
              binder.mapColumnValue(
                  spannerColDef,
                  col,
                  dmlGeneratorRequest.getKeyValuesJson(),
                  dmlGeneratorRequest.getSourceDbTimezoneOffset(),
                  null));
        } else if (dmlGeneratorRequest.getNewValuesJson().has(spannerColName)
            && !dmlGeneratorRequest.getNewValuesJson().isNull(spannerColName)) {
          generatedColumnValues.put(
              col.name(),
              binder.mapColumnValue(
                  spannerColDef,
                  col,
                  dmlGeneratorRequest.getNewValuesJson(),
                  dmlGeneratorRequest.getSourceDbTimezoneOffset(),
                  null));
        }
      }
    }

    return getUpsertStatement(
        sourceTable.name(),
        orderedColumnNameValues,
        generatedColumnValues,
        sourceTable.primaryKeyColumns(),
        binder);
  }

  /**
   * Converts the column values of one DML request into SQL placeholder expressions and bind values.
   *
   * <p>{@link DMLGeneratorUtils} returns a {@code Map<column, SQL value>} where the SQL value is:
   *
   * <ul>
   *   <li>{@code null} for a {@code NULL} column value,
   *   <li>the placeholder returned by {@link #mapColumnValue} for any other column value, or
   *   <li>the output of the custom transformation for a custom transformed column.
   * </ul>
   *
   * <p>{@link #mapColumnValue} records each bind value by column name, and {@link #bindColumn}
   * appends the bind values in the order the columns are written to the SQL statement.
   */
  private static final class ColumnBinder {
    private final ISchemaMapper schemaMapper;
    private final Table spannerTable;
    private final SourceTable sourceTable;
    private final String sourceDbTimezoneOffset;
    private final Map<String, Object> customTransformationResponse;

    /** Bind values of the non-null columns, keyed by source column name. */
    private final Map<String, Object> bindValuesByColumn = new HashMap<>();

    ColumnBinder(
        ISchemaMapper schemaMapper,
        Table spannerTable,
        SourceTable sourceTable,
        String sourceDbTimezoneOffset,
        Map<String, Object> customTransformationResponse) {
      this.schemaMapper = schemaMapper;
      this.spannerTable = spannerTable;
      this.sourceTable = sourceTable;
      this.sourceDbTimezoneOffset = sourceDbTimezoneOffset;
      this.customTransformationResponse =
          (customTransformationResponse == null) ? Map.of() : customTransformationResponse;
    }

    /** {@link DMLGeneratorUtils.ColumnValueMapper} for non-null column values. */
    String mapColumnValue(
        Column spannerColDef,
        SourceColumn sourceColDef,
        JSONObject valuesJson,
        String sourceDbTimezoneOffset,
        List<Object> unusedPreparedStatementParameters) {
      bindValuesByColumn.put(
          sourceColDef.name(), toBindValue(spannerColDef, sourceColDef, valuesJson));
      return getColumnValueByType(
          sourceColDef.type(), sourceDbTimezoneOffset, spannerColDef.type().getCode().toString());
    }

    /**
     * Returns the SQL expression to write for {@code colName} and appends its bind value (if any)
     * to {@code bindParams}.
     */
    String bindColumn(String colName, String sqlValue, List<Object> bindParams) {
      SourceColumn sourceColDef = sourceTable.column(colName);
      String sourceType = (sourceColDef == null) ? "" : sourceColDef.type();

      if (sqlValue == null) {
        // NULL column value: same placeholder as non-null values, bound as a typed NULL.
        bindParams.add(
            new OracleBindParameter(getBindSqlType(normalizeSourceType(sourceType)), null));
        return getColumnValueByType(
            sourceType, sourceDbTimezoneOffset, getSpannerTypeCode(colName));
      }
      if (bindValuesByColumn.containsKey(colName)) {
        bindParams.add(bindValuesByColumn.get(colName));
        return sqlValue;
      }
      if (customTransformationResponse.containsKey(colName)) {
        return bindCustomTransformationValue(sourceType, sqlValue, bindParams);
      }
      throw new IllegalStateException("No bind value was recorded for column " + colName);
    }

    /**
     * Returns the SQL expression for a custom transformation output and appends its bind value (if
     * any) to {@code bindParams}.
     *
     * <p>As for the other JDBC sources, custom transformations return SQL expressions such as
     * {@code 'abc'}, {@code 123}, {@code HEXTORAW('...')} or {@code TO_DATE(...)}. Plain literals
     * are bound so that the statement can be cached and values larger than Oracle's 4000 byte
     * literal limit (ORA-01704) are supported; any other expression is written to the SQL statement
     * as is.
     */
    private String bindCustomTransformationValue(
        String rawSourceType, String sqlValue, List<Object> bindParams) {
      String sourceType = normalizeSourceType(rawSourceType);
      String literal = sqlValue.trim();
      Object bindValue =
          "null".equalsIgnoreCase(literal)
              ? new OracleBindParameter(getBindSqlType(sourceType), null)
              : toCustomTransformationBindValue(sourceType, literal);
      if (bindValue == null) {
        return sqlValue;
      }
      bindParams.add(bindValue);
      return getColumnValueByType(
          rawSourceType, sourceDbTimezoneOffset, CUSTOM_TRANSFORMATION_SPANNER_TYPE);
    }

    private String getSpannerTypeCode(String sourceColName) {
      try {
        String spannerColName =
            schemaMapper.getSpannerColumnName("", sourceTable.name(), sourceColName);
        Column spannerColDef =
            (spannerColName == null) ? null : spannerTable.column(spannerColName);
        if (spannerColDef != null) {
          return spannerColDef.type().getCode().toString();
        }
      } catch (NoSuchElementException e) {
        // Column is not mapped to Spanner.
      }
      return CUSTOM_TRANSFORMATION_SPANNER_TYPE;
    }
  }

  /**
   * Returns the bind value of a plain custom transformation literal, or {@code null} for any other
   * SQL expression.
   */
  private static Object toCustomTransformationBindValue(String sourceType, String literal) {
    int sqlType = getBindSqlType(sourceType);
    if (sqlType == Types.BLOB || sqlType == Types.VARBINARY) {
      byte[] bytes = parseHexToRawLiteral(literal);
      return (bytes == null) ? null : toTypedBindValue(sqlType, bytes);
    }
    if (NUMERIC_TYPES.contains(sourceType)) {
      String unquoted = unquoteStrictSqlStringLiteral(literal);
      String number = (unquoted == null) ? literal : unquoted.trim();
      return PLAIN_NUMERIC_LITERAL.matcher(number).matches() ? new BigDecimal(number) : null;
    }
    if (CHARACTER_TYPES.contains(sourceType)) {
      String unquoted = unquoteStrictSqlStringLiteral(literal);
      return (unquoted == null)
          ? null
          : toBindValueByType(sourceType, StringUtils.replace(unquoted, "\u0000", ""));
    }
    return null;
  }

  /** Returns the bytes of a {@code HEXTORAW('<hex>')} literal, or {@code null} otherwise. */
  private static byte[] parseHexToRawLiteral(String literal) {
    if (!StringUtils.startsWithIgnoreCase(literal, "HEXTORAW(") || !literal.endsWith(")")) {
      return null;
    }
    String hex = unquoteStrictSqlStringLiteral(literal.substring(9, literal.length() - 1));
    if (hex == null || hex.length() % 2 != 0) {
      return null;
    }
    try {
      return HexFormat.of().parseHex(hex);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /**
   * Unquotes a single SQL string literal ({@code '...'}) in which every {@code '} is escaped as
   * {@code ''}. Returns {@code null} for any other expression (e.g. {@code 'a' || 'b'}).
   */
  @VisibleForTesting
  static String unquoteStrictSqlStringLiteral(String s) {
    if (s == null) {
      return null;
    }
    String trimmed = s.trim();
    int len = trimmed.length();
    if (len < 2 || trimmed.charAt(0) != '\'' || trimmed.charAt(len - 1) != '\'') {
      return null;
    }
    StringBuilder sb = new StringBuilder(len - 2);
    for (int i = 1; i < len - 1; i++) {
      char c = trimmed.charAt(i);
      if (c == '\'') {
        if (i + 1 < len - 1 && trimmed.charAt(i + 1) == '\'') {
          i++;
        } else {
          return null;
        }
      }
      sb.append(c);
    }
    return sb.toString();
  }

  /** Returns the bind value for a non-null column value. */
  private static Object toBindValue(
      Column spannerColDef, SourceColumn sourceColDef, JSONObject valuesJson) {
    Type colType = spannerColDef.type();
    String colName = spannerColDef.name();
    String sourceType = normalizeSourceType(sourceColDef.type());

    if (isBytesType(colType.getCode().toString())) {
      return toBytesBindValue(sourceType, valuesJson.getString(colName));
    }
    String colInputValue = getColumnInputValue(colType, sourceType, valuesJson, colName);
    return toBindValueByType(sourceType, StringUtils.replace(colInputValue, "\u0000", ""));
  }

  /** Converts a non-BYTES Spanner value to its string form. */
  private static String getColumnInputValue(
      Type colType, String sourceType, JSONObject valuesJson, String colName) {
    if ("binary_float".equals(sourceType) || "binary_double".equals(sourceType)) {
      // Passed as text to TO_BINARY_FLOAT / TO_BINARY_DOUBLE, which also accept NaN and Infinity.
      return valuesJson.get(colName).toString();
    }
    if (colType.getCode().equals(Type.Code.FLOAT64)
        || colType.getCode().equals(Type.Code.FLOAT32)
        || colType.getCode().equals(Type.Code.PG_FLOAT4)
        || colType.getCode().equals(Type.Code.PG_FLOAT8)
        || colType.getCode().equals(Type.Code.PG_NUMERIC)) {
      return valuesJson.getBigDecimal(colName).toString();
    }
    if (colType.getCode().equals(Type.Code.BOOL) || colType.getCode().equals(Type.Code.PG_BOOL)) {
      return String.valueOf(valuesJson.getBoolean(colName));
    }
    if ((colType.getCode().equals(Type.Code.ARRAY)
            && colType.getArrayElementType().getCode().equals(Type.Code.STRING))
        || (colType.getCode().equals(Type.Code.PG_ARRAY)
            && (colType.getArrayElementType().getCode().equals(Type.Code.PG_VARCHAR)
                || colType.getArrayElementType().getCode().equals(Type.Code.PG_TEXT)))) {
      return valuesJson.getJSONArray(colName).toList().stream()
          .map(String::valueOf)
          .collect(Collectors.joining(","));
    }
    return valuesJson.getString(colName);
  }

  /** Returns the bind value for a Base64 encoded BYTES value. */
  private static Object toBytesBindValue(String sourceType, String base64Value) {
    switch (sourceType) {
      case "bytea":
      case "blob":
        return toTypedBindValue(Types.BLOB, decodeBase64(base64Value));
      case "raw":
      case "long raw":
      case "binary":
      case "varbinary":
        return toTypedBindValue(Types.VARBINARY, decodeBase64(base64Value));
      default:
        // Character columns: bound as hex text into HEXTORAW(?) (see getColumnValueByType).
        String rawHex = DMLGeneratorUtils.convertBase64ToRawHex(base64Value);
        return StringUtils.isEmpty(rawHex) ? new OracleBindParameter(Types.VARCHAR, null) : rawHex;
    }
  }

  private static byte[] decodeBase64(String base64Value) {
    return StringUtils.isEmpty(base64Value) ? new byte[0] : Base64.getDecoder().decode(base64Value);
  }

  /**
   * Returns the bind value for a non-null, non-BYTES column value:
   *
   * <ul>
   *   <li>character and LOB types are bound with their JDBC type,
   *   <li>numeric types are bound as {@link BigDecimal},
   *   <li>all other types are bound as text.
   * </ul>
   */
  private static Object toBindValueByType(String columnType, String colValue) {
    if (NUMERIC_TYPES.contains(columnType)) {
      return toNumberBindValue(colValue);
    }
    switch (columnType) {
      case "nvarchar2":
      case "nchar varying":
      case "national char varying":
      case "national character varying":
        return toTypedBindValue(Types.NVARCHAR, colValue);
      case "nchar":
      case "national char":
      case "national character":
        // Fixed width type: bound with setFixedCHAR for blank-padded comparison.
        return toTypedBindValue(Types.NCHAR, colValue);
      case "nclob":
        return toTypedBindValue(Types.NCLOB, colValue);
      case "clob":
        return toTypedBindValue(Types.CLOB, colValue);
      case "char":
      case "character":
        // Fixed width type: bound with setFixedCHAR for blank-padded comparison.
        return toTypedBindValue(Types.CHAR, colValue);
      case "varchar":
      case "varchar2":
      case "text":
      case "character varying":
      case "json":
      case "jsonb":
      case "time":
      case "uuid":
      case "date":
      case "timestamp":
      case "timestamp without time zone":
      case "timestamp with time zone":
      case "timestamp with local time zone":
      case "timestamptz":
      case "bytea":
      case "blob":
      case "raw":
      case "binary":
      case "varbinary":
      case "binary_float":
      case "binary_double":
        return colValue;
      default:
        // Other types: Spanner booleans become 1 / 0.
        return toNumericBoolean(colValue);
    }
  }

  /**
   * Returns the value as a {@link BigDecimal}, or the text itself if it is not a number (Oracle
   * then reports the conversion error).
   */
  private static Object toNumberBindValue(String colValue) {
    String number = toNumericBoolean(colValue.trim());
    try {
      return new BigDecimal(number);
    } catch (NumberFormatException e) {
      return colValue;
    }
  }

  /** Converts Spanner booleans ({@code true} / {@code false}) to {@code 1} / {@code 0}. */
  private static String toNumericBoolean(String colValue) {
    if ("true".equalsIgnoreCase(colValue)) {
      return "1";
    } else if ("false".equalsIgnoreCase(colValue)) {
      return "0";
    }
    return colValue;
  }

  /** Returns the JDBC type used to bind values (including {@code NULL}) of the Oracle type. */
  private static int getBindSqlType(String columnType) {
    return switch (columnType) {
      case "char", "character" -> Types.CHAR;
      case "nchar", "national char", "national character" -> Types.NCHAR;
      case "nvarchar2",
          "nchar varying",
          "national char varying",
          "national character varying" -> Types.NVARCHAR;
      case "clob" -> Types.CLOB;
      case "nclob" -> Types.NCLOB;
      case "bytea", "blob" -> Types.BLOB;
      case "raw", "long raw", "binary", "varbinary" -> Types.VARBINARY;
      default -> Types.VARCHAR;
    };
  }

  /**
   * Returns a typed bind value. Empty values are bound as typed {@code NULL} because Oracle stores
   * empty strings and empty RAW values as {@code NULL}.
   */
  private static OracleBindParameter toTypedBindValue(int sqlType, Object value) {
    boolean isEmpty =
        (value instanceof String str && str.isEmpty())
            || (value instanceof byte[] bytes && bytes.length == 0);
    return new OracleBindParameter(sqlType, isEmpty ? null : value);
  }

  private static boolean isBytesType(String spannerColType) {
    return "BYTES".equalsIgnoreCase(spannerColType) || "PG_BYTEA".equalsIgnoreCase(spannerColType);
  }

  /** Lower-cased Oracle type without precision/length, e.g. "TIMESTAMP(6)" -> "timestamp". */
  @VisibleForTesting
  static String normalizeSourceType(String columnType) {
    if (columnType == null) {
      return "";
    }
    return columnType.replaceAll("\\([^)]*\\)", "").trim().replaceAll("\\s+", " ").toLowerCase();
  }

  /**
   * Returns the SQL expression, with a {@code ?} placeholder for the bind value, used to write a
   * value to a column of the given Oracle type.
   */
  @VisibleForTesting
  static String getColumnValueByType(
      String columnType, String sourceDbTimezoneOffset, String spannerColType) {
    switch (normalizeSourceType(columnType)) {
      case "nvarchar2":
      case "nclob":
      case "nchar":
      case "nchar varying":
      case "national char":
      case "national char varying":
      case "national character":
      case "national character varying":
        return isBytesType(spannerColType)
            ? "TO_NCLOB(UTL_RAW.CAST_TO_NVARCHAR2(HEXTORAW(?)))"
            : "?";
      case "varchar":
      case "varchar2":
      case "clob":
      case "char":
      case "text":
      case "character varying":
      case "character":
      case "json":
      case "jsonb":
      case "time":
      case "uuid":
        return isBytesType(spannerColType) ? "HEXTORAW(?)" : "?";
      case "date":
        if ("DATE".equalsIgnoreCase(spannerColType) || "PG_DATE".equalsIgnoreCase(spannerColType)) {
          return "TO_DATE(?, 'YYYY-MM-DD')";
        }
        return "FROM_TZ(TO_TIMESTAMP(?, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF\"Z\"'), 'UTC')";
      case "timestamp with time zone":
      case "timestamp with local time zone":
      case "timestamptz":
        // Time zone aware types store the exact instant, so the source offset is not applied.
        return "FROM_TZ(TO_TIMESTAMP(?, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF\"Z\"'), 'UTC')";
      case "timestamp":
      case "timestamp without time zone":
        if (sourceDbTimezoneOffset != null
            && !sourceDbTimezoneOffset.isEmpty()
            && !"+00:00".equals(sourceDbTimezoneOffset)
            && !"Z".equalsIgnoreCase(sourceDbTimezoneOffset)) {
          return "CAST(FROM_TZ(TO_TIMESTAMP(?, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF\"Z\"'), 'UTC') AT TIME ZONE '"
              + sourceDbTimezoneOffset
              + "' AS TIMESTAMP)";
        }
        return "FROM_TZ(TO_TIMESTAMP(?, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF\"Z\"'), 'UTC')";
      case "bytea":
      case "blob":
      case "raw":
      case "binary":
      case "varbinary":
        return "?";
      case "binary_float":
        return "TO_BINARY_FLOAT(?)";
      case "binary_double":
        return "TO_BINARY_DOUBLE(?)";
      case "number":
      case "numeric":
      case "decimal":
      case "float":
      case "double precision":
      case "integer":
      case "smallint":
      case "int":
      case "boolean":
      case "bool":
        return "TO_NUMBER(?)";
      default:
        return "?";
    }
  }
}
