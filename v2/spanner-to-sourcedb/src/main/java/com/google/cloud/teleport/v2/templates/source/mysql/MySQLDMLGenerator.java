/*
 * Copyright (C) 2024 Google LLC
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
package com.google.cloud.teleport.v2.templates.source.mysql;

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
import com.google.cloud.teleport.v2.templates.models.ColumnBinding;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.models.PreparedStatementValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.math.BigDecimal;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

/**
 * Creates parameterized DML statements for MySQL.
 *
 * <p>All column values (including custom transformation values and the source database timezone
 * offset) are bound as {@link java.sql.PreparedStatement} parameters. Only template-generated SQL
 * and escaped identifiers are part of the SQL text.
 *
 * <p>Values keep the semantics of the SQL literals generated before parameter binding was
 * introduced:
 *
 * <ul>
 *   <li>Character, temporal and binary columns receive strings (quoted literals previously), and
 *       Spanner {@code BYTES} values are bound as the decoded bytes.
 *   <li>All other columns (numeric, {@code bit}, {@code bool} and unknown types) previously
 *       received unquoted literals, which MySQL evaluates as numbers / booleans. Their values are
 *       therefore bound as numbers ({@code Long}, {@code BigDecimal}, {@code Double}) or booleans
 *       when the value is a numeric / boolean literal, see {@link #toNumericLiteralValue(String)}.
 *       This matters for {@code bit} columns, where a quoted {@code '5'} would be stored as the
 *       byte {@code 0x35}.
 * </ul>
 *
 * <p>Custom transformation values are bound as returned ({@code byte[]} as bytes, {@code Boolean},
 * {@code Long}, {@code Double}, {@code BigDecimal} with their type), except that strings returned
 * for numeric / {@code bit} / {@code bool} columns are bound as numbers or booleans when they are
 * numeric / boolean literals. Custom values are never interpreted as SQL and no timezone conversion
 * is applied to them.
 */
public class MySQLDMLGenerator implements IDMLGenerator {

  /** SQL fragment converting a UTC timestamp to the source database timezone. */
  @VisibleForTesting static final String CONVERT_TZ_PLACEHOLDER = "CONVERT_TZ(?, '+00:00', ?)";

  /** SQL fragment casting a string to a binary string. */
  @VisibleForTesting static final String BINARY_PLACEHOLDER = "BINARY(?)";

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
              "Cannot reverse replicate for source table %s without primary key, skipping the"
                  + " record.",
              sourceTableName));
    }

    Map<String, ColumnBinding> pkColumnBindings =
        DMLGeneratorUtils.getPkColumnBindings(
            schemaMapper,
            spannerTable,
            sourceTable,
            dmlGeneratorRequest.getNewValuesJson(),
            dmlGeneratorRequest.getKeyValuesJson(),
            dmlGeneratorRequest.getSourceDbTimezoneOffset(),
            dmlGeneratorRequest.getCustomTransformationResponse(),
            MySQLDMLGenerator::getMappedColumnBinding,
            MySQLDMLGenerator::getCustomValueBinding);
    if (pkColumnBindings == null || pkColumnBindings.isEmpty()) {
      throw new InvalidDMLGenerationException(
          String.format(
              "Cannot reverse replicate for table %s without primary key, skipping the record",
              sourceTableName));
    }

    if ("INSERT".equals(dmlGeneratorRequest.getModType())
        || "UPDATE".equals(dmlGeneratorRequest.getModType())) {
      return generateUpsertStatement(
          spannerTable, sourceTable, dmlGeneratorRequest, pkColumnBindings);

    } else if ("DELETE".equals(dmlGeneratorRequest.getModType())) {
      return getDeleteStatement(sourceTable.name(), pkColumnBindings);
    } else {
      throw new InvalidDMLGenerationException(
          String.format(
              "Unsupported modType: %s for table %s",
              dmlGeneratorRequest.getModType(), spannerTableName));
    }
  }

  /**
   * Builds {@code INSERT INTO `t`(`c1`,...) VALUES (?,...) ON DUPLICATE KEY UPDATE `c1` = ?, ...}.
   * The value parameters are bound twice: once for the {@code VALUES} clause and once for the
   * {@code ON DUPLICATE KEY UPDATE} clause.
   */
  private static DMLGeneratorResponse getUpsertStatement(
      String tableName, Map<String, ColumnBinding> allColumnBindings) {
    List<String> columns = new ArrayList<>();
    List<String> values = new ArrayList<>();
    List<String> updates = new ArrayList<>();
    List<PreparedStatementValue> valueParams = new ArrayList<>();

    for (Map.Entry<String, ColumnBinding> entry : allColumnBindings.entrySet()) {
      String quotedColumn = quoteIdentifier(entry.getKey());
      ColumnBinding binding = entry.getValue();
      columns.add(quotedColumn);
      values.add(binding.getPlaceholderSql());
      updates.add(quotedColumn + " = " + binding.getPlaceholderSql());
      valueParams.addAll(binding.getParams());
    }

    String sql =
        "INSERT INTO "
            + quoteIdentifier(tableName)
            + "("
            + String.join(",", columns)
            + ") VALUES ("
            + String.join(",", values)
            + ") ON DUPLICATE KEY UPDATE "
            + String.join(", ", updates);

    List<Object> params = new ArrayList<>(valueParams.size() * 2);
    params.addAll(valueParams); // VALUES (...)
    params.addAll(valueParams); // ON DUPLICATE KEY UPDATE ...
    return newResponse(sql, params);
  }

  /** Builds {@code DELETE FROM `t` WHERE `pk1` = ? AND ...}. */
  private static DMLGeneratorResponse getDeleteStatement(
      String tableName, Map<String, ColumnBinding> pkColumnBindings) {
    List<String> conditions = new ArrayList<>();
    List<Object> params = new ArrayList<>();
    for (Map.Entry<String, ColumnBinding> entry : pkColumnBindings.entrySet()) {
      ColumnBinding binding = entry.getValue();
      conditions.add(quoteIdentifier(entry.getKey()) + " = " + binding.getPlaceholderSql());
      params.addAll(binding.getParams());
    }
    String sql =
        "DELETE FROM " + quoteIdentifier(tableName) + " WHERE " + String.join(" AND ", conditions);
    return newResponse(sql, params);
  }

  private static DMLGeneratorResponse newResponse(String sql, List<Object> params) {
    DMLGeneratorResponse response = new DMLGeneratorResponse(sql);
    response.setPreparedStatementParameters(params);
    return response;
  }

  private static DMLGeneratorResponse generateUpsertStatement(
      Table spannerTable,
      SourceTable sourceTable,
      DMLGeneratorRequest dmlGeneratorRequest,
      Map<String, ColumnBinding> pkColumnBindings) {
    Map<String, ColumnBinding> columnBindings =
        DMLGeneratorUtils.getColumnBindings(
            dmlGeneratorRequest.getSchemaMapper(),
            spannerTable,
            sourceTable,
            dmlGeneratorRequest.getNewValuesJson(),
            dmlGeneratorRequest.getKeyValuesJson(),
            dmlGeneratorRequest.getSourceDbTimezoneOffset(),
            dmlGeneratorRequest.getCustomTransformationResponse(),
            MySQLDMLGenerator::getMappedColumnBinding,
            MySQLDMLGenerator::getCustomValueBinding);
    columnBindings.putAll(pkColumnBindings);
    return getUpsertStatement(sourceTable.name(), columnBindings);
  }

  /** Quotes a MySQL identifier with backticks, escaping embedded backticks by doubling them. */
  @VisibleForTesting
  static String quoteIdentifier(String identifier) {
    return "`" + StringUtils.replace(identifier, "`", "``") + "`";
  }

  /** Character types (and types written as text), previously written as quoted literals. */
  private static final ImmutableSet<String> TEXT_TYPES =
      ImmutableSet.of(
          "varchar",
          "char",
          "text",
          "tinytext",
          "mediumtext",
          "longtext",
          "enum",
          "date",
          "time",
          "year",
          "set",
          "json",
          "geometry",
          "geometrycollection",
          "point",
          "multipoint",
          "linestring",
          "multilinestring",
          "polygon",
          "multipolygon",
          "tinyblob",
          "mediumblob",
          "blob",
          "longblob");

  /** Types written through {@link #CONVERT_TZ_PLACEHOLDER}. */
  private static final ImmutableSet<String> TIMESTAMP_TYPES =
      ImmutableSet.of("timestamp", "datetime");

  /** Types written through {@link #BINARY_PLACEHOLDER}. */
  private static final ImmutableSet<String> BINARY_TYPES = ImmutableSet.of("binary", "varbinary");

  private static final Pattern INTEGER_LITERAL = Pattern.compile("[+-]?\\d+");
  private static final Pattern DECIMAL_LITERAL = Pattern.compile("[+-]?(\\d+\\.\\d*|\\.\\d+)");
  private static final Pattern SCIENTIFIC_LITERAL =
      Pattern.compile("[+-]?(\\d+(\\.\\d*)?|\\.\\d+)[eE][+-]?\\d+");

  /**
   * Returns true for MySQL types that previously received unquoted literals: numeric, {@code bit},
   * {@code bool} and unknown types.
   */
  @VisibleForTesting
  static boolean isNumericLiteralType(String sourceType) {
    String type = normalizeSourceType(sourceType);
    return !TEXT_TYPES.contains(type)
        && !TIMESTAMP_TYPES.contains(type)
        && !BINARY_TYPES.contains(type);
  }

  private static String normalizeSourceType(String sourceType) {
    return sourceType == null ? "" : sourceType.toLowerCase(Locale.ROOT);
  }

  /**
   * Maps a non-null Spanner column value to a {@link ColumnBinding} based on the Spanner column
   * type and the MySQL source column type.
   */
  @VisibleForTesting
  static ColumnBinding getMappedColumnBinding(
      Column spannerColDef,
      SourceColumn sourceColDef,
      JSONObject valuesJson,
      String sourceDbTimezoneOffset) {
    Type colType = spannerColDef.type();
    String colName = spannerColDef.name();
    Type.Code code = colType.getCode();

    // Spanner BYTES values are Base64 encoded in the change stream. Bind the decoded bytes for all
    // source types (blob, binary, varbinary, bit, and string types).
    if (code.equals(Type.Code.BYTES) || code.equals(Type.Code.PG_BYTEA)) {
      byte[] bytes = Base64.getDecoder().decode(valuesJson.getString(colName));
      return ColumnBinding.of(PreparedStatementValue.ofBytes(bytes));
    }

    String sourceType = normalizeSourceType(sourceColDef.type());
    if (isNumericLiteralType(sourceType)) {
      return ColumnBinding.of(getNumericLiteralTypeValue(colType, valuesJson, colName));
    }

    String stringValue = getStringValue(colType, valuesJson, colName);
    if (TIMESTAMP_TYPES.contains(sourceType)) {
      // Trim the trailing 'Z' of the UTC timestamp for MySQL.
      String utcTimestamp = stringValue.substring(0, stringValue.length() - 1);
      return ColumnBinding.of(
          CONVERT_TZ_PLACEHOLDER,
          ImmutableList.of(
              PreparedStatementValue.ofString(removeNullBytes(utcTimestamp)),
              PreparedStatementValue.ofString(sourceDbTimezoneOffset)));
    }
    if (BINARY_TYPES.contains(sourceType)) {
      return ColumnBinding.of(
          BINARY_PLACEHOLDER,
          ImmutableList.of(PreparedStatementValue.ofString(removeNullBytes(stringValue))));
    }
    return ColumnBinding.of(PreparedStatementValue.ofString(removeNullBytes(stringValue)));
  }

  /**
   * Maps a custom transformation value of a source column to a {@link ColumnBinding}. Values are
   * bound as data with their Java type, except that strings for numeric / {@code bit} / {@code
   * bool} columns are bound as numbers or booleans when they are numeric / boolean literals.
   */
  @VisibleForTesting
  static ColumnBinding getCustomValueBinding(SourceColumn sourceColDef, Object customValue) {
    if (customValue == null) {
      return ColumnBinding.of(PreparedStatementValue.nullValue(Types.NULL));
    }
    if (customValue instanceof String && isNumericLiteralType(sourceColDef.type())) {
      return ColumnBinding.of(toNumericLiteralValue((String) customValue));
    }
    return ColumnBinding.of(PreparedStatementValue.fromObject(customValue));
  }

  /** String form of a non-BYTES Spanner value, as written to character / temporal columns. */
  private static String getStringValue(Type colType, JSONObject valuesJson, String colName) {
    Type.Code code = colType.getCode();
    if (isFloatOrNumeric(code)) {
      // TODO Test and Handle NAN/Infinity.
      return valuesJson.getBigDecimal(colName).toString();
    }
    if (code.equals(Type.Code.BOOL) || code.equals(Type.Code.PG_BOOL)) {
      return Boolean.toString(valuesJson.getBoolean(colName));
    }
    if (isStringArray(colType)) {
      return valuesJson.getJSONArray(colName).toList().stream()
          .map(String::valueOf)
          .collect(Collectors.joining(","));
    }
    return valuesJson.getString(colName);
  }

  /**
   * Value of a non-BYTES Spanner value for a column that previously received an unquoted literal
   * (numeric, {@code bit}, {@code bool} and unknown types).
   */
  private static PreparedStatementValue getNumericLiteralTypeValue(
      Type colType, JSONObject valuesJson, String colName) {
    Type.Code code = colType.getCode();
    if (code.equals(Type.Code.FLOAT64)
        || code.equals(Type.Code.FLOAT32)
        || code.equals(Type.Code.PG_FLOAT4)
        || code.equals(Type.Code.PG_FLOAT8)) {
      // Bound as a double (not a BigDecimal, which the driver writes in plain notation) so that
      // very large / small magnitudes such as 1.7976931348623157E308 keep their exponent.
      // TODO Test and Handle NAN/Infinity.
      return PreparedStatementValue.ofDouble(valuesJson.getBigDecimal(colName).doubleValue());
    }
    if (code.equals(Type.Code.PG_NUMERIC)) {
      return PreparedStatementValue.ofBigDecimal(valuesJson.getBigDecimal(colName));
    }
    if (code.equals(Type.Code.BOOL) || code.equals(Type.Code.PG_BOOL)) {
      return PreparedStatementValue.ofBoolean(valuesJson.getBoolean(colName));
    }
    String stringValue = getStringValue(colType, valuesJson, colName);
    if (code.equals(Type.Code.INT64) || code.equals(Type.Code.PG_INT8)) {
      try {
        return PreparedStatementValue.ofLong(Long.parseLong(stringValue.trim()));
      } catch (NumberFormatException e) {
        // Fall through to the generic literal handling below.
      }
    }
    return toNumericLiteralValue(stringValue);
  }

  /**
   * Binds a string written to a numeric / {@code bit} / {@code bool} column the way MySQL evaluated
   * the unquoted literal previously generated for it: integers and plain decimals as {@link
   * BigDecimal}, scientific notation as {@link Double}, {@code true} / {@code false} as {@link
   * Boolean}. Any other string is bound as a string (it is never interpreted as SQL).
   */
  @VisibleForTesting
  static PreparedStatementValue toNumericLiteralValue(String value) {
    String trimmed = value.trim();
    if (INTEGER_LITERAL.matcher(trimmed).matches() || DECIMAL_LITERAL.matcher(trimmed).matches()) {
      return PreparedStatementValue.ofBigDecimal(new BigDecimal(trimmed));
    }
    if (SCIENTIFIC_LITERAL.matcher(trimmed).matches()) {
      double d = Double.parseDouble(trimmed);
      if (Double.isFinite(d)) {
        return PreparedStatementValue.ofDouble(d);
      }
    }
    if ("true".equalsIgnoreCase(trimmed)) {
      return PreparedStatementValue.ofBoolean(true);
    }
    if ("false".equalsIgnoreCase(trimmed)) {
      return PreparedStatementValue.ofBoolean(false);
    }
    return PreparedStatementValue.ofString(value);
  }

  private static boolean isFloatOrNumeric(Type.Code code) {
    return code.equals(Type.Code.FLOAT64)
        || code.equals(Type.Code.FLOAT32)
        || code.equals(Type.Code.PG_FLOAT4)
        || code.equals(Type.Code.PG_FLOAT8)
        || code.equals(Type.Code.PG_NUMERIC);
  }

  private static boolean isStringArray(Type colType) {
    return (colType.getCode().equals(Type.Code.ARRAY)
            && colType.getArrayElementType().getCode().equals(Type.Code.STRING))
        || (colType.getCode().equals(Type.Code.PG_ARRAY)
            && (colType.getArrayElementType().getCode().equals(Type.Code.PG_VARCHAR)
                || colType.getArrayElementType().getCode().equals(Type.Code.PG_TEXT)));
  }

  private static String removeNullBytes(String input) {
    return StringUtils.replace(input, "\u0000", "");
  }
}
