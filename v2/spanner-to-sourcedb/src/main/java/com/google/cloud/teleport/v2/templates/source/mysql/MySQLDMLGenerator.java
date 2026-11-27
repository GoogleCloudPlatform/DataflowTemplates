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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;

/**
 * Creates parameterized DML statements for MySQL.
 *
 * <p>All column values (including custom transformation values and the source database timezone
 * offset) are bound as {@link java.sql.PreparedStatement} parameters. Only template-generated SQL
 * and escaped identifiers are part of the SQL text.
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
            MySQLDMLGenerator::getMappedColumnBinding);
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
            MySQLDMLGenerator::getMappedColumnBinding);
    columnBindings.putAll(pkColumnBindings);
    return getUpsertStatement(sourceTable.name(), columnBindings);
  }

  /** Quotes a MySQL identifier with backticks, escaping embedded backticks by doubling them. */
  @VisibleForTesting
  static String quoteIdentifier(String identifier) {
    return "`" + StringUtils.replace(identifier, "`", "``") + "`";
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

    BigDecimal decimalValue = null;
    Boolean booleanValue = null;
    String stringValue;
    if (isFloatOrNumeric(code)) {
      // TODO Test and Handle NAN/Infinity.
      decimalValue = valuesJson.getBigDecimal(colName);
      stringValue = decimalValue.toString();
    } else if (code.equals(Type.Code.BOOL) || code.equals(Type.Code.PG_BOOL)) {
      booleanValue = valuesJson.getBoolean(colName);
      stringValue = booleanValue.toString();
    } else if (isStringArray(colType)) {
      stringValue =
          valuesJson.getJSONArray(colName).toList().stream()
              .map(String::valueOf)
              .collect(Collectors.joining(","));
    } else {
      stringValue = valuesJson.getString(colName);
    }

    String sourceType = sourceColDef.type().toLowerCase(Locale.ROOT);
    switch (sourceType) {
      case "varchar":
      case "char":
      case "text":
      case "tinytext":
      case "mediumtext":
      case "longtext":
      case "enum":
      case "date":
      case "time":
      case "year":
      case "set":
      case "json":
      case "geometry":
      case "geometrycollection":
      case "point":
      case "multipoint":
      case "linestring":
      case "multilinestring":
      case "polygon":
      case "multipolygon":
      case "tinyblob":
      case "mediumblob":
      case "blob":
      case "longblob":
        return ColumnBinding.of(PreparedStatementValue.ofString(removeNullBytes(stringValue)));
      case "timestamp":
      case "datetime":
        // Trim the trailing 'Z' of the UTC timestamp for MySQL.
        String utcTimestamp = stringValue.substring(0, stringValue.length() - 1);
        return ColumnBinding.of(
            CONVERT_TZ_PLACEHOLDER,
            ImmutableList.of(
                PreparedStatementValue.ofString(removeNullBytes(utcTimestamp)),
                PreparedStatementValue.ofString(sourceDbTimezoneOffset)));
      case "binary":
      case "varbinary":
        return ColumnBinding.of(
            BINARY_PLACEHOLDER,
            ImmutableList.of(PreparedStatementValue.ofString(removeNullBytes(stringValue))));
      default:
        if (decimalValue != null) {
          return ColumnBinding.of(PreparedStatementValue.ofBigDecimal(decimalValue));
        }
        if (booleanValue != null) {
          return ColumnBinding.of(PreparedStatementValue.ofBoolean(booleanValue));
        }
        // MySQL implicitly converts the string to the column type (e.g. integer types).
        return ColumnBinding.of(PreparedStatementValue.ofString(stringValue));
    }
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
