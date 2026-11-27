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
package com.google.cloud.teleport.v2.templates.dbutils.dml;

import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable;
import com.google.cloud.teleport.v2.templates.models.ColumnBinding;
import com.google.cloud.teleport.v2.templates.models.PreparedStatementValue;
import java.sql.Types;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utilities for generating DML statements securely and uniformly. */
public class DMLGeneratorUtils {
  private static final Logger LOG = LoggerFactory.getLogger(DMLGeneratorUtils.class);

  @FunctionalInterface
  public interface ColumnValueMapper {
    String getMappedColumnValue(
        Column spannerColDef,
        SourceColumn sourceColDef,
        JSONObject valuesJson,
        String sourceDbTimezoneOffset,
        List<Object> preparedStatementParameters);
  }

  /** Maps a non-null Spanner column value to a {@link ColumnBinding} for a parameterized DML. */
  @FunctionalInterface
  public interface ColumnBindingMapper {
    ColumnBinding getMappedColumnBinding(
        Column spannerColDef,
        SourceColumn sourceColDef,
        JSONObject valuesJson,
        String sourceDbTimezoneOffset);
  }

  /**
   * Maps a custom transformation value (possibly {@code null}) for a source column to a {@link
   * ColumnBinding}. The value must always be bound as data, never interpreted as SQL.
   */
  @FunctionalInterface
  public interface CustomValueBindingMapper {
    ColumnBinding getCustomValueBinding(SourceColumn sourceColDef, Object customValue);
  }

  /** Generic per-column value mapper used by the shared column iteration logic. */
  @FunctionalInterface
  private interface GenericColumnMapper<T> {
    T map(
        Column spannerColDef,
        SourceColumn sourceColDef,
        JSONObject valuesJson,
        String sourceDbTimezoneOffset);
  }

  public static String convertBase64ToRawHex(String base64EncodedString) {
    if (base64EncodedString == null) {
      return null;
    }
    if (StringUtils.isEmpty(base64EncodedString)) {
      return "";
    }
    try {
      byte[] decodedBytes = Base64.getDecoder().decode(base64EncodedString);
      StringBuilder hexStringBuilder = new StringBuilder(decodedBytes.length * 2);
      for (byte b : decodedBytes) {
        hexStringBuilder.append(String.format("%02x", b & 0xFF));
      }
      return hexStringBuilder.toString();
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "Invalid Base64 encoded string provided: " + e.getMessage(), e);
    }
  }

  public static Map<String, String> getColumnValues(
      ISchemaMapper schemaMapper,
      Table spannerTable,
      SourceTable sourceTable,
      JSONObject newValuesJson,
      JSONObject keyValuesJson,
      String sourceDbTimezoneOffset,
      Map<String, Object> customTransformationResponse,
      ColumnValueMapper columnValueMapper,
      List<Object> preparedStatementParameters) {
    return getColumnValuesInternal(
        schemaMapper,
        spannerTable,
        sourceTable,
        newValuesJson,
        keyValuesJson,
        sourceDbTimezoneOffset,
        customTransformationResponse,
        toGenericMapper(columnValueMapper, preparedStatementParameters),
        (sourceColDef, customValue) -> customValue.toString(),
        () -> null);
  }

  public static Map<String, String> getPkColumnValues(
      ISchemaMapper schemaMapper,
      Table spannerTable,
      SourceTable sourceTable,
      JSONObject newValuesJson,
      JSONObject keyValuesJson,
      String sourceDbTimezoneOffset,
      Map<String, Object> customTransformationResponse,
      ColumnValueMapper columnValueMapper,
      List<Object> preparedStatementParameters) {
    return getPkColumnValuesInternal(
        schemaMapper,
        spannerTable,
        sourceTable,
        newValuesJson,
        keyValuesJson,
        sourceDbTimezoneOffset,
        customTransformationResponse,
        toGenericMapper(columnValueMapper, preparedStatementParameters),
        (sourceColDef, customValue) -> customValue.toString(),
        () -> null);
  }

  /**
   * Returns the {@link ColumnBinding} for each non-primary-key source column, in source column
   * order. Custom transformation values are mapped by {@code customValueBindingMapper} and {@code
   * null} column values are bound as SQL {@code NULL}; neither is ever interpreted as SQL.
   */
  public static Map<String, ColumnBinding> getColumnBindings(
      ISchemaMapper schemaMapper,
      Table spannerTable,
      SourceTable sourceTable,
      JSONObject newValuesJson,
      JSONObject keyValuesJson,
      String sourceDbTimezoneOffset,
      Map<String, Object> customTransformationResponse,
      ColumnBindingMapper columnBindingMapper,
      CustomValueBindingMapper customValueBindingMapper) {
    return getColumnValuesInternal(
        schemaMapper,
        spannerTable,
        sourceTable,
        newValuesJson,
        keyValuesJson,
        sourceDbTimezoneOffset,
        customTransformationResponse,
        columnBindingMapper::getMappedColumnBinding,
        customValueBindingMapper::getCustomValueBinding,
        DMLGeneratorUtils::nullBinding);
  }

  /**
   * Returns the {@link ColumnBinding} for each primary key source column (plus all non-primary-key
   * columns if the primary key contains a generated column), or {@code null} if a primary key
   * column could not be resolved.
   */
  public static Map<String, ColumnBinding> getPkColumnBindings(
      ISchemaMapper schemaMapper,
      Table spannerTable,
      SourceTable sourceTable,
      JSONObject newValuesJson,
      JSONObject keyValuesJson,
      String sourceDbTimezoneOffset,
      Map<String, Object> customTransformationResponse,
      ColumnBindingMapper columnBindingMapper,
      CustomValueBindingMapper customValueBindingMapper) {
    return getPkColumnValuesInternal(
        schemaMapper,
        spannerTable,
        sourceTable,
        newValuesJson,
        keyValuesJson,
        sourceDbTimezoneOffset,
        customTransformationResponse,
        columnBindingMapper::getMappedColumnBinding,
        customValueBindingMapper::getCustomValueBinding,
        DMLGeneratorUtils::nullBinding);
  }

  private static ColumnBinding nullBinding() {
    return ColumnBinding.of(PreparedStatementValue.nullValue(Types.NULL));
  }

  private static GenericColumnMapper<String> toGenericMapper(
      ColumnValueMapper columnValueMapper, List<Object> preparedStatementParameters) {
    return (spannerColDef, sourceColDef, valuesJson, sourceDbTimezoneOffset) ->
        columnValueMapper.getMappedColumnValue(
            spannerColDef,
            sourceColDef,
            valuesJson,
            sourceDbTimezoneOffset,
            preparedStatementParameters);
  }

  /**
   * Shared iteration over non-primary-key source columns.
   *
   * @param customValueMapper maps a custom transformation value of a source column to {@code T}.
   * @param nullValueSupplier supplies the {@code T} used when the record value is {@code null}.
   */
  private static <T> Map<String, T> getColumnValuesInternal(
      ISchemaMapper schemaMapper,
      Table spannerTable,
      SourceTable sourceTable,
      JSONObject newValuesJson,
      JSONObject keyValuesJson,
      String sourceDbTimezoneOffset,
      Map<String, Object> customTransformationResponse,
      GenericColumnMapper<T> columnMapper,
      BiFunction<SourceColumn, Object, T> customValueMapper,
      Supplier<T> nullValueSupplier) {
    Map<String, T> response = new LinkedHashMap<>();

    List<String> sourcePKs = sourceTable.primaryKeyColumns();
    Set<String> customTransformColumns = null;
    if (customTransformationResponse != null) {
      customTransformColumns = customTransformationResponse.keySet();
    }
    for (SourceColumn sourceColDef : sourceTable.columns()) {
      String colName = sourceColDef.name();
      if (sourcePKs.contains(colName)) {
        continue; // we only need non-primary keys
      }
      if (sourceColDef.isGenerated()) {
        continue;
      }
      if (customTransformColumns != null && customTransformColumns.contains(colName)) {
        response.put(
            colName,
            customValueMapper.apply(sourceColDef, customTransformationResponse.get(colName)));
        continue;
      }
      String spannerColumnName = "";
      try {
        spannerColumnName = schemaMapper.getSpannerColumnName("", sourceTable.name(), colName);
      } catch (NoSuchElementException e) {
        continue;
      }
      Column spannerColDef = spannerTable.column(spannerColumnName);
      if (spannerColDef == null) {
        continue;
      }
      T columnValue;
      String actualColName = spannerColDef.name();
      if (keyValuesJson.has(actualColName)) {
        if (keyValuesJson.isNull(actualColName)) {
          response.put(colName, nullValueSupplier.get());
          continue;
        }
        columnValue =
            columnMapper.map(spannerColDef, sourceColDef, keyValuesJson, sourceDbTimezoneOffset);
      } else if (newValuesJson.has(actualColName)) {
        if (newValuesJson.isNull(actualColName)) {
          response.put(colName, nullValueSupplier.get());
          continue;
        }
        columnValue =
            columnMapper.map(spannerColDef, sourceColDef, newValuesJson, sourceDbTimezoneOffset);
      } else {
        continue;
      }

      response.put(colName, columnValue);
    }

    return response;
  }

  /** Shared iteration over primary key source columns. See {@link #getColumnValuesInternal}. */
  private static <T> Map<String, T> getPkColumnValuesInternal(
      ISchemaMapper schemaMapper,
      Table spannerTable,
      SourceTable sourceTable,
      JSONObject newValuesJson,
      JSONObject keyValuesJson,
      String sourceDbTimezoneOffset,
      Map<String, Object> customTransformationResponse,
      GenericColumnMapper<T> columnMapper,
      BiFunction<SourceColumn, Object, T> customValueMapper,
      Supplier<T> nullValueSupplier) {
    Map<String, T> response = new LinkedHashMap<>();

    List<String> sourcePKs = sourceTable.primaryKeyColumns();
    Set<String> customTransformColumns = null;
    if (customTransformationResponse != null) {
      customTransformColumns = customTransformationResponse.keySet();
    }

    boolean doesGeneratedColumnExist = false;

    for (int i = 0; i < sourcePKs.size(); i++) {
      String sourceColName = sourcePKs.get(i);
      SourceColumn sourceColDef = sourceTable.column(sourceColName);
      if (sourceColDef == null) {
        LOG.warn(
            "The source column definition for {} was not found in source schema", sourceColName);
        return null;
      }

      if (sourceColDef.isGenerated()) {
        doesGeneratedColumnExist = true;
        continue;
      }

      if (customTransformColumns != null && customTransformColumns.contains(sourceColName)) {
        response.put(
            sourceColName,
            customValueMapper.apply(sourceColDef, customTransformationResponse.get(sourceColName)));
        continue;
      }

      String spannerColName = "";
      try {
        spannerColName = schemaMapper.getSpannerColumnName("", sourceTable.name(), sourceColName);
      } catch (NoSuchElementException e) {
        continue;
      }
      if (spannerColName == null) {
        continue;
      }
      Column spannerColDef = spannerTable.column(spannerColName);
      if (spannerColDef == null) {
        LOG.warn(
            "The spanner column definition for {} was not found in spanner schema", spannerColName);
        continue;
      }
      T columnValue;
      String actualColName = spannerColDef.name();
      if (keyValuesJson.has(actualColName)) {
        if (keyValuesJson.isNull(actualColName)) {
          response.put(sourceColName, nullValueSupplier.get());
          continue;
        }
        columnValue =
            columnMapper.map(spannerColDef, sourceColDef, keyValuesJson, sourceDbTimezoneOffset);
      } else if (newValuesJson.has(actualColName)) {
        if (newValuesJson.isNull(actualColName)) {
          response.put(sourceColName, nullValueSupplier.get());
          continue;
        }
        columnValue =
            columnMapper.map(spannerColDef, sourceColDef, newValuesJson, sourceDbTimezoneOffset);
      } else {
        LOG.warn("The column {} was not found in input record", actualColName);
        return null;
      }

      response.put(sourceColName, columnValue);
    }

    if (doesGeneratedColumnExist) {
      Map<String, T> generatedColumnValues =
          getColumnValuesInternal(
              schemaMapper,
              spannerTable,
              sourceTable,
              newValuesJson,
              keyValuesJson,
              sourceDbTimezoneOffset,
              customTransformationResponse,
              columnMapper,
              customValueMapper,
              nullValueSupplier);
      response.putAll(generatedColumnValues);
    }

    return response;
  }
}
