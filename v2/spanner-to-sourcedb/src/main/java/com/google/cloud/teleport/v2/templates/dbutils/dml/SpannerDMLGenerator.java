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

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.IndexColumn;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.templates.exceptions.InvalidDMLGenerationException;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.models.SpannerMutationResponse;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates Spanner {@link Mutation} objects for reverse-replication to a Cloud Spanner target.
 *
 * <p>INSERT and UPDATE change-stream events produce an {@code insertOrUpdate} mutation. DELETE
 * events produce a {@code delete} mutation keyed on the primary-key values from the change record.
 *
 * <p>Value conversion reads directly from the source Spanner DDL column type ({@link Type}), which
 * avoids ambiguity when the source and target share the same type system.
 */
public class SpannerDMLGenerator implements IDMLGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerDMLGenerator.class);

  @Override
  public DMLGeneratorResponse getDMLStatement(DMLGeneratorRequest request) {
    if (request == null) {
      throw new InvalidDMLGenerationException(
          "DMLGeneratorRequest is null. Cannot process the request.");
    }

    String spannerTableName = request.getSpannerTableName();
    ISchemaMapper schemaMapper = request.getSchemaMapper();
    Ddl spannerDdl = request.getSpannerDdl();
    SourceSchema sourceSchema = request.getSourceSchema();

    if (schemaMapper == null) {
      throw new InvalidDMLGenerationException("SchemaMapper must not be null.");
    }
    if (spannerDdl == null) {
      throw new InvalidDMLGenerationException("Spanner DDL must not be null.");
    }
    if (sourceSchema == null) {
      throw new InvalidDMLGenerationException("SourceSchema must not be null.");
    }

    Table spannerTable = spannerDdl.table(spannerTableName);
    if (spannerTable == null) {
      throw new InvalidDMLGenerationException(
          "Spanner table '" + spannerTableName + "' not found in DDL.");
    }

    String targetTableName;
    try {
      targetTableName = schemaMapper.getSourceTableName("", spannerTableName);
    } catch (NoSuchElementException e) {
      throw new InvalidDMLGenerationException(
          "Could not find target table name for Spanner table: " + spannerTableName, e);
    }

    SourceTable targetTable = sourceSchema.table(targetTableName);
    if (targetTable == null) {
      throw new InvalidDMLGenerationException(
          "Target table '" + targetTableName + "' not found in SourceSchema.");
    }

    if (targetTable.primaryKeyColumns() == null || targetTable.primaryKeyColumns().isEmpty()) {
      throw new InvalidDMLGenerationException(
          "Cannot reverse replicate to target table '"
              + targetTableName
              + "' without a primary key.");
    }

    String modType = request.getModType();
    if ("INSERT".equals(modType) || "UPDATE".equals(modType)) {
      return buildUpsertMutation(spannerTable, targetTable, schemaMapper, request, targetTableName);
    } else if ("DELETE".equals(modType)) {
      return buildDeleteMutation(spannerTable, targetTable, schemaMapper, request, targetTableName);
    } else {
      throw new InvalidDMLGenerationException(
          "Unsupported modType '" + modType + "' for table " + spannerTableName);
    }
  }

  private static DMLGeneratorResponse buildUpsertMutation(
      Table spannerTable,
      SourceTable targetTable,
      ISchemaMapper schemaMapper,
      DMLGeneratorRequest request,
      String targetTableName) {

    Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(targetTableName);
    JSONObject newValuesJson = request.getNewValuesJson();
    JSONObject keyValuesJson = request.getKeyValuesJson();

    for (SourceColumn targetCol : targetTable.columns()) {
      if (targetCol.isGenerated()) {
        continue;
      }

      String targetColName = targetCol.name();

      String sourceColName;
      try {
        sourceColName = schemaMapper.getSpannerColumnName("", targetTable.name(), targetColName);
      } catch (NoSuchElementException e) {
        continue;
      }

      Column sourceCol = spannerTable.column(sourceColName);
      if (sourceCol == null) {
        continue;
      }

      if (request.getCustomTransformationResponse() != null
          && request.getCustomTransformationResponse().containsKey(targetColName)) {
        Object customVal = request.getCustomTransformationResponse().get(targetColName);
        if (customVal == null) {
          setNullValue(builder, targetColName, sourceCol.type());
        } else {
          setCustomColumnValue(builder, targetColName, sourceCol, customVal);
        }
        continue;
      }

      JSONObject valuesJson = keyValuesJson.has(sourceColName) ? keyValuesJson : newValuesJson;

      if (!valuesJson.has(sourceColName)) {
        continue;
      }

      if (valuesJson.isNull(sourceColName)) {
        setNullValue(builder, targetColName, sourceCol.type());
      } else {
        setColumnValue(builder, targetColName, sourceCol, valuesJson);
      }
    }

    return new SpannerMutationResponse(builder.build());
  }

  private static DMLGeneratorResponse buildDeleteMutation(
      Table spannerTable,
      SourceTable targetTable,
      ISchemaMapper schemaMapper,
      DMLGeneratorRequest request,
      String targetTableName) {

    JSONObject keyValuesJson = request.getKeyValuesJson();
    JSONObject newValuesJson = request.getNewValuesJson();

    Key.Builder keyBuilder = Key.newBuilder();
    for (IndexColumn pkIndexCol : spannerTable.primaryKeys()) {
      String sourceColName = pkIndexCol.name();

      String targetColName;
      try {
        targetColName = schemaMapper.getSourceColumnName("", targetTableName, sourceColName);
      } catch (NoSuchElementException e) {
        targetColName = sourceColName;
      }

      Column sourceCol = spannerTable.column(sourceColName);
      if (sourceCol == null) {
        throw new InvalidDMLGenerationException(
            "Column '" + sourceColName + "' not found in Spanner DDL for table " + targetTableName);
      }

      if (request.getCustomTransformationResponse() != null
          && request.getCustomTransformationResponse().containsKey(targetColName)) {
        Object customVal = request.getCustomTransformationResponse().get(targetColName);
        appendCustomKeyComponent(keyBuilder, sourceCol, customVal);
        continue;
      }

      JSONObject valuesJson = keyValuesJson.has(sourceColName) ? keyValuesJson : newValuesJson;

      if (!valuesJson.has(sourceColName)) {
        LOG.warn("Primary key column '{}' not found in change record for DELETE.", sourceColName);
        throw new InvalidDMLGenerationException(
            "Primary key column '"
                + sourceColName
                + "' missing from change record for table "
                + targetTableName);
      }

      if (valuesJson.isNull(sourceColName)) {
        keyBuilder.append((String) null);
      } else {
        appendKeyComponent(keyBuilder, sourceCol, valuesJson, sourceColName);
      }
    }

    Mutation mutation = Mutation.delete(targetTableName, keyBuilder.build());
    return new SpannerMutationResponse(mutation);
  }

  private static void setColumnValue(
      Mutation.WriteBuilder builder, String targetColName, Column col, JSONObject valuesJson) {
    String sourceColName = col.name();
    Type type = col.type();

    switch (type.getCode()) {
      case BOOL:
        builder.set(targetColName).to(valuesJson.getBoolean(sourceColName));
        break;
      case INT64:
        builder.set(targetColName).to(Long.parseLong(valuesJson.getString(sourceColName)));
        break;
      case FLOAT64:
        builder.set(targetColName).to(valuesJson.getBigDecimal(sourceColName).doubleValue());
        break;
      case FLOAT32:
        builder
            .set(targetColName)
            .to((float) valuesJson.getBigDecimal(sourceColName).doubleValue());
        break;
      case STRING:
        builder.set(targetColName).to(valuesJson.getString(sourceColName));
        break;
      case JSON:
        builder.set(targetColName).to(Value.json(valuesJson.getString(sourceColName)));
        break;
      case BYTES:
        builder.set(targetColName).to(ByteArray.fromBase64(valuesJson.getString(sourceColName)));
        break;
      case DATE:
        builder.set(targetColName).to(Date.parseDate(valuesJson.getString(sourceColName)));
        break;
      case TIMESTAMP:
        builder
            .set(targetColName)
            .to(Timestamp.parseTimestamp(valuesJson.getString(sourceColName)));
        break;
      case NUMERIC:
        builder.set(targetColName).to(new BigDecimal(valuesJson.getString(sourceColName)));
        break;
      case ARRAY:
        builder
            .set(targetColName)
            .to(
                buildArrayValue(
                    type.getArrayElementType(), valuesJson.getJSONArray(sourceColName)));
        break;
      default:
        LOG.warn(
            "Unrecognised Spanner type code {} for column '{}'; falling back to STRING.",
            type.getCode(),
            targetColName);
        builder.set(targetColName).to(valuesJson.getString(sourceColName));
    }
  }

  private static void setNullValue(Mutation.WriteBuilder builder, String targetColName, Type type) {
    switch (type.getCode()) {
      case BOOL:
        builder.set(targetColName).to((Boolean) null);
        break;
      case INT64:
        builder.set(targetColName).to((Long) null);
        break;
      case FLOAT64:
        builder.set(targetColName).to((Double) null);
        break;
      case FLOAT32:
        builder.set(targetColName).to((Float) null);
        break;
      case BYTES:
        builder.set(targetColName).to((ByteArray) null);
        break;
      case DATE:
        builder.set(targetColName).to((Date) null);
        break;
      case TIMESTAMP:
        builder.set(targetColName).to((Timestamp) null);
        break;
      case NUMERIC:
        builder.set(targetColName).to((BigDecimal) null);
        break;
      case JSON:
        builder.set(targetColName).to(Value.json(null));
        break;
      case ARRAY:
        setNullArrayValue(builder, targetColName, type.getArrayElementType());
        break;
      default:
        builder.set(targetColName).to((String) null);
    }
  }

  /**
   * Emits a typed NULL for an ARRAY column. The Spanner client requires the null value to carry
   * the array element type, otherwise a commit-time type mismatch occurs (e.g. binding
   * {@code Value.stringArray(null)} to an {@code ARRAY<INT64>} column).
   */
  private static void setNullArrayValue(
      Mutation.WriteBuilder builder, String targetColName, Type elementType) {
    switch (elementType.getCode()) {
      case BOOL:
        builder.set(targetColName).to(Value.boolArray((Iterable<Boolean>) null));
        break;
      case INT64:
        builder.set(targetColName).to(Value.int64Array((Iterable<Long>) null));
        break;
      case FLOAT64:
        builder.set(targetColName).to(Value.float64Array((Iterable<Double>) null));
        break;
      case FLOAT32:
        builder.set(targetColName).to(Value.float32Array((Iterable<Float>) null));
        break;
      case BYTES:
        builder.set(targetColName).to(Value.bytesArray((Iterable<ByteArray>) null));
        break;
      case DATE:
        builder.set(targetColName).to(Value.dateArray((Iterable<Date>) null));
        break;
      case TIMESTAMP:
        builder.set(targetColName).to(Value.timestampArray((Iterable<Timestamp>) null));
        break;
      case NUMERIC:
        builder.set(targetColName).to(Value.numericArray((Iterable<BigDecimal>) null));
        break;
      case JSON:
        builder.set(targetColName).to(Value.jsonArray(null));
        break;
      default:
        builder.set(targetColName).to(Value.stringArray((Iterable<String>) null));
    }
  }

  /** Appends a single primary-key component to the Key builder. */
  private static void appendKeyComponent(
      Key.Builder keyBuilder, Column col, JSONObject valuesJson, String sourceColName) {
    Type type = col.type();
    switch (type.getCode()) {
      case BOOL:
        keyBuilder.append(valuesJson.getBoolean(sourceColName));
        break;
      case INT64:
        keyBuilder.append(Long.parseLong(valuesJson.getString(sourceColName)));
        break;
      case FLOAT64:
        keyBuilder.append(valuesJson.getBigDecimal(sourceColName).doubleValue());
        break;
      case FLOAT32:
        keyBuilder.append((float) valuesJson.getBigDecimal(sourceColName).doubleValue());
        break;
      case BYTES:
        keyBuilder.append(ByteArray.fromBase64(valuesJson.getString(sourceColName)));
        break;
      case DATE:
        keyBuilder.append(Date.parseDate(valuesJson.getString(sourceColName)));
        break;
      case TIMESTAMP:
        keyBuilder.append(Timestamp.parseTimestamp(valuesJson.getString(sourceColName)));
        break;
      case NUMERIC:
        keyBuilder.append(new BigDecimal(valuesJson.getString(sourceColName)));
        break;
      default:
        keyBuilder.append(valuesJson.getString(sourceColName));
    }
  }

  /**
   * Binds a custom-transformation {@link Object} to the mutation builder using the target column's
   * Spanner type. Strings are coerced into the correct primitive when needed; already-typed values
   * are passed through.
   */
  private static void setCustomColumnValue(
      Mutation.WriteBuilder builder, String targetColName, Column col, Object value) {
    Type type = col.type();
    switch (type.getCode()) {
      case BOOL:
        if (value instanceof Boolean) {
          builder.set(targetColName).to((Boolean) value);
        } else {
          builder.set(targetColName).to(Boolean.parseBoolean(value.toString()));
        }
        break;
      case INT64:
        if (value instanceof Number) {
          builder.set(targetColName).to(((Number) value).longValue());
        } else {
          builder.set(targetColName).to(Long.parseLong(value.toString()));
        }
        break;
      case FLOAT64:
        if (value instanceof Number) {
          builder.set(targetColName).to(((Number) value).doubleValue());
        } else {
          builder.set(targetColName).to(Double.parseDouble(value.toString()));
        }
        break;
      case FLOAT32:
        if (value instanceof Number) {
          builder.set(targetColName).to(((Number) value).floatValue());
        } else {
          builder.set(targetColName).to(Float.parseFloat(value.toString()));
        }
        break;
      case STRING:
        builder.set(targetColName).to(value.toString());
        break;
      case JSON:
        builder.set(targetColName).to(Value.json(value.toString()));
        break;
      case BYTES:
        if (value instanceof byte[]) {
          builder.set(targetColName).to(ByteArray.copyFrom((byte[]) value));
        } else if (value instanceof ByteArray) {
          builder.set(targetColName).to((ByteArray) value);
        } else {
          builder.set(targetColName).to(ByteArray.fromBase64(value.toString()));
        }
        break;
      case DATE:
        if (value instanceof Date) {
          builder.set(targetColName).to((Date) value);
        } else {
          builder.set(targetColName).to(Date.parseDate(value.toString()));
        }
        break;
      case TIMESTAMP:
        if (value instanceof Timestamp) {
          builder.set(targetColName).to((Timestamp) value);
        } else {
          builder.set(targetColName).to(Timestamp.parseTimestamp(value.toString()));
        }
        break;
      case NUMERIC:
        if (value instanceof BigDecimal) {
          builder.set(targetColName).to((BigDecimal) value);
        } else {
          builder.set(targetColName).to(new BigDecimal(value.toString()));
        }
        break;
      default:
        LOG.warn(
            "Unrecognised Spanner type code {} for custom-transformation column '{}'; falling back to STRING.",
            type.getCode(),
            targetColName);
        builder.set(targetColName).to(value.toString());
    }
  }

  /**
   * Appends a custom-transformation primary-key {@link Object} to the {@link Key.Builder} using the
   * source column's Spanner type. Mirrors {@link #setCustomColumnValue} for the DELETE path.
   */
  private static void appendCustomKeyComponent(
      Key.Builder keyBuilder, Column col, Object value) {
    if (value == null) {
      keyBuilder.append((String) null);
      return;
    }
    Type type = col.type();
    switch (type.getCode()) {
      case BOOL:
        if (value instanceof Boolean) {
          keyBuilder.append((Boolean) value);
        } else {
          keyBuilder.append(Boolean.parseBoolean(value.toString()));
        }
        break;
      case INT64:
        if (value instanceof Number) {
          keyBuilder.append(((Number) value).longValue());
        } else {
          keyBuilder.append(Long.parseLong(value.toString()));
        }
        break;
      case FLOAT64:
        if (value instanceof Number) {
          keyBuilder.append(((Number) value).doubleValue());
        } else {
          keyBuilder.append(Double.parseDouble(value.toString()));
        }
        break;
      case FLOAT32:
        if (value instanceof Number) {
          keyBuilder.append(((Number) value).floatValue());
        } else {
          keyBuilder.append(Float.parseFloat(value.toString()));
        }
        break;
      case BYTES:
        if (value instanceof byte[]) {
          keyBuilder.append(ByteArray.copyFrom((byte[]) value));
        } else if (value instanceof ByteArray) {
          keyBuilder.append((ByteArray) value);
        } else {
          keyBuilder.append(ByteArray.fromBase64(value.toString()));
        }
        break;
      case DATE:
        if (value instanceof Date) {
          keyBuilder.append((Date) value);
        } else {
          keyBuilder.append(Date.parseDate(value.toString()));
        }
        break;
      case TIMESTAMP:
        if (value instanceof Timestamp) {
          keyBuilder.append((Timestamp) value);
        } else {
          keyBuilder.append(Timestamp.parseTimestamp(value.toString()));
        }
        break;
      case NUMERIC:
        if (value instanceof BigDecimal) {
          keyBuilder.append((BigDecimal) value);
        } else {
          keyBuilder.append(new BigDecimal(value.toString()));
        }
        break;
      default:
        keyBuilder.append(value.toString());
    }
  }

  /** Builds a Spanner {@link Value} representing a Spanner ARRAY column. */
  private static Value buildArrayValue(Type elementType, JSONArray jsonArray) {
    switch (elementType.getCode()) {
      case BOOL:
        {
          List<Boolean> vals = new ArrayList<>();
          for (int i = 0; i < jsonArray.length(); i++) {
            vals.add(jsonArray.isNull(i) ? null : jsonArray.getBoolean(i));
          }
          return Value.boolArray(vals);
        }
      case INT64:
        {
          List<Long> vals = new ArrayList<>();
          for (int i = 0; i < jsonArray.length(); i++) {
            vals.add(jsonArray.isNull(i) ? null : Long.parseLong(jsonArray.getString(i)));
          }
          return Value.int64Array(vals);
        }
      case FLOAT64:
        {
          List<Double> vals = new ArrayList<>();
          for (int i = 0; i < jsonArray.length(); i++) {
            vals.add(jsonArray.isNull(i) ? null : jsonArray.getBigDecimal(i).doubleValue());
          }
          return Value.float64Array(vals);
        }
      case BYTES:
        {
          List<ByteArray> vals = new ArrayList<>();
          for (int i = 0; i < jsonArray.length(); i++) {
            vals.add(jsonArray.isNull(i) ? null : ByteArray.fromBase64(jsonArray.getString(i)));
          }
          return Value.bytesArray(vals);
        }
      case DATE:
        {
          List<Date> vals = new ArrayList<>();
          for (int i = 0; i < jsonArray.length(); i++) {
            vals.add(jsonArray.isNull(i) ? null : Date.parseDate(jsonArray.getString(i)));
          }
          return Value.dateArray(vals);
        }
      case TIMESTAMP:
        {
          List<Timestamp> vals = new ArrayList<>();
          for (int i = 0; i < jsonArray.length(); i++) {
            vals.add(jsonArray.isNull(i) ? null : Timestamp.parseTimestamp(jsonArray.getString(i)));
          }
          return Value.timestampArray(vals);
        }
      case NUMERIC:
        {
          List<BigDecimal> vals = new ArrayList<>();
          for (int i = 0; i < jsonArray.length(); i++) {
            vals.add(jsonArray.isNull(i) ? null : new BigDecimal(jsonArray.getString(i)));
          }
          return Value.numericArray(vals);
        }
      default:
        {
          List<String> vals = new ArrayList<>();
          for (int i = 0; i < jsonArray.length(); i++) {
            vals.add(jsonArray.isNull(i) ? null : jsonArray.getString(i));
          }
          return Value.stringArray(vals);
        }
    }
  }
}
