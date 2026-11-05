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
package com.google.cloud.teleport.v2.templates.source.spanner;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.templates.dbutils.dml.IDMLGenerator;
import com.google.cloud.teleport.v2.templates.exceptions.InvalidDMLGenerationException;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorRequest;
import com.google.cloud.teleport.v2.templates.models.DMLGeneratorResponse;
import com.google.cloud.teleport.v2.templates.models.SpannerMutationResponse;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Spanner implementation of {@link IDMLGenerator}. Generates Spanner {@link Mutation} objects for
 * Cloud Spanner targets.
 */
public class SpannerDMLGenerator implements IDMLGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(SpannerDMLGenerator.class);

  @Override
  public DMLGeneratorResponse getDMLStatement(DMLGeneratorRequest request) {
    if (request == null) {
      throw new InvalidDMLGenerationException(
          "DMLGeneratorRequest is null. Cannot process the request.");
    }

    // Target - The spanner database which this pipeline is writing to
    // Original - The spanner database whose writes are being replicated via changestream
    String origSpannerTableName = request.getSpannerTableName();
    ISchemaMapper schemaMapper = request.getSchemaMapper();
    Ddl originalSpannerDdl = request.getSpannerDdl();
    com.google.cloud.teleport.v2.spanner.sourceddl.SourceSchema targetSpannerSchema =
        request.getSourceSchema();

    if (schemaMapper == null) {
      throw new InvalidDMLGenerationException("SchemaMapper must not be null.");
    }
    if (originalSpannerDdl == null) {
      throw new InvalidDMLGenerationException("Spanner DDL must not be null.");
    }
    if (targetSpannerSchema == null) {
      throw new InvalidDMLGenerationException("target spanner schema could not be fetched.");
    }

    Ddl targetDdl =
        (targetSpannerSchema.rawDdl() instanceof Ddl) ? (Ddl) targetSpannerSchema.rawDdl() : null;
    if (targetDdl == null) {
      throw new InvalidDMLGenerationException("target spanner ddl could not be fetched.");
    }

    Table origSpannerTable = originalSpannerDdl.table(origSpannerTableName);
    if (origSpannerTable == null) {
      throw new InvalidDMLGenerationException(
          "Original Spanner table '" + origSpannerTableName + "' not found in source DDL.");
    }

    String targetTableName;
    try {
      targetTableName = schemaMapper.getSourceTableName("", origSpannerTableName);
    } catch (NoSuchElementException e) {
      throw new InvalidDMLGenerationException(
          "Could not find target table name for source Spanner table: " + origSpannerTableName, e);
    }

    SourceTable targetSpannerTable = targetSpannerSchema.table(targetTableName);
    if (targetSpannerTable == null) {
      throw new InvalidDMLGenerationException(
          "Target table '" + targetTableName + "' not found in SourceSchema.");
    }

    if (targetSpannerTable.primaryKeyColumns() == null
        || targetSpannerTable.primaryKeyColumns().isEmpty()) {
      throw new InvalidDMLGenerationException(
          "Cannot reverse replicate to target table '"
              + targetTableName
              + "' without a primary key.");
    }

    String modType = request.getModType();
    if ("INSERT".equals(modType) || "UPDATE".equals(modType)) {
      return buildUpsertMutation(
          origSpannerTable, targetSpannerTable, schemaMapper, request, targetTableName, targetDdl);
    } else if ("DELETE".equals(modType)) {
      return buildDeleteMutation(
          origSpannerTable, targetSpannerTable, schemaMapper, request, targetTableName, targetDdl);
    } else {
      throw new InvalidDMLGenerationException(
          "Unsupported modType '" + modType + "' for table " + origSpannerTableName);
    }
  }

  private static DMLGeneratorResponse buildUpsertMutation(
      Table origSpannerTable,
      SourceTable targetSpannerTable,
      ISchemaMapper schemaMapper,
      DMLGeneratorRequest request,
      String targetTableName,
      Ddl targetDdl) {

    Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(targetTableName);
    JSONObject newValuesJson = request.getNewValuesJson();
    JSONObject keyValuesJson = request.getKeyValuesJson();
    Table targetSpTable = targetDdl.table(targetTableName);

    for (Column targetCol : targetSpTable.columns()) {
      if (targetCol.isGenerated()) {
        continue;
      }

      String targetColName = targetCol.name();

      if (request.getCustomTransformationResponse() != null
          && request.getCustomTransformationResponse().containsKey(targetColName)) {
        Object customVal = request.getCustomTransformationResponse().get(targetColName);
        if (customVal == null) {
          setNullValue(builder, targetColName, targetCol.type());
        } else {
          setCustomColumnValue(builder, targetCol, customVal);
        }
      } else {
        String originalColName;
        try {
          originalColName =
              schemaMapper.getSpannerColumnName("", targetSpannerTable.name(), targetColName);
        } catch (NoSuchElementException e) {
          continue;
        }

        Column origCol = origSpannerTable.column(originalColName);
        if (origCol == null) {
          // There is no column in the original spanner which maps to the target spanner
          continue;
        }
        JSONObject valuesJson = keyValuesJson.has(originalColName) ? keyValuesJson : newValuesJson;
        if (valuesJson.has(originalColName)) {
          if (valuesJson.isNull(originalColName)) {
            setNullValue(builder, targetColName, origCol.type());
          } else {
            setColumnValue(builder, targetCol, origCol, valuesJson);
          }
        }
      }
    }

    Key primaryKey =
        buildTargetPrimaryKey(
            origSpannerTable,
            targetDdl,
            targetSpannerTable,
            schemaMapper,
            newValuesJson,
            keyValuesJson,
            request.getCustomTransformationResponse());
    return new SpannerMutationResponse(builder.build(), primaryKey);
  }

  private static Key buildTargetPrimaryKey(
      Table origSpannerTable,
      Ddl targetDdl,
      SourceTable targetSpannerTable,
      ISchemaMapper schemaMapper,
      JSONObject newValuesJson,
      JSONObject keyValuesJson,
      Map<String, Object> customTransformationResponse) {
    Key.Builder keyBuilder = Key.newBuilder();
    for (String targetColName : targetSpannerTable.primaryKeyColumns()) {
      Column targetCol = null;
      if (targetDdl != null && targetDdl.table(targetSpannerTable.name()) != null) {
        targetCol = targetDdl.table(targetSpannerTable.name()).column(targetColName);
      }
      if (targetCol == null) {
        throw new InvalidDMLGenerationException(
            "Primary key column '" + targetColName + "' not found in DDL.");
      }

      Object targetColValue = null;
      if (customTransformationResponse != null
          && customTransformationResponse.containsKey(targetColName)) {
        targetColValue = customTransformationResponse.get(targetColName);
      } else {
        String origColName = null;
        try {
          if (schemaMapper != null) {
            origColName =
                schemaMapper.getSpannerColumnName("", targetSpannerTable.name(), targetColName);
          }
        } catch (NoSuchElementException e) {
          throw new InvalidDMLGenerationException(
              "there is no mapped column or custom transformation for table'"
                  + targetSpannerTable.name()
                  + "' column'"
                  + targetColName
                  + "'");
        }
        // Fetch the value from the changestream record
        if (keyValuesJson != null && keyValuesJson.has(origColName)) {
          // column was a part of key in the original record.
          targetColValue = keyValuesJson.get(origColName);
        } else if (newValuesJson != null && newValuesJson.has(origColName)) {
          // column was not a part of key in the original record.
          targetColValue = newValuesJson.get(origColName);
        } else {
          // column not resolvable
          throw new InvalidDMLGenerationException(
              "Primary key column '"
                  + targetColName
                  + "' could not be resolved because of incorrect schema mapper.");
        }
      }
      appendCustomKeyComponent(keyBuilder, targetCol, targetColValue);
    }
    return keyBuilder.build();
  }

  private static DMLGeneratorResponse buildDeleteMutation(
      Table origSpannerTable,
      SourceTable targetSpannerTable,
      ISchemaMapper schemaMapper,
      DMLGeneratorRequest request,
      String targetTableName,
      Ddl targetDdl) {

    JSONObject keyValuesJson = request.getKeyValuesJson();
    JSONObject newValuesJson = request.getNewValuesJson();

    Key primaryKey =
        buildTargetPrimaryKey(
            origSpannerTable,
            targetDdl,
            targetSpannerTable,
            schemaMapper,
            newValuesJson,
            keyValuesJson,
            request.getCustomTransformationResponse());
    Mutation mutation = Mutation.delete(targetTableName, primaryKey);
    return new SpannerMutationResponse(mutation, primaryKey);
  }

  private static void setColumnValue(
      WriteBuilder builder, Column targetCol, Column origCol, JSONObject valuesJson) {
    String origColName = origCol.name();
    Type type = targetCol.type();

    if (type.getCode() == Type.Code.ARRAY || type.getCode() == Type.Code.PG_ARRAY) {
      builder
          .set(targetCol.name())
          .to(buildArrayValue(type.getArrayElementType(), valuesJson.getJSONArray(origColName)));
      return;
    }

    Object value = valuesJson.get(origColName);
    setCustomColumnValue(builder, targetCol, value);
  }

  private static void setNullValue(Mutation.WriteBuilder builder, String targetColName, Type type) {
    switch (type.getCode()) {
      case BOOL:
      case PG_BOOL:
        builder.set(targetColName).to((Boolean) null);
        break;
      case INT64:
      case PG_INT8:
        builder.set(targetColName).to((Long) null);
        break;
      case FLOAT64:
      case PG_FLOAT8:
        builder.set(targetColName).to((Double) null);
        break;
      case FLOAT32:
      case PG_FLOAT4:
        builder.set(targetColName).to((Float) null);
        break;
      case STRING:
      case PG_TEXT:
      case PG_VARCHAR:
      case UUID:
      case PG_UUID:
        builder.set(targetColName).to((String) null);
        break;
      case JSON:
        builder.set(targetColName).to(Value.json(null));
        break;
      case PG_JSONB:
        builder.set(targetColName).to(Value.pgJsonb(null));
        break;
      case BYTES:
      case PG_BYTEA:
        builder.set(targetColName).to((ByteArray) null);
        break;
      case DATE:
      case PG_DATE:
        builder.set(targetColName).to((Date) null);
        break;
      case TIMESTAMP:
      case PG_TIMESTAMPTZ:
      case PG_COMMIT_TIMESTAMP:
        builder.set(targetColName).to((Timestamp) null);
        break;
      case NUMERIC:
        builder.set(targetColName).to((BigDecimal) null);
        break;
      case PG_NUMERIC:
        builder.set(targetColName).to(Value.pgNumeric(null));
        break;
      case ARRAY:
      case PG_ARRAY:
        setNullArrayValue(builder, targetColName, type.getArrayElementType());
        break;
      default:
        builder.set(targetColName).to((String) null);
    }
  }

  private static void setNullArrayValue(
      Mutation.WriteBuilder builder, String targetColName, Type elementType) {
    switch (elementType.getCode()) {
      case BOOL:
      case PG_BOOL:
        builder.set(targetColName).toBoolArray((Iterable<Boolean>) null);
        break;
      case INT64:
      case PG_INT8:
        builder.set(targetColName).toInt64Array((Iterable<Long>) null);
        break;
      case FLOAT64:
      case PG_FLOAT8:
        builder.set(targetColName).toFloat64Array((Iterable<Double>) null);
        break;
      case FLOAT32:
      case PG_FLOAT4:
        builder.set(targetColName).toFloat32Array((Iterable<Float>) null);
        break;
      case STRING:
      case PG_TEXT:
      case PG_VARCHAR:
      case UUID:
      case PG_UUID:
        builder.set(targetColName).toStringArray((Iterable<String>) null);
        break;
      case JSON:
        builder.set(targetColName).toJsonArray((Iterable<String>) null);
        break;
      case PG_JSONB:
        builder.set(targetColName).toPgJsonbArray((Iterable<String>) null);
        break;
      case BYTES:
      case PG_BYTEA:
        builder.set(targetColName).toBytesArray((Iterable<ByteArray>) null);
        break;
      case DATE:
      case PG_DATE:
        builder.set(targetColName).toDateArray((Iterable<Date>) null);
        break;
      case TIMESTAMP:
      case PG_TIMESTAMPTZ:
      case PG_COMMIT_TIMESTAMP:
        builder.set(targetColName).toTimestampArray((Iterable<Timestamp>) null);
        break;
      case NUMERIC:
        builder.set(targetColName).toNumericArray((Iterable<BigDecimal>) null);
        break;
      case PG_NUMERIC:
        builder.set(targetColName).toPgNumericArray((Iterable<String>) null);
        break;
      default:
        builder.set(targetColName).toStringArray((Iterable<String>) null);
    }
  }

  /**
   * Binds a custom-transformation {@link Object} to the mutation builder using the target column's
   * Spanner type. Strings are coerced into the correct primitive when needed; already-typed values
   * (e.g. from Java-based transformers) are bound directly.
   */
  private static void setCustomColumnValue(WriteBuilder builder, Column targetCol, Object value) {
    String targetColName = targetCol.name();
    Type type = targetCol.type();
    switch (type.getCode()) {
      case BOOL:
      case PG_BOOL:
        if (value instanceof Boolean) {
          builder.set(targetColName).to((Boolean) value);
        } else {
          builder.set(targetColName).to(Boolean.parseBoolean(value.toString()));
        }
        break;
      case INT64:
      case PG_INT8:
        if (value instanceof Number) {
          builder.set(targetColName).to(((Number) value).longValue());
        } else {
          builder.set(targetColName).to(Long.parseLong(value.toString()));
        }
        break;
      case FLOAT64:
      case PG_FLOAT8:
        if (value instanceof Number) {
          builder.set(targetColName).to(((Number) value).doubleValue());
        } else {
          builder.set(targetColName).to(Double.parseDouble(value.toString()));
        }
        break;
      case FLOAT32:
      case PG_FLOAT4:
        if (value instanceof Number) {
          builder.set(targetColName).to(((Number) value).floatValue());
        } else {
          builder.set(targetColName).to(Float.parseFloat(value.toString()));
        }
        break;
      case STRING:
      case PG_TEXT:
      case PG_VARCHAR:
      case UUID:
      case PG_UUID:
        builder.set(targetColName).to(value.toString());
        break;
      case JSON:
        builder.set(targetColName).to(Value.json(value.toString()));
        break;
      case PG_JSONB:
        builder.set(targetColName).to(Value.pgJsonb(value.toString()));
        break;
      case BYTES:
      case PG_BYTEA:
        if (value instanceof byte[]) {
          builder.set(targetColName).to(ByteArray.copyFrom((byte[]) value));
        } else if (value instanceof ByteArray) {
          builder.set(targetColName).to((ByteArray) value);
        } else {
          builder.set(targetColName).to(ByteArray.fromBase64(value.toString()));
        }
        break;
      case DATE:
      case PG_DATE:
        if (value instanceof com.google.cloud.Date) {
          builder.set(targetColName).to((com.google.cloud.Date) value);
        } else {
          builder.set(targetColName).to(Date.parseDate(value.toString()));
        }
        break;
      case TIMESTAMP:
      case PG_TIMESTAMPTZ:
      case PG_COMMIT_TIMESTAMP:
        if (value instanceof com.google.cloud.Timestamp) {
          builder.set(targetColName).to((com.google.cloud.Timestamp) value);
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
      case PG_NUMERIC:
        builder.set(targetColName).to(Value.pgNumeric(value.toString()));
        break;
      default:
        LOG.warn(
            "Unrecognised Spanner type code {} for column '{}'; falling back to STRING.",
            type.getCode(),
            targetColName);
        builder.set(targetColName).to(value.toString());
    }
  }

  /**
   * Appends a custom-transformation {@link Object} to the primary-key {@link Key.Builder}. Mirrors
   * {@link #setCustomColumnValue} for the DELETE path.
   */
  private static void appendCustomKeyComponent(Key.Builder keyBuilder, Column col, Object value) {
    if (value == null) {
      keyBuilder.appendObject(null);
      return;
    }
    Type type = col.type();
    switch (type.getCode()) {
      case BOOL:
      case PG_BOOL:
        if (value instanceof Boolean) {
          keyBuilder.append((Boolean) value);
        } else {
          keyBuilder.append(Boolean.parseBoolean(value.toString()));
        }
        break;
      case INT64:
      case PG_INT8:
        if (value instanceof Number) {
          keyBuilder.append(((Number) value).longValue());
        } else {
          keyBuilder.append(Long.parseLong(value.toString()));
        }
        break;
      case FLOAT64:
      case PG_FLOAT8:
        if (value instanceof Number) {
          keyBuilder.append(((Number) value).doubleValue());
        } else {
          keyBuilder.append(Double.parseDouble(value.toString()));
        }
        break;
      case FLOAT32:
      case PG_FLOAT4:
        if (value instanceof Number) {
          keyBuilder.append(((Number) value).floatValue());
        } else {
          keyBuilder.append(Float.parseFloat(value.toString()));
        }
        break;
      case BYTES:
      case PG_BYTEA:
        if (value instanceof byte[]) {
          keyBuilder.append(ByteArray.copyFrom((byte[]) value));
        } else if (value instanceof ByteArray) {
          keyBuilder.append((ByteArray) value);
        } else {
          keyBuilder.append(ByteArray.fromBase64(value.toString()));
        }
        break;
      case DATE:
      case PG_DATE:
        if (value instanceof com.google.cloud.Date) {
          keyBuilder.append((com.google.cloud.Date) value);
        } else {
          keyBuilder.append(Date.parseDate(value.toString()));
        }
        break;
      case TIMESTAMP:
      case PG_TIMESTAMPTZ:
      case PG_COMMIT_TIMESTAMP:
        if (value instanceof com.google.cloud.Timestamp) {
          keyBuilder.append((com.google.cloud.Timestamp) value);
        } else {
          keyBuilder.append(Timestamp.parseTimestamp(value.toString()));
        }
        break;
      case NUMERIC:
      case PG_NUMERIC:
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
  private static Value buildArrayValue(Type elementSpannerType, JSONArray jsonArray) {
    switch (elementSpannerType.getCode()) {
      case BOOL:
      case PG_BOOL:
        {
          List<Boolean> vals = new ArrayList<>();
          for (int i = 0; i < jsonArray.length(); i++) {
            vals.add(jsonArray.isNull(i) ? null : jsonArray.getBoolean(i));
          }
          return Value.boolArray(vals);
        }
      case INT64:
      case PG_INT8:
        {
          List<Long> vals = new ArrayList<>();
          for (int i = 0; i < jsonArray.length(); i++) {
            vals.add(jsonArray.isNull(i) ? null : jsonArray.getLong(i));
          }
          return Value.int64Array(vals);
        }
      case FLOAT64:
      case PG_FLOAT8:
        {
          List<Double> vals = new ArrayList<>();
          for (int i = 0; i < jsonArray.length(); i++) {
            vals.add(jsonArray.isNull(i) ? null : jsonArray.getDouble(i));
          }
          return Value.float64Array(vals);
        }
      case FLOAT32:
      case PG_FLOAT4:
        {
          List<Float> vals = new ArrayList<>();
          for (int i = 0; i < jsonArray.length(); i++) {
            vals.add(jsonArray.isNull(i) ? null : (float) jsonArray.getDouble(i));
          }
          return Value.float32Array(vals);
        }
      case DATE:
      case PG_DATE:
        {
          List<Date> vals = new ArrayList<>();
          for (int i = 0; i < jsonArray.length(); i++) {
            vals.add(jsonArray.isNull(i) ? null : Date.parseDate(jsonArray.getString(i)));
          }
          return Value.dateArray(vals);
        }
      case TIMESTAMP:
      case PG_TIMESTAMPTZ:
      case PG_COMMIT_TIMESTAMP:
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
      case PG_NUMERIC:
        {
          List<String> vals = new ArrayList<>();
          for (int i = 0; i < jsonArray.length(); i++) {
            vals.add(jsonArray.isNull(i) ? null : jsonArray.getString(i));
          }
          return Value.pgNumericArray(vals);
        }
      case JSON:
        {
          List<String> vals = new ArrayList<>();
          for (int i = 0; i < jsonArray.length(); i++) {
            vals.add(jsonArray.isNull(i) ? null : jsonArray.getString(i));
          }
          return Value.jsonArray(vals);
        }
      case PG_JSONB:
        {
          List<String> vals = new ArrayList<>();
          for (int i = 0; i < jsonArray.length(); i++) {
            vals.add(jsonArray.isNull(i) ? null : jsonArray.getString(i));
          }
          return Value.pgJsonbArray(vals);
        }
      case BYTES:
      case PG_BYTEA:
        {
          List<ByteArray> vals = new ArrayList<>();
          for (int i = 0; i < jsonArray.length(); i++) {
            vals.add(jsonArray.isNull(i) ? null : ByteArray.fromBase64(jsonArray.getString(i)));
          }
          return Value.bytesArray(vals);
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
