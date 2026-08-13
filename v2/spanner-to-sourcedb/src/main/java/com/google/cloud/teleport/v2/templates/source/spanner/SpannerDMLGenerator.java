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
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.spanner.ddl.Column;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.ddl.Table;
import com.google.cloud.teleport.v2.spanner.migrations.schema.ISchemaMapper;
import com.google.cloud.teleport.v2.spanner.sourceddl.SourceTable;
import com.google.cloud.teleport.v2.spanner.type.Type;
import com.google.cloud.teleport.v2.templates.constants.Constants;
import com.google.cloud.teleport.v2.templates.dbutils.dml.IDMLGenerator;
import com.google.cloud.teleport.v2.templates.dbutils.processor.ISpToSrcSourceConnector;
import com.google.cloud.teleport.v2.templates.dbutils.processor.SourceProcessorFactory;
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
      throw new InvalidDMLGenerationException("SourceSchema must not be null.");
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
          origSpannerTable, targetSpannerTable, schemaMapper, request, targetTableName);
    } else if ("DELETE".equals(modType)) {
      return buildDeleteMutation(
          origSpannerTable, targetSpannerTable, schemaMapper, request, targetTableName);
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
      String targetTableName) {

    Mutation.WriteBuilder builder = Mutation.newInsertOrUpdateBuilder(targetTableName);
    JSONObject newValuesJson = request.getNewValuesJson();
    JSONObject keyValuesJson = request.getKeyValuesJson();

    for (com.google.cloud.teleport.v2.spanner.sourceddl.SourceColumn targetCol :
        targetSpannerTable.columns()) {
      if (targetCol.isGenerated()) {
        continue;
      }

      String targetColName = targetCol.name();

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

      if (request.getCustomTransformationResponse() != null
          && request.getCustomTransformationResponse().containsKey(originalColName)) {
        Object customVal = request.getCustomTransformationResponse().get(originalColName);
        if (customVal == null) {
          setNullValue(builder, targetColName, origCol.type());
        } else {
          setCustomColumnValue(builder, targetColName, origCol, customVal);
        }
      } else if (newValuesJson.has(originalColName)) {
        if (newValuesJson.isNull(originalColName)) {
          setNullValue(builder, targetColName, origCol.type());
        } else {
          setColumnValue(builder, targetColName, origCol, newValuesJson);
        }
      } else if (keyValuesJson.has(originalColName)) {
        if (keyValuesJson.isNull(originalColName)) {
          setNullValue(builder, targetColName, origCol.type());
        } else {
          setColumnValue(builder, targetColName, origCol, keyValuesJson);
        }
      }
    }

    Key primaryKey =
        buildTargetPrimaryKey(
            origSpannerTable,
            targetSpannerTable,
            schemaMapper,
            newValuesJson,
            keyValuesJson,
            request.getCustomTransformationResponse());
    return new SpannerMutationResponse(builder.build(), primaryKey);
  }

  private static Key buildTargetPrimaryKey(
      Table origSpannerTable,
      SourceTable targetSpannerTable,
      ISchemaMapper schemaMapper,
      JSONObject newValuesJson,
      JSONObject keyValuesJson,
      Map<String, Object> customTransformationResponse) {
    // TODO- rework class to take targetDdl in constructor
    Ddl targetDdl = null;
    try {
      ISpToSrcSourceConnector sourceConnector =
          SourceProcessorFactory.getSource(Constants.SOURCE_SPANNER);
      if (sourceConnector instanceof SpannerSpToSrcSourceConnector) {
        targetDdl = ((SpannerSpToSrcSourceConnector) sourceConnector).getTargetDdl();
      }
    } catch (Exception e) {
      LOG.warn("could not fetch spanner source", e);
    }

    Key.Builder keyBuilder = Key.newBuilder();
    for (String targetColName : targetSpannerTable.primaryKeyColumns()) {
      Object customVal = null;
      if (customTransformationResponse != null
          && customTransformationResponse.containsKey(targetColName)) {
        customVal = customTransformationResponse.get(targetColName);
      }
      Column targetCol = null;
      if (targetDdl != null && targetDdl.table(targetSpannerTable.name()) != null) {
        targetCol = targetDdl.table(targetSpannerTable.name()).column(targetColName);
      }
      if (targetCol == null) {
        String origColName = null;
        try {
          if (schemaMapper != null) {
            origColName =
                schemaMapper.getSpannerColumnName("", targetSpannerTable.name(), targetColName);
          }
        } catch (NoSuchElementException ignored) {
        }
        if (origColName == null) {
          origColName = targetColName;
        }
        if (origSpannerTable != null && origColName != null) {
          targetCol = origSpannerTable.column(origColName);
        }
      }
      if (targetCol == null) {
        throw new InvalidDMLGenerationException(
            "Primary key column '" + targetColName + "' not found in DDL.");
      }

      JSONObject valuesJson = keyValuesJson.has(targetColName) ? keyValuesJson : newValuesJson;
      if (customVal != null) {
        appendCustomKeyComponent(keyBuilder, targetCol, customVal);
      } else if (valuesJson.has(targetColName)) {
        appendCustomKeyComponent(keyBuilder, targetCol, valuesJson.get(targetColName));
      } else {
        LOG.warn("Primary key column '{}' could not be resolved.", targetColName);
        throw new InvalidDMLGenerationException(
            "Primary key column '" + targetColName + "' could not be resolved.");
      }
    }
    return keyBuilder.build();
  }

  private static DMLGeneratorResponse buildDeleteMutation(
      Table origSpannerTable,
      SourceTable targetSpannerTable,
      ISchemaMapper schemaMapper,
      DMLGeneratorRequest request,
      String targetTableName) {

    JSONObject keyValuesJson = request.getKeyValuesJson();
    JSONObject newValuesJson = request.getNewValuesJson();

    Key primaryKey =
        buildTargetPrimaryKey(
            origSpannerTable,
            targetSpannerTable,
            schemaMapper,
            newValuesJson,
            keyValuesJson,
            request.getCustomTransformationResponse());
    Mutation mutation = Mutation.delete(targetTableName, primaryKey);
    return new SpannerMutationResponse(mutation, primaryKey);
  }

  private static void setColumnValue(
      Mutation.WriteBuilder builder, String targetColName, Column col, JSONObject valuesJson) {
    String sourceColName = col.name();
    Type type = col.type();

    if (type.getCode() == Type.Code.ARRAY) {
      builder
          .set(targetColName)
          .to(buildArrayValue(type.getArrayElementType(), valuesJson.getJSONArray(sourceColName)));
      return;
    }

    Object value = valuesJson.get(sourceColName);
    setCustomColumnValue(builder, targetColName, col, value);
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
      case STRING:
        builder.set(targetColName).to((String) null);
        break;
      case JSON:
        builder.set(targetColName).to(Value.json(null));
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
      case ARRAY:
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
        builder.set(targetColName).toBoolArray((Iterable<Boolean>) null);
        break;
      case INT64:
        builder.set(targetColName).toInt64Array((Iterable<Long>) null);
        break;
      case FLOAT64:
        builder.set(targetColName).toFloat64Array((Iterable<Double>) null);
        break;
      case FLOAT32:
        builder.set(targetColName).toFloat32Array((Iterable<Float>) null);
        break;
      case STRING:
        builder.set(targetColName).toStringArray((Iterable<String>) null);
        break;
      case JSON:
        builder.set(targetColName).toJsonArray((Iterable<String>) null);
        break;
      case BYTES:
        builder.set(targetColName).toBytesArray((Iterable<ByteArray>) null);
        break;
      case DATE:
        builder.set(targetColName).toDateArray((Iterable<Date>) null);
        break;
      case TIMESTAMP:
        builder.set(targetColName).toTimestampArray((Iterable<Timestamp>) null);
        break;
      case NUMERIC:
        builder.set(targetColName).toNumericArray((Iterable<BigDecimal>) null);
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
        if (value instanceof com.google.cloud.Date) {
          builder.set(targetColName).to((com.google.cloud.Date) value);
        } else {
          builder.set(targetColName).to(Date.parseDate(value.toString()));
        }
        break;
      case TIMESTAMP:
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
        if (value instanceof com.google.cloud.Date) {
          keyBuilder.append((com.google.cloud.Date) value);
        } else {
          keyBuilder.append(Date.parseDate(value.toString()));
        }
        break;
      case TIMESTAMP:
        if (value instanceof com.google.cloud.Timestamp) {
          keyBuilder.append((com.google.cloud.Timestamp) value);
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
  private static Value buildArrayValue(Type elementSpannerType, JSONArray jsonArray) {
    switch (elementSpannerType.getCode()) {
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
            vals.add(jsonArray.isNull(i) ? null : jsonArray.getLong(i));
          }
          return Value.int64Array(vals);
        }
      case FLOAT64:
        {
          List<Double> vals = new ArrayList<>();
          for (int i = 0; i < jsonArray.length(); i++) {
            vals.add(jsonArray.isNull(i) ? null : jsonArray.getDouble(i));
          }
          return Value.float64Array(vals);
        }
      case FLOAT32:
        {
          List<Float> vals = new ArrayList<>();
          for (int i = 0; i < jsonArray.length(); i++) {
            vals.add(jsonArray.isNull(i) ? null : (float) jsonArray.getDouble(i));
          }
          return Value.float32Array(vals);
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
      case JSON:
        {
          List<String> vals = new ArrayList<>();
          for (int i = 0; i < jsonArray.length(); i++) {
            vals.add(jsonArray.isNull(i) ? null : jsonArray.getString(i));
          }
          return Value.jsonArray(vals);
        }
      case BYTES:
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
