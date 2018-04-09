/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.teleport.templates.common;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.templates.common.DatastoreConverters.CheckNoKey;
import com.google.datastore.v1.Entity;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.Key.PathElement;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.Value;
import com.google.protobuf.NullValue;
import com.google.protobuf.util.Timestamps;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;

/** Common Code for Teleport BigQueryIO. */
public class BigQueryConverters {

  public static final int MAX_STRING_SIZE_BYTES = 1500;
  public static final int TRUNCATE_STRING_SIZE_CHARS = 401;
  public static final String TRUNCATE_STRING_MESSAGE = "First %d characters of row %s";
  public static final List<String> SUPPORTED_KEY_NAME_TYPES =
      Arrays.asList(
          "STRING",
          "INTEGER",
          "INT64",
          "FLOAT",
          "FLOAT64",
          "BOOLEAN",
          "BOOL",
          "TIMESTAMP",
          "DATE",
          "TIME",
          "DATETIME");

  /** Options for reading data from BigQuery. */
  public interface BigQueryReadOptions extends PipelineOptions {
    @Description("SQL query in standard SQL to pull data from BigQuery")
    ValueProvider<String> getReadQuery();

    void setReadQuery(ValueProvider<String> value);

    @Description("Name of the BQ column storing the unique identifier of the row")
    ValueProvider<String> getReadIdColumn();

    void setReadIdColumn(ValueProvider<String> value);

    @Description("Name of the BQ column storing the unique identifier of the row")
    ValueProvider<String> getInvalidOutputPath();

    void setInvalidOutputPath(ValueProvider<String> value);
  }

  /** Factory method for {@link JsonToTableRow}. */
  public static PTransform<PCollection<String>, PCollection<TableRow>> jsonToTableRow() {
    return new JsonToTableRow();
  }

  /** Converts UTF8 encoded Json records to TableRow records. */
  private static class JsonToTableRow
      extends PTransform<PCollection<String>, PCollection<TableRow>> {

    @Override
    public PCollection<TableRow> expand(PCollection<String> stringPCollection) {
      return stringPCollection.apply("JsonToTableRow", MapElements.<String, TableRow>via(
          new SimpleFunction<String, TableRow>() {
            @Override
            public TableRow apply(String json) {
                try {

                  InputStream inputStream = new ByteArrayInputStream(
                      json.getBytes(StandardCharsets.UTF_8.name()));

                  //OUTER is used here to prevent EOF exception
                  return TableRowJsonCoder.of().decode(inputStream, Context.OUTER);
                } catch (IOException e) {
                  throw new RuntimeException("Unable to parse input", e);
                }
            }
          }));
    }
  }

  /** Reads data from BigQuery and converts it to Datastore Entity format. */
  @AutoValue
  public abstract static class BigQueryToEntity extends PTransform<PBegin, PCollectionTuple> {

    abstract ValueProvider<String> query();

    abstract ValueProvider<String> entityKind();

    abstract ValueProvider<String> uniqueNameColumn();

    @Nullable
    abstract ValueProvider<String> namespace();

    abstract TupleTag<Entity> successTag();

    abstract TupleTag<String> failureTag();

    /** Builder for BigQuery. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setQuery(ValueProvider<String> query);

      public abstract Builder setEntityKind(ValueProvider<String> entityKind);

      public abstract Builder setUniqueNameColumn(ValueProvider<String> uniqueNameColumn);

      public abstract Builder setNamespace(ValueProvider<String> namespace);

      public abstract Builder setSuccessTag(TupleTag<Entity> successTag);

      public abstract Builder setFailureTag(TupleTag<String> failureTag);

      public abstract BigQueryToEntity build();
    }

    public static Builder newBuilder() {
      return new AutoValue_BigQueryConverters_BigQueryToEntity.Builder();
    }

    @Override
    public PCollectionTuple expand(PBegin begin) {
      return begin
          .apply(
              "AvroToEntity",
              BigQueryIO.read(
                      AvroToEntity.newBuilder()
                          .setEntityKind(entityKind())
                          .setUniqueNameColumn(uniqueNameColumn())
                          .setNamespace(namespace())
                          .build())
                  .fromQuery(query())
                  .withoutValidation()
                  .withTemplateCompatibility()
                  .usingStandardSql())
          .apply(
              "CheckNoKey",
              CheckNoKey.newBuilder()
                  .setFailureTag(failureTag())
                  .setSuccessTag(successTag())
                  .build());
    }
  }

  /** Converts from the BigQuery Avro format into Datastore Entity. */
  @AutoValue
  public abstract static class AvroToEntity
      implements SerializableFunction<SchemaAndRecord, Entity> {

    public abstract ValueProvider<String> entityKind();

    public abstract ValueProvider<String> uniqueNameColumn();

    @Nullable
    public abstract ValueProvider<String> namespace();

    /** Builder for AvroToEntity. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setEntityKind(ValueProvider<String> entityKind);

      public abstract Builder setUniqueNameColumn(ValueProvider<String> uniqueNameColumn);

      public abstract Builder setNamespace(ValueProvider<String> namespace);

      public abstract AvroToEntity build();
    }

    public static Builder newBuilder() {
      return new AutoValue_BigQueryConverters_AvroToEntity.Builder();
    }

    public Entity apply(SchemaAndRecord record) {
      GenericRecord row = record.getRecord();
      try {
        Entity.Builder entityBuilder = Entity.newBuilder();
        List<TableFieldSchema> columns = record.getTableSchema().getFields();
        for (TableFieldSchema column : columns) {
          String columnName = column.getName();
          Object columnValue = row.get(columnName);
          if (uniqueNameColumn().get().equals(columnName)) {
            validateKeyColumn(column, columnValue);
            // Set the value of the UniqueNameColumn as Datastore entity's name and the namespace if
            // it's not the default one
            Key.Builder keyBuilder =
                Key.newBuilder()
                    .addPath(
                        PathElement.newBuilder()
                            .setKind(entityKind().get())
                            .setName(columnValue.toString()));
            if (namespace() != null && namespace().get() != null) {
              keyBuilder.setPartitionId(
                  PartitionId.newBuilder().setProjectId("").setNamespaceId(namespace().get()));
            }
            entityBuilder.setKey(keyBuilder);
          } else {
            // Set the values of all other columns and Datastore entity's properties
            entityBuilder
                .getMutableProperties()
                .put(columnName, columnToValue(column, columnValue));
          }
        }
        return entityBuilder.build();
      } catch (IllegalArgumentException e) {
        // Create an invalid Entity without a key for logging purposes further down the stream
        Entity.Builder entityBuilder = Entity.newBuilder();
        entityBuilder
            .getMutableProperties()
            .put("cause", Value.newBuilder().setStringValue(e.getMessage()).build());
        // Store the Avro message in an unindexed string property of the entity to avoid the 1500
        // bytes limit of index-able string properties
        entityBuilder
            .getMutableProperties()
            .put(
                "row",
                Value.newBuilder()
                    .setStringValue(row.toString())
                    .setExcludeFromIndexes(true)
                    .build());
        return entityBuilder.build();
      }
    }
  }

  /**
   * Validates that a BigQuery column in Avro format can be used as a valid Datastore Entity key
   * name.
   */
  public static void validateKeyColumn(TableFieldSchema column, Object columnValue)
      throws IllegalArgumentException {

    // Entity key name must be different than null
    if (columnValue == null) {
      throw new IllegalArgumentException(
          String.format(
              "Column [%s] with NULL value cannot be set as Entity name.", column.getName()));
    }

    // Entity key names cannot exceed 1500 bytes, the maximum size of an idex-able Datastore
    // string property: https://cloud.google.com/datastore/docs/concepts/limits
    if (column.getType().equals("STRING")
        && columnValue.toString().getBytes().length > MAX_STRING_SIZE_BYTES) {
      throw new IllegalArgumentException(
          String.format(
              "Column [%s] exceeding %d bytes cannot be set as Entity name.",
              column.getName(), MAX_STRING_SIZE_BYTES));
    }

    // BigQuery column type must be among the supported ones (ex: cannot be RECORD)
    if (!SUPPORTED_KEY_NAME_TYPES.contains(column.getType())) {
      throw new IllegalArgumentException(
          String.format(
              "Column [%s] of type %s cannot be set as Entity name.",
              column.getName(), column.getType()));
    }
  }

  /**
   * Converts the value of a BigQuery column in Avro format into the value of a Datastore Entity
   * property.
   */
  public static Value columnToValue(TableFieldSchema column, Object columnValue)
      throws IllegalArgumentException {
    String columnName = column.getName();
    if (columnValue == null) {
      return Value.newBuilder().setNullValue(NullValue.NULL_VALUE).build();
    } else {
      Value.Builder valueBuilder = Value.newBuilder();
      switch (column.getType()) {
        case "STRING":
          // Datastore string properties greater than 1500 bytes will not be indexed in order to
          // respect the limit imposed on the maximum size of index-able string properties. See
          // https://cloud.google.com/datastore/docs/concepts/limits
          String strValue = columnValue.toString();
          valueBuilder.setStringValue(strValue);
          boolean excludeFromIndexes = strValue.getBytes().length > MAX_STRING_SIZE_BYTES;
          valueBuilder.setExcludeFromIndexes(excludeFromIndexes);
          break;
        case "INTEGER":
        case "INT64":
          valueBuilder.setIntegerValue((long) columnValue);
          break;
        case "FLOAT":
        case "FLOAT64":
          valueBuilder.setDoubleValue((double) columnValue);
          break;
        case "BOOLEAN":
        case "BOOL":
          valueBuilder.setBooleanValue((boolean) columnValue);
          break;
        case "TIMESTAMP":
          // Convert into milliseconds from the BigQuery timestamp, which is in micro seconds
          long timeInMillis = ((long) columnValue) / 1000;
          valueBuilder.setTimestampValue(Timestamps.fromMillis(timeInMillis));
          break;
        case "DATE":
        case "TIME":
        case "DATETIME":
          // Handle these types as STRING by default, no dedicated type exists in Datastore
          valueBuilder.setStringValue(columnValue.toString());
          break;
        default:
          // TODO: handle nested fields (types "RECORD" or "STRUCT")
          throw new IllegalArgumentException(
              String.format(
                  "Column [%s] of type [%s] not supported.", column.getName(), column.getType()));
      }
      return valueBuilder.build();
    }
  }
}
