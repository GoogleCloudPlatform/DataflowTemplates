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
package com.google.cloud.teleport.v2.transforms;

import com.google.auto.value.AutoValue;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.templates.AvroToSpannerScdPipeline.AvroToSpannerScdOptions.ScdType;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

@AutoValue
public abstract class SpannerScdMutationTransform extends PTransform<PCollection<Struct>, PDone> {

  abstract ScdType scdType();

  abstract Integer batchSize();

  abstract SpannerConfig spannerConfig();

  abstract String tableName();

  abstract List<String> primaryKeyColumnNames();

  @Nullable
  abstract String startDateColumnName();

  abstract String endDateColumnName();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setScdType(ScdType value);

    public abstract Builder setBatchSize(Integer value);

    public abstract Builder setSpannerConfig(SpannerConfig spannerConfig);

    public abstract Builder setTableName(String value);

    public abstract Builder setPrimaryKeyColumnNames(List<String> value);

    public abstract Builder setStartDateColumnName(String value);

    public abstract Builder setEndDateColumnName(String value);

    public abstract SpannerScdMutationTransform build();
  }

  @Override
  public PDone expand(PCollection<Struct> input) {

    PCollection<Iterable<Struct>> batchRecords =
        input.apply("Create batches of struct", MakeBatchesTransform.create(batchSize()));

    switch (scdType()) {
      default:
        throw new UnsupportedOperationException(
            String.format("Only SCD Type 1 and 2 are supported. Found %s.", scdType()));
      case TYPE_1:
        batchRecords.apply(
            "Create Spanner mutations in atomic groups based on data and SCD Type 1",
            ParDo.of(new SpannerScdType1Runner()));
        break;
      case TYPE_2:
        batchRecords.apply(
            "Create Spanner mutations in atomic groups based on data and SCD Type 2",
            ParDo.of(new SpannerScdType2Runner()));
        break;
    }

    return PDone.in(input.getPipeline());
  }

  public static Builder builder() {
    return new AutoValue_SpannerScdMutationTransform.Builder();
  }

  public class SpannerScdType1Runner extends DoFn<Iterable<Struct>, Void> implements Serializable {

    @ProcessElement
    public void writeBatchChanges(@Element Iterable<Struct> recordBatch) {
      SpannerQueryHelper.create(spannerConfig())
          .createDatabaseClient()
          .readWriteTransaction()
          .allowNestedTransaction()
          .run(transaction -> createMutationGroups(recordBatch, transaction));
    }

    public Void createMutationGroups(Iterable<Struct> recordBatch, TransactionContext transaction) {
      recordBatch.forEach(record -> transaction.buffer(createUpsertMutation(record)));
      return null;
    }

    private Mutation createUpsertMutation(Struct record) {
      Mutation.WriteBuilder upsertMutationBuilder = Mutation.newInsertOrUpdateBuilder(tableName());
      record
          .getType()
          .getStructFields()
          .forEach(
              field ->
                  upsertMutationBuilder.set(field.getName()).to(record.getValue(field.getName())));
      return upsertMutationBuilder.build();
    }
  }

  public class SpannerScdType2Runner extends DoFn<Iterable<Struct>, Void> implements Serializable {

    @ProcessElement
    public void writeBatchChanges(@Element Iterable<Struct> recordBatch) {
      SpannerQueryHelper.create(spannerConfig())
          .createDatabaseClient()
          .readWriteTransaction()
          .allowNestedTransaction()
          .run(transaction -> createMutationGroups(recordBatch, transaction));
    }

    public Void createMutationGroups(Iterable<Struct> recordBatch, TransactionContext transaction) {
      com.google.cloud.Timestamp nullTimestamp = null;
      SpannerQueryHelper spannerQueryHelper = SpannerQueryHelper.create(spannerConfig());
      recordBatch.forEach(
          record -> {
            HashMap<com.google.cloud.spanner.Key, Struct> existingRows =
                getMatchingRecords(recordBatch, transaction);

            com.google.cloud.spanner.Key recordKey =
                spannerQueryHelper
                    .addRecordFieldsToKeyBuilder(
                        record, primaryKeyColumnNames(), com.google.cloud.spanner.Key.newBuilder())
                    .append(nullTimestamp) // endTimestamp
                    .build();

            if (existingRows.containsKey(recordKey)) {
              Struct existingRow = existingRows.get(recordKey);
              transaction.buffer(createDeleteOldRowMutation(existingRow));
              transaction.buffer(createInsertOldRowMutation(existingRow));
            }

            transaction.buffer(createInsertNewDataMutation(record));
          });
      return null;
    }

    private HashMap<com.google.cloud.spanner.Key, Struct> getMatchingRecords(
        Iterable<Struct> recordBatch, TransactionContext transaction) {
      com.google.cloud.Timestamp nullTimestamp = null;

      SpannerQueryHelper spannerQueryHelper = SpannerQueryHelper.create(spannerConfig());
      KeySet.Builder keySetBuilder = KeySet.newBuilder();
      recordBatch.forEach(
          record -> {
            com.google.cloud.spanner.Key recordQueryKey =
                spannerQueryHelper
                    .addRecordFieldsToKeyBuilder(
                        record, primaryKeyColumnNames(), com.google.cloud.spanner.Key.newBuilder())
                    .append(nullTimestamp) // endTimestamp
                    .build();
            keySetBuilder.addKey(recordQueryKey);
          });
      KeySet queryKeySet = keySetBuilder.build();

      ResultSet results =
          spannerQueryHelper.queryRecordsWithTransaction(
              transaction, tableName(), queryKeySet, null);
      HashMap<com.google.cloud.spanner.Key, Struct> existingRows = new HashMap<>();
      while (results.next()) {
        Struct resultRow = results.getCurrentRowAsStruct();
        com.google.cloud.spanner.Key resultKey =
            spannerQueryHelper.createKeyForRecord(resultRow, primaryKeyColumnNames());
        existingRows.put(resultKey, resultRow);
      }
      return existingRows;
    }

    private Mutation createDeleteOldRowMutation(Struct record) {
      SpannerQueryHelper spannerQueryHelper = SpannerQueryHelper.create(spannerConfig());
      com.google.cloud.spanner.Key recordKey =
          spannerQueryHelper.createKeyForRecord(record, primaryKeyColumnNames());
      return Mutation.delete(tableName(), recordKey);
    }

    private Mutation createInsertOldRowMutation(Struct record) {
      Mutation.WriteBuilder insertMutationBuilder = Mutation.newInsertBuilder(tableName());
      record.getType().getStructFields().stream()
          .filter(field -> !field.getName().equals(endDateColumnName()))
          .forEach(
              field ->
                  insertMutationBuilder.set(field.getName()).to(record.getValue(field.getName())));
      insertMutationBuilder.set(endDateColumnName()).to(com.google.cloud.Timestamp.now());
      return insertMutationBuilder.build();
    }

    private Mutation createInsertNewDataMutation(Struct record) {
      Mutation.WriteBuilder insertMutationBuilder = Mutation.newInsertBuilder(tableName());
      record
          .getType()
          .getStructFields()
          .forEach(
              field ->
                  insertMutationBuilder.set(field.getName()).to(record.getValue(field.getName())));
      if (startDateColumnName() != null) {
        insertMutationBuilder.set(startDateColumnName()).to(com.google.cloud.Timestamp.now());
      }
      com.google.cloud.Timestamp nullTimestamp = null;
      insertMutationBuilder.set(endDateColumnName()).to(nullTimestamp);
      return insertMutationBuilder.build();
    }

    /** Primary key column names excluding columns used for SCD Typing. */
    private List<String> getBasePrimaryKeyColumnNames() {
      return primaryKeyColumnNames().stream()
          .filter(
              primaryKeyColumnName ->
                  (!primaryKeyColumnName.equals(startDateColumnName())
                      && !primaryKeyColumnName.equals(endDateColumnName())))
          .collect(Collectors.toList());
    }
  }

  @AutoValue
  abstract static class SpannerQueryHelper {

    public static SpannerQueryHelper create(SpannerConfig spannerConfig) {
      return new AutoValue_SpannerScdMutationTransform_SpannerQueryHelper(spannerConfig);
    }

    abstract SpannerConfig spannerConfig();

    private DatabaseClient createDatabaseClient() {
      SpannerAccessor spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig());
      return spannerAccessor.getDatabaseClient();
    }

    private Key createKeyForRecord(Struct record, Iterable<String> primaryKeyColumnNames) {
      Key.Builder keyBuilder = Key.newBuilder();
      addRecordFieldsToKeyBuilder(record, primaryKeyColumnNames, keyBuilder);
      return keyBuilder.build();
    }

    private Key.Builder addRecordFieldsToKeyBuilder(
        Struct record, Iterable<String> columnNames, Key.Builder keyBuilder) {
      HashSet<String> columnNamesSet = new HashSet<>();
      columnNames.forEach(columnNamesSet::add);

      record.getType().getStructFields().stream()
          .filter(field -> columnNamesSet.contains(field.getName()))
          .forEach(
              field -> {
                Value fieldValue = record.getValue(field.getName());
                Type fieldType = fieldValue.getType();

                switch (fieldType.getCode()) {
                    // TODO(Nito): handle isNull() for all data types.
                  case BOOL:
                    keyBuilder.append(fieldValue.getBool());
                    break;
                  case BYTES:
                    keyBuilder.append(fieldValue.getBytes());
                    break;
                  case DATE:
                    keyBuilder.append(fieldValue.getDate());
                    break;
                  case FLOAT32:
                    keyBuilder.append(fieldValue.getFloat32());
                    break;
                  case FLOAT64:
                    keyBuilder.append(fieldValue.getFloat64());
                    break;
                  case INT64:
                    keyBuilder.append(fieldValue.getInt64());
                    break;
                  case JSON:
                    keyBuilder.append(fieldValue.getJson());
                    break;
                  case NUMERIC:
                  case PG_NUMERIC:
                    keyBuilder.append(fieldValue.getNumeric());
                    break;
                  case PG_JSONB:
                    keyBuilder.append(fieldValue.getPgJsonb());
                    break;
                  case STRING:
                    keyBuilder.append(fieldValue.getString());
                    break;
                  case TIMESTAMP:
                    if (fieldValue.isNull()) {
                      Timestamp nullTimestamp = null;
                      keyBuilder.append(nullTimestamp);
                    } else {
                      keyBuilder.append(fieldValue.getTimestamp());
                    }
                    break;
                  default:
                    throw new UnsupportedOperationException(
                        String.format("Unsupported Spanner field type %s.", fieldType.getCode()));
                }
              });
      return keyBuilder;
    }

    private ResultSet queryRecords(String tableName, KeySet queryKeySet, Iterable<String> columns) {
      ReadOnlyTransaction transaction = createDatabaseClient().readOnlyTransaction();
      return queryRecordsWithTransaction(transaction, tableName, queryKeySet, columns);
    }

    private ResultSet queryRecordsWithTransaction(
        ReadContext transaction, String tableName, KeySet queryKeySet, Iterable<String> columns) {
      Iterable<String> queryColumns = columns != null ? columns : getTableColumnNames(tableName);
      return transaction.read(tableName, queryKeySet, queryColumns);
    }

    private Iterable<String> getTableColumnNames(String tableName) {
      DatabaseClient spannerClient = createDatabaseClient();
      String schemaQuery =
          String.format(
              "SELECT COLUMN_NAME FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE TABLE_NAME = \"%s\"",
              tableName);
      ResultSet results =
          spannerClient.readOnlyTransaction().executeQuery(Statement.of(schemaQuery));

      ArrayList<String> columnNames = new ArrayList<>();
      while (results.next()) {
        Struct rowStruct = results.getCurrentRowAsStruct();
        columnNames.add(rowStruct.getString("COLUMN_NAME"));
      }
      return columnNames;
    }
  }
}
