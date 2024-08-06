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
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.teleport.v2.templates.AvroToSpannerScdPipeline.AvroToSpannerScdOptions.ScdType;
import com.google.cloud.teleport.v2.utils.SpannerQueryHelper;
import com.google.cloud.teleport.v2.utils.StructValueHelper.CommonValues;
import com.google.cloud.teleport.v2.utils.StructValueHelper.NullTypes;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * Writes batch rows into Spanner using the defined SCD Type.
 *
 * <ul>
 *   <li>
 *     SCD Type 1: if primary key(s) exist, updates the existing row; it inserts a new row
 *     otherwise.
 *   </li>
 *   <li>
 *     SCD Type 2: if primary key(s) exist, updates the end timestamp to the current timestamp.
 *     Note: since end timestamp is part of the primary key, it requires delete and insert to
 *     achieve this. In all cases, it inserts a new row with the new data and null end timestamp.
 *     If start timestamp column is specified, it sets it to the current timestamp when inserting.
 *   </li>
 * </ul>
 */
@AutoValue
public abstract class SpannerScdMutationTransform
    extends PTransform<PCollection<Iterable<Struct>>, PDone> {

  abstract ScdType scdType();

  abstract SpannerConfig spannerConfig();

  abstract String tableName();

  abstract List<String> primaryKeyColumnNames();

  @Nullable
  abstract String startDateColumnName();

  abstract String endDateColumnName();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setScdType(ScdType value);

    public abstract Builder setSpannerConfig(SpannerConfig spannerConfig);

    public abstract Builder setTableName(String value);

    public abstract Builder setPrimaryKeyColumnNames(List<String> value);

    public abstract Builder setStartDateColumnName(String value);

    public abstract Builder setEndDateColumnName(String value);

    public abstract SpannerScdMutationTransform build();
  }

  @Override
  public PDone expand(PCollection<Iterable<Struct>> input) {

    switch (scdType()) {
      default:
        throw new UnsupportedOperationException(
            String.format("Only SCD Type 1 and 2 are supported. Found %s.", scdType()));
      case TYPE_1:
        input.apply("WriteScdType1ToSpanner", ParDo.of(new SpannerScdType1Runner()));
        break;
      case TYPE_2:
        input.apply("WriteScdType2ToSpanner", ParDo.of(new SpannerScdType2Runner()));
        break;
    }

    return PDone.in(input.getPipeline());
  }

  public static Builder builder() {
    return new AutoValue_SpannerScdMutationTransform.Builder();
  }

  /**
   * Runs SCD Type 1 mutations to Spanner.
   *
   * <p>If primary key(s) exist, updates the existing row; it inserts a new row otherwise.
   */
  class SpannerScdType1Runner extends DoFn<Iterable<Struct>, Void> implements Serializable {

    @ProcessElement
    public void writeBatchChanges(@Element Iterable<Struct> recordBatch) {
      SpannerQueryHelper.create(spannerConfig())
          .createDatabaseClient()
          .readWriteTransaction()
          .allowNestedTransaction()
          .run(transaction -> createMutationGroups(recordBatch, transaction));
    }

    /**
     * Creates the mutations required for the batch of records for SCD Type 1.
     *
     * Only upsert is required.
     * @param recordBatch
     * @param transaction
     */
    private Void createMutationGroups(
        Iterable<Struct> recordBatch, TransactionContext transaction) {
      recordBatch.forEach(record -> transaction.buffer(createUpsertMutation(record)));
      return null;
    }

    /**
     * Creates an upsert (insertOrUpdate) mutation for the given record.
     * @param record
     * @return Spanner upsert mutation performed within the transaction.
     */
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

  /**
   * Runs SCD Type 2 mutations to Spanner.
   *
   * <p>If primary key(s) exist, updates the end timestamp to the current timestamp.
   * Note: since end timestamp is part of the primary key, it requires delete and insert to
   * achieve this.
   *
   * <p>In all cases, it inserts a new row with the new data and null end timestamp.
   * If start timestamp column is specified, it sets it to the current timestamp when inserting.
   */
  class SpannerScdType2Runner extends DoFn<Iterable<Struct>, Void> implements Serializable {

    @ProcessElement
    public void writeBatchChanges(@Element Iterable<Struct> recordBatch) {
      SpannerQueryHelper.create(spannerConfig())
          .createDatabaseClient()
          .readWriteTransaction()
          .allowNestedTransaction()
          .run(transaction -> createMutationGroups(recordBatch, transaction));
    }

    /**
     * Creates the mutations required for the batch of records for SCD Type 2.
     *
     * Update (insert and delete) of existing (old) data is required if the row exists.
     * Insert of new data is required for all cases.
     * @param recordBatch
     * @param transaction
     */
    private Void createMutationGroups(
        Iterable<Struct> recordBatch, TransactionContext transaction) {
      SpannerQueryHelper spannerQueryHelper = SpannerQueryHelper.create(spannerConfig());
      recordBatch.forEach(
          record -> {
            com.google.cloud.Timestamp currentTimestamp = CommonValues.currentTimestamp();
            HashMap<com.google.cloud.spanner.Key, Struct> existingRows =
                getMatchingRecords(recordBatch, transaction);

            com.google.cloud.spanner.Key recordKey =
                spannerQueryHelper
                    .addRecordFieldsToKeyBuilder(
                        record, primaryKeyColumnNames(), com.google.cloud.spanner.Key.newBuilder())
                    .append(NullTypes.NULL_TIMESTAMP) // endTimestamp
                    .build();

            if (existingRows.containsKey(recordKey)) {
              Struct existingRow = existingRows.get(recordKey);
              transaction.buffer(createDeleteOldRowMutation(existingRow));
              transaction.buffer(createInsertOldRowMutation(existingRow, currentTimestamp));
            }

            transaction.buffer(createInsertNewDataMutation(record, currentTimestamp));
          });
      return null;
    }

    /**
     * Gets the matching rows in the Spanner table for the given batch of records.
     * @param recordBatch
     * @param transaction Transaction in which to operate the database read.
     * @return Map of the matching rows' Keys to the matching rows' Structs.
     */
    private HashMap<com.google.cloud.spanner.Key, Struct> getMatchingRecords(
        Iterable<Struct> recordBatch, TransactionContext transaction) {
      SpannerQueryHelper spannerQueryHelper = SpannerQueryHelper.create(spannerConfig());
      KeySet.Builder keySetBuilder = KeySet.newBuilder();
      recordBatch.forEach(
          record -> {
            com.google.cloud.spanner.Key recordQueryKey =
                spannerQueryHelper
                    .addRecordFieldsToKeyBuilder(
                        record, primaryKeyColumnNames(), com.google.cloud.spanner.Key.newBuilder())
                    .append(NullTypes.NULL_TIMESTAMP) // endTimestamp
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

    /**
     * Creates a deletion mutation for the existing given record.
     * Required since it is not possible to update primary keys.
     * @param record
     * @return Spanner mutation performed within the transaction.
     */
    private Mutation createDeleteOldRowMutation(Struct record) {
      SpannerQueryHelper spannerQueryHelper = SpannerQueryHelper.create(spannerConfig());
      com.google.cloud.spanner.Key recordKey =
          spannerQueryHelper.createKeyForRecord(record, primaryKeyColumnNames());
      return Mutation.delete(tableName(), recordKey);
    }

    /**
     * Creates an insert mutation for the existing given record.
     * Required since it is not possible to update primary keys.
     * @param record
     * @return Spanner mutation performed within the transaction.
     */
    private Mutation createInsertOldRowMutation(
        Struct record, com.google.cloud.Timestamp currentTimestamp) {
      Mutation.WriteBuilder insertMutationBuilder = Mutation.newInsertBuilder(tableName());
      record.getType().getStructFields().stream()
          .filter(field -> !field.getName().equals(endDateColumnName()))
          .forEach(
              field ->
                  insertMutationBuilder.set(field.getName()).to(record.getValue(field.getName())));
      insertMutationBuilder.set(endDateColumnName()).to(currentTimestamp);
      return insertMutationBuilder.build();
    }

    /**
     * Creates an insert mutation for the new given record.
     * @param record
     * @return Spanner mutation performed within the transaction.
     */
    private Mutation createInsertNewDataMutation(
        Struct record, com.google.cloud.Timestamp currentTimestamp) {
      Mutation.WriteBuilder insertMutationBuilder = Mutation.newInsertBuilder(tableName());
      record
          .getType()
          .getStructFields()
          .forEach(
              field ->
                  insertMutationBuilder.set(field.getName()).to(record.getValue(field.getName())));
      if (startDateColumnName() != null) {
        insertMutationBuilder.set(startDateColumnName()).to(currentTimestamp);
      }
      insertMutationBuilder.set(endDateColumnName()).to(NullTypes.NULL_TIMESTAMP);
      return insertMutationBuilder.build();
    }
  }
}
