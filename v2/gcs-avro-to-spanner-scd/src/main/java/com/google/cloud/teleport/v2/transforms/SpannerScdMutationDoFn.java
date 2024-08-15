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

import com.google.api.gax.retrying.RetrySettings;
import com.google.auto.value.AutoValue;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TransactionContext;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.templates.AvroToSpannerScdPipeline.AvroToSpannerScdOptions.ScdType;
import com.google.cloud.teleport.v2.utils.StructHelper;
import com.google.cloud.teleport.v2.utils.StructHelper.ValueHelper;
import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.transforms.DoFn;
import org.threeten.bp.Duration;

@AutoValue
abstract class SpannerScdMutationDoFn extends DoFn<Iterable<Struct>, Void> {

  abstract ScdType scdType();

  abstract SpannerConfig spannerConfig();

  abstract String tableName();

  @Nullable
  abstract List<String> primaryKeyColumnNames();

  @Nullable
  abstract String startDateColumnName();

  @Nullable
  abstract String endDateColumnName();

  abstract ImmutableList<String> tableColumnNames();

  private transient SpannerAccessor spannerAccessor;

  private transient CurrentTimestampGetter currentTimestampGetter = new CurrentTimestampGetter();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setScdType(ScdType value);

    public abstract Builder setSpannerConfig(SpannerConfig value);

    public abstract Builder setTableName(String value);

    public abstract Builder setPrimaryKeyColumnNames(List<String> value);

    public abstract Builder setStartDateColumnName(String value);

    public abstract Builder setEndDateColumnName(String value);

    public abstract Builder setTableColumnNames(ImmutableList<String> value);

    public abstract SpannerScdMutationDoFn build();
  }

  public static Builder builder() {
    return new AutoValue_SpannerScdMutationDoFn.Builder();
  }

  SpannerScdMutationDoFn setSpannerAccessor(SpannerAccessor spannerAccessor) {
    this.spannerAccessor = spannerAccessor;
    return this;
  }

  SpannerScdMutationDoFn setCurrentTimestampGetter(CurrentTimestampGetter currentTimestampGetter) {
    this.currentTimestampGetter = currentTimestampGetter;
    return this;
  }

  interface SpannerScdTypeRunner {
    /**
     * Buffers the required mutations for the batch of records within the transaction.
     *
     * <p>Takes a transaction context and adds the required mutations for the given SCD Type for all
     * the records in the batch.
     *
     * @param transactionContext Transaction where mutations will be executed.
     * @param recordBatch Batch of records for which mutations will be created.
     */
    Void bufferMutations(TransactionContext transactionContext, Iterable<Struct> recordBatch);
  }

  @Setup
  public void setup() throws Exception {
    RetrySettings retrySettings =
        RetrySettings.newBuilder()
            .setInitialRpcTimeout(Duration.ofHours(2))
            .setMaxRpcTimeout(Duration.ofHours(2))
            .setTotalTimeout(Duration.ofHours(2))
            .setRpcTimeoutMultiplier(1.0)
            .setInitialRetryDelay(Duration.ofSeconds(2))
            .setMaxRetryDelay(Duration.ofSeconds(60))
            .setRetryDelayMultiplier(1.5)
            .setMaxAttempts(100)
            .build();
    // This property sets the default timeout between 2 response packets in the client library.
    System.setProperty("com.google.cloud.spanner.watchdogTimeoutSeconds", "7200");
    spannerAccessor =
        SpannerAccessor.getOrCreate(
            spannerConfig().withExecuteStreamingSqlRetrySettings(retrySettings));
  }

  @Teardown
  public void teardown() throws Exception {
    spannerAccessor.close();
  }

  @ProcessElement
  public void writeBatchChanges(@Element Iterable<Struct> recordBatch) {
    spannerAccessor
        .getDatabaseClient()
        .readWriteTransaction()
        .allowNestedTransaction()
        .run(
            transaction -> {
              chooseAndBuildScdTypeRunner().bufferMutations(transaction, recordBatch);
              return null;
            });
  }

  private SpannerScdTypeRunner chooseAndBuildScdTypeRunner() {
    switch (scdType()) {
      case TYPE_1:
        return new SpannerSpannerScdType1TypeRunner();
      case TYPE_2:
        return new SpannerSpannerScdType2TypeRunner();
      default:
        throw new UnsupportedOperationException(
            String.format("Only SCD Type 1 and 2 are supported. Found %s.", scdType()));
    }
  }

  /**
   * Runs SCD Type 1 mutations to Spanner.
   *
   * <p>If primary key(s) exist, updates the existing row; it inserts a new row otherwise.
   */
  private class SpannerSpannerScdType1TypeRunner implements SpannerScdTypeRunner {
    /**
     * Buffers the mutations required for the batch of records for SCD Type 1.
     *
     * <p>Only upsert is required for each of the records.
     */
    @Nullable
    @Override
    public Void bufferMutations(TransactionContext transaction, Iterable<Struct> recordBatch) {
      recordBatch.forEach(record -> transaction.buffer(createUpsertMutation(record)));
      return null;
    }

    /**
     * Creates an upsert (insertOrUpdate) mutation for the given record.
     *
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
   * <p>If primary key(s) exist, updates the end timestamp to the current timestamp. Note: since end
   * timestamp is part of the primary key, it requires delete and insert to achieve this.
   *
   * <p>In all cases, it inserts a new row with the new data and null end timestamp. If start
   * timestamp column is specified, it sets it to the current timestamp when inserting.
   */
  private class SpannerSpannerScdType2TypeRunner implements SpannerScdTypeRunner {
    /**
     * Buffers the mutations required for the batch of records for SCD Type 2.
     *
     * <p>Update (insert and delete) of existing (old) data is required if the row exists. Insert of
     * new data is required for all cases.
     */
    @Override
    public Void bufferMutations(TransactionContext transaction, Iterable<Struct> recordBatch) {
      recordBatch.forEach(
          record -> {
            com.google.cloud.Timestamp currentTimestamp = currentTimestampGetter.get();
            HashMap<com.google.cloud.spanner.Key, Struct> existingRows =
                getMatchingRecords(recordBatch, transaction);
            com.google.cloud.spanner.Key recordKey =
                StructHelper.of(record)
                    .keyMaker(
                        primaryKeyColumnNames().stream()
                            .filter(columnName -> columnName != endDateColumnName())
                            .collect(Collectors.toList()))
                    .createKeyWithExtraValues(
                        /* endTimestamp= */ Value.timestamp(ValueHelper.NullTypes.NULL_TIMESTAMP));

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
     *
     * @param transaction Transaction in which to operate the database read.
     * @return Map of the matching rows' Keys to the matching rows' Structs.
     */
    private HashMap<com.google.cloud.spanner.Key, Struct> getMatchingRecords(
        Iterable<Struct> recordBatch, TransactionContext transaction) {
      KeySet.Builder keySetBuilder = KeySet.newBuilder();
      recordBatch.forEach(
          record -> {
            com.google.cloud.spanner.Key recordQueryKey =
                StructHelper.of(record)
                    .keyMaker(
                        primaryKeyColumnNames().stream()
                            .filter(columnName -> columnName != endDateColumnName())
                            .collect(Collectors.toList()))
                    .createKeyWithExtraValues(
                        /* endTimestamp= */ Value.timestamp(ValueHelper.NullTypes.NULL_TIMESTAMP));
            keySetBuilder.addKey(recordQueryKey);
          });
      KeySet queryKeySet = keySetBuilder.build();

      ResultSet results = transaction.read(tableName(), queryKeySet, tableColumnNames());

      HashMap<com.google.cloud.spanner.Key, Struct> existingRows = new HashMap<>();
      while (results.next()) {
        Struct resultRow = results.getCurrentRowAsStruct();
        com.google.cloud.spanner.Key resultKey =
            StructHelper.of(resultRow).keyMaker(primaryKeyColumnNames()).createKey();
        existingRows.put(resultKey, resultRow);
      }

      return existingRows;
    }

    /**
     * Creates a deletion mutation for the existing given record. Required since it is not possible
     * to update primary keys.
     *
     * @return Spanner mutation performed within the transaction.
     */
    private Mutation createDeleteOldRowMutation(Struct record) {
      com.google.cloud.spanner.Key recordKey =
          StructHelper.of(record).keyMaker(primaryKeyColumnNames()).createKey();
      return Mutation.delete(tableName(), recordKey);
    }

    /**
     * Creates an insert mutation for the existing given record. Required since it is not possible
     * to update primary keys.
     *
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
     *
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
      insertMutationBuilder.set(endDateColumnName()).to(ValueHelper.NullTypes.NULL_TIMESTAMP);
      return insertMutationBuilder.build();
    }
  }

  public static class CurrentTimestampGetter {
    public com.google.cloud.Timestamp get() {
      return com.google.cloud.Timestamp.now();
    }
  }
}
