/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.v2.cdc.merge;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.joda.time.Duration;

/** Class {@link MergeConfiguration}. */
@AutoValue
public abstract class MergeConfiguration implements Serializable {
  /**
   * The top-level template for building merge statements to be issued to BigQuery.
   *
   * <p>The way to read it is:
   *
   * <p>Merge the REPLICA table Using a view of the STAGING table containing the latest changes On a
   * join condition (join on all the columns of the primary key). If there is a match, and there has
   * been a deletion: -- This means that both STAGING and REPLICA tables contain the primary key,
   * and that the latest change in that primary key is a deletion. <b>Therefore delete the row.</b>
   * If there is a match, and the STAGING table contains a newer record: -- This means that both
   * STAGING and REPLICA tables contain the primary key, and that the STAGING table contains a newer
   * version of the data. <b>Therefore, update the row.</b> If there has not been a match: -- This
   * means that the REPLICA table does not contain a row that is contained in the STAGING table.
   * <b>Therefore, insert this new row.</b>
   */
  private static final String DEFAULT_MERGE_QUERY_TEMPLATE =
      String.join(
          "",
          "MERGE `{replicaTable}` AS {replicaAlias} ",
          "USING ({stagingViewSql}) AS {stagingAlias} ",
          "ON {joinCondition} ",
          "WHEN MATCHED AND {sortFieldsCompareSql} AND {stagingAlias}.{deleteColumn}=True THEN"
              + " DELETE ", // TODO entire block should be configurably removed
          "WHEN MATCHED AND {sortFieldsCompareSql} THEN {mergeUpdateSql} ",
          "WHEN NOT MATCHED BY TARGET AND {stagingAlias}.{deleteColumn}!=True ",
          "THEN {mergeInsertSql}");

  private static final Boolean DEFAULT_SUPPORT_PARTITIONED_TABLES = true;
  private static final Integer DEFAULT_PARTITION_RETENTION_DAYS = 1;
  private static final Duration DEFAULT_MERGE_WINDOW_DURATION = Duration.standardMinutes(30);
  private static final int DEFAULT_MERGE_CONCURRENCY = 30;

  // BigQuery-specific properties
  public static final String BIGQUERY_QUOTE_CHARACTER = "`";

  public abstract String quoteCharacter();

  public abstract Boolean supportPartitionedTables();

  public abstract String mergeQueryTemplate();

  public abstract Integer partitionRetention();

  public abstract Duration mergeWindowDuration();

  public abstract int mergeConcurrency();

  public static MergeConfiguration bigQueryConfiguration() {
    return MergeConfiguration.builder().setQuoteCharacter(BIGQUERY_QUOTE_CHARACTER).build();
  }

  public MergeConfiguration withPartitionRetention(int partitionRetention) {
    return this.toBuilder().setPartitionRetention(Integer.valueOf(partitionRetention)).build();
  }

  public MergeConfiguration withMergeWindowDuration(Duration duration) {
    return this.toBuilder().setMergeWindowDuration(duration).build();
  }

  public MergeConfiguration withMergeConcurrency(int mergeConcurrency) {
    return this.toBuilder().setMergeConcurrency(mergeConcurrency).build();
  }

  public abstract Builder toBuilder();

  static Builder builder() {
    return new AutoValue_MergeConfiguration.Builder()
        .setMergeQueryTemplate(DEFAULT_MERGE_QUERY_TEMPLATE)
        .setSupportPartitionedTables(DEFAULT_SUPPORT_PARTITIONED_TABLES)
        .setPartitionRetention(DEFAULT_PARTITION_RETENTION_DAYS)
        .setSupportPartitionedTables(true)
        .setMergeWindowDuration(DEFAULT_MERGE_WINDOW_DURATION)
        .setMergeConcurrency(DEFAULT_MERGE_CONCURRENCY);
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setQuoteCharacter(String quote);

    abstract Builder setSupportPartitionedTables(Boolean supportPartitionedTables);

    abstract Builder setMergeQueryTemplate(String mergeQueryTemplate);

    abstract Builder setPartitionRetention(Integer partitionRetention);

    abstract Builder setMergeWindowDuration(Duration mergeWindowDuration);

    abstract Builder setMergeConcurrency(int mergeConcurrency);

    abstract MergeConfiguration build();
  }
}
