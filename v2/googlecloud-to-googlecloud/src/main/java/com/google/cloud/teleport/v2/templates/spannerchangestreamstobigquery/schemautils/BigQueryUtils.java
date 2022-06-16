/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.schemautils;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.Timestamp;
import com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model.Mod;
import java.util.HashSet;
import java.util.Set;

/** {@link BigQueryUtils} providdes utils for processing BigQuery schema. */
public class BigQueryUtils {

  public static final String BQ_CHANGELOG_FIELD_NAME_ORIGINAL_PAYLOAD_JSON =
      "_metadata_spanner_original_payload_json";
  // TODO(haikuo-google): Create static variables for "_metadata_error" and "_metadata_retry_count"
  // in com.google.cloud.teleport.v2.cdc.dlq.FileBasedDeadLetterQueueReconsumer and use them here.
  public static final String BQ_CHANGELOG_FIELD_NAME_ERROR = "_metadata_error";
  public static final String BQ_CHANGELOG_FIELD_NAME_RETRY_COUNT = "_metadata_retry_count";
  public static final String BQ_CHANGELOG_FIELD_NAME_MOD_TYPE = "_metadata_spanner_mod_type";
  public static final String BQ_CHANGELOG_FIELD_NAME_TABLE_NAME = "_metadata_spanner_table_name";
  public static final String BQ_CHANGELOG_FIELD_NAME_SPANNER_COMMIT_TIMESTAMP =
      "_metadata_spanner_commit_timestamp";
  public static final String BQ_CHANGELOG_FIELD_NAME_SERVER_TRANSACTION_ID =
      "_metadata_spanner_server_transaction_id";
  public static final String BQ_CHANGELOG_FIELD_NAME_RECORD_SEQUENCE =
      "_metadata_spanner_record_sequence";
  public static final String BQ_CHANGELOG_FIELD_NAME_IS_LAST_RECORD_IN_TRANSACTION_IN_PARTITION =
      "_metadata_spanner_is_last_record_in_transaction_in_partition";
  public static final String BQ_CHANGELOG_FIELD_NAME_NUMBER_OF_RECORDS_IN_TRANSACTION =
      "_metadata_spanner_number_of_records_in_transaction";
  public static final String BQ_CHANGELOG_FIELD_NAME_NUMBER_OF_PARTITIONS_IN_TRANSACTION =
      "_metadata_spanner_number_of_partitions_in_transaction";
  public static final String BQ_CHANGELOG_FIELD_NAME_BIGQUERY_COMMIT_TIMESTAMP =
      "_metadata_big_query_commit_timestamp";

  /**
   * @return the fields that are only used intermediately in the pipeline and are not corresponding
   *     to Spanner columns.
   */
  public static Set<String> getBigQueryIntermediateMetadataFieldNames() {
    Set<String> fieldNames = new HashSet<>();
    fieldNames.add(BQ_CHANGELOG_FIELD_NAME_ERROR);
    fieldNames.add(BQ_CHANGELOG_FIELD_NAME_RETRY_COUNT);
    fieldNames.add(BQ_CHANGELOG_FIELD_NAME_ORIGINAL_PAYLOAD_JSON);
    return fieldNames;
  }

  public static void setMetadataFiledsOfTableRow(
      String spannerTableName,
      Mod mod,
      String modJsonString,
      Timestamp spannerCommitTimestamp,
      TableRow tableRow) {
    tableRow.set(BQ_CHANGELOG_FIELD_NAME_ORIGINAL_PAYLOAD_JSON, modJsonString);
    tableRow.set(BQ_CHANGELOG_FIELD_NAME_MOD_TYPE, mod.getModType().name());
    tableRow.set(BQ_CHANGELOG_FIELD_NAME_TABLE_NAME, spannerTableName);
    tableRow.set(
        BQ_CHANGELOG_FIELD_NAME_SPANNER_COMMIT_TIMESTAMP, spannerCommitTimestamp.toString());
    tableRow.set(BQ_CHANGELOG_FIELD_NAME_SERVER_TRANSACTION_ID, mod.getServerTransactionId());
    tableRow.set(BQ_CHANGELOG_FIELD_NAME_RECORD_SEQUENCE, mod.getRecordSequence());
    tableRow.set(
        BQ_CHANGELOG_FIELD_NAME_IS_LAST_RECORD_IN_TRANSACTION_IN_PARTITION,
        mod.getIsLastRecordInTransactionInPartition());
    tableRow.set(
        BQ_CHANGELOG_FIELD_NAME_NUMBER_OF_RECORDS_IN_TRANSACTION,
        mod.getNumberOfRecordsInTransaction());
    tableRow.set(
        BQ_CHANGELOG_FIELD_NAME_NUMBER_OF_PARTITIONS_IN_TRANSACTION,
        mod.getNumberOfPartitionsInTransaction());
    tableRow.set(BigQueryUtils.BQ_CHANGELOG_FIELD_NAME_BIGQUERY_COMMIT_TIMESTAMP, "AUTO");
  }
}
