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
package com.google.cloud.teleport.v2.templates.spannerchangestreamstobigquery.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.Timestamp;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ValueCaptureType;

/**
 * The {@link Mod} contains the keys, new values (from {@link
 * org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod}) and metadata ({@link
 * org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord}) of a Spanner row. Note
 * it's different from the {@link org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod}.
 */
@DefaultCoder(AvroCoder.class)
public final class Mod implements Serializable {

  private static final long serialVersionUID = 8703257194338184299L;

  private String keysJson;
  private String newValuesJson;
  private long commitTimestampSeconds;
  private int commitTimestampNanos;
  private String serverTransactionId;
  private boolean isLastRecordInTransactionInPartition;
  private String recordSequence;
  private String tableName;
  private ModType modType;
  private ValueCaptureType valueCaptureType;
  private long numberOfRecordsInTransaction;
  private long numberOfPartitionsInTransaction;

  /** Default constructor for serialization only. */
  private Mod() {}

  /**
   * @param keysJson JSON object as String, where the keys are the primary key column names and the
   *     values are the primary key column values
   * @param newValuesJson JSON object as String, displaying the new state of the columns modified.
   *     This JSON object can be null in the case of a DELETE
   * @param commitTimestamp the timestamp at which the modifications within were committed in Cloud
   *     Spanner
   * @param serverTransactionId the unique transaction id in which the modifications occurred
   * @param isLastRecordInTransactionInPartition indicates whether this record is the last emitted
   *     for the given transaction in the given partition
   * @param recordSequence indicates the order in which this modification was made in the original
   *     Spanner transaction. The value is unique and monotonically increasing in the context of a
   *     particular serverTransactionId
   * @param tableName the name of the table in which the modifications occurred
   * @param modType the operation that caused the modification to occur, can only be one of INSERT,
   *     UPDATE and DELETE
   * @param valueCaptureType the value capture type of the change stream
   * @param numberOfRecordsInTransaction the total number of records for the given transaction
   * @param numberOfPartitionsInTransaction the total number of partitions within the given
   *     transaction
   * @return a {@link Mod} corresponding to a {@link
   *     org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod}. The constructed {@link Mod} is
   *     used as the processing unit of the pipeline, it contains all the information from {@link
   *     org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord} and {@link
   *     org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod}, except columns JSON, since we
   *     will read the columns from Spanner instead.
   */
  public Mod(
      String keysJson,
      String newValuesJson,
      Timestamp commitTimestamp,
      String serverTransactionId,
      boolean isLastRecordInTransactionInPartition,
      String recordSequence,
      String tableName,
      ModType modType,
      ValueCaptureType valueCaptureType,
      long numberOfRecordsInTransaction,
      long numberOfPartitionsInTransaction) {
    this.keysJson = keysJson;
    this.newValuesJson = newValuesJson;
    this.commitTimestampSeconds = commitTimestamp.getSeconds();
    this.commitTimestampNanos = commitTimestamp.getNanos();
    this.serverTransactionId = serverTransactionId;
    this.isLastRecordInTransactionInPartition = isLastRecordInTransactionInPartition;
    this.recordSequence = recordSequence;
    this.tableName = tableName;
    this.modType = modType;
    this.valueCaptureType = valueCaptureType;
    this.numberOfRecordsInTransaction = numberOfRecordsInTransaction;
    this.numberOfPartitionsInTransaction = numberOfPartitionsInTransaction;
  }

  public static Mod fromJson(String json) throws IOException {
    return new ObjectMapper().readValue(json, Mod.class);
  }

  /**
   * The primary keys of this specific modification. This is always present and can not be null. The
   * keys are returned as a JSON object (stringified), where the keys are the column names and the
   * values are the column values.
   *
   * @return JSON object as String representing the primary key state for the row modified
   */
  public String getKeysJson() {
    return keysJson;
  }

  /**
   * The new column values after the modification was applied. This can be null when the
   * modification was emitted for a DELETE operation. The values are returned as a JSON object
   * (stringified), where the keys are the column names and the values are the column values.
   *
   * @return JSON object as String representing the new column values after the row was modified
   */
  public @Nullable String getNewValuesJson() {
    return newValuesJson;
  }

  /**
   * The seconds part of the timestamp at which the modifications within were committed in Cloud
   * Spanner.
   */
  public long getCommitTimestampSeconds() {
    return commitTimestampSeconds;
  }

  /**
   * The nanoseconds part of the timestamp at which the modifications within were committed in Cloud
   * Spanner.
   */
  public int getCommitTimestampNanos() {
    return commitTimestampNanos;
  }

  /** The unique transaction id in which the modifications occurred. */
  public String getServerTransactionId() {
    return serverTransactionId;
  }

  /**
   * Indicates whether this record is the last emitted for the given transaction in the given
   * partition.
   */
  public boolean getIsLastRecordInTransactionInPartition() {
    return isLastRecordInTransactionInPartition;
  }

  /**
   * indicates the order in which this modification was made in the original Spanner transaction.
   * The value is unique and monotonically increasing in the context of a particular
   * serverTransactionId.
   */
  public String getRecordSequence() {
    return recordSequence;
  }

  /** The name of the table in which the modifications within this record occurred. */
  public String getTableName() {
    return tableName;
  }

  /** The type of operation that caused the modifications within this record. */
  public ModType getModType() {
    return modType;
  }

  /** The value capture type that defined the values in data change records. */
  public ValueCaptureType getValueCaptureType() {
    return valueCaptureType;
  }

  /** The total number of data change records for the given transaction. */
  public long getNumberOfRecordsInTransaction() {
    return numberOfRecordsInTransaction;
  }

  /** The total number of partitions for the given transaction. */
  public long getNumberOfPartitionsInTransaction() {
    return numberOfPartitionsInTransaction;
  }

  @Override
  public boolean equals(@javax.annotation.Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Mod)) {
      return false;
    }
    Mod that = (Mod) o;
    return keysJson == that.keysJson
        && isLastRecordInTransactionInPartition == that.isLastRecordInTransactionInPartition
        && numberOfRecordsInTransaction == that.numberOfRecordsInTransaction
        && numberOfPartitionsInTransaction == that.numberOfPartitionsInTransaction
        && commitTimestampSeconds == that.commitTimestampSeconds
        && commitTimestampNanos == that.commitTimestampNanos
        && Objects.equals(serverTransactionId, that.serverTransactionId)
        && Objects.equals(recordSequence, that.recordSequence)
        && Objects.equals(tableName, that.tableName)
        && modType == that.modType
        && valueCaptureType == that.valueCaptureType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        keysJson,
        commitTimestampSeconds,
        commitTimestampNanos,
        serverTransactionId,
        isLastRecordInTransactionInPartition,
        recordSequence,
        tableName,
        modType,
        valueCaptureType,
        numberOfRecordsInTransaction,
        numberOfPartitionsInTransaction);
  }

  @Override
  public String toString() {
    return "Mod{"
        + "keysJson='"
        + keysJson
        + '\''
        + ", commitTimestampSeconds="
        + commitTimestampSeconds
        + ", commitTimestampNanos="
        + commitTimestampNanos
        + ", serverTransactionId='"
        + serverTransactionId
        + '\''
        + ", isLastRecordInTransactionInPartition="
        + isLastRecordInTransactionInPartition
        + ", recordSequence='"
        + recordSequence
        + '\''
        + ", tableName='"
        + tableName
        + '\''
        + ", modType="
        + modType
        + ", valueCaptureType="
        + valueCaptureType
        + ", numberOfRecordsInTransaction="
        + numberOfRecordsInTransaction
        + ", numberOfPartitionsInTransaction="
        + numberOfPartitionsInTransaction
        + '}';
  }

  public String toJson() throws JsonProcessingException {
    return new ObjectMapper().writeValueAsString(this);
  }
}
