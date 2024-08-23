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
package com.google.cloud.teleport.v2.templates.changestream;

import com.google.cloud.Timestamp;
import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;

/**
 * Trimmed version of the Apache Beam DataChangeRecord class that only contains the field we need in
 * this pipeline.
 */
@SuppressWarnings("initialization.fields.uninitialized") // Avro requires the default constructor
public class TrimmedShardedDataChangeRecord extends java.lang.Object implements Serializable {
  private Timestamp commitTimestamp;
  private String serverTransactionId;
  private String recordSequence;
  private String tableName;
  private Mod mod;
  private ModType modType;
  private long numberOfRecordsInTransaction;
  private String transactionTag;
  private String shard;
  private boolean isRetryRecord;

  public TrimmedShardedDataChangeRecord(
      com.google.cloud.Timestamp commitTimestamp,
      String serverTransactionId,
      String recordSequence,
      String tableName,
      Mod mod,
      ModType modType,
      long numberOfRecordsInTransaction,
      String transactionTag) {
    this.commitTimestamp = commitTimestamp;
    this.serverTransactionId = serverTransactionId;
    this.recordSequence = recordSequence;
    this.tableName = tableName;
    this.mod = mod;
    this.modType = modType;
    this.numberOfRecordsInTransaction = numberOfRecordsInTransaction;
    this.transactionTag = transactionTag;
    this.isRetryRecord = false;
  }

  public TrimmedShardedDataChangeRecord(TrimmedShardedDataChangeRecord other) {
    this.commitTimestamp = other.commitTimestamp;
    this.serverTransactionId = other.serverTransactionId;
    this.recordSequence = other.recordSequence;
    this.tableName = other.tableName;
    this.mod = other.mod;
    this.modType = other.modType;
    this.numberOfRecordsInTransaction = other.numberOfRecordsInTransaction;
    this.transactionTag = other.transactionTag;
    this.shard = other.shard;
    this.isRetryRecord = other.isRetryRecord;
  }

  public Timestamp getCommitTimestamp() {
    return commitTimestamp;
  }

  public String getServerTransactionId() {
    return serverTransactionId;
  }

  public String getRecordSequence() {
    return recordSequence;
  }

  public String getTableName() {
    return tableName;
  }

  public Mod getMod() {
    return mod;
  }

  public ModType getModType() {
    return modType;
  }

  public long getNumberOfRecordsInTransaction() {
    return numberOfRecordsInTransaction;
  }

  public String getTransactionTag() {
    return transactionTag;
  }

  public void setShard(String shard) {
    this.shard = shard;
  }

  public String getShard() {
    return shard;
  }

  public boolean isRetryRecord() {
    return isRetryRecord;
  }

  public void setRetryRecord(boolean isRetryRecord) {
    this.isRetryRecord = isRetryRecord;
  }

  @Override
  public boolean equals(@javax.annotation.Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TrimmedShardedDataChangeRecord)) {
      return false;
    }
    TrimmedShardedDataChangeRecord that = (TrimmedShardedDataChangeRecord) o;
    return Objects.equals(commitTimestamp, that.commitTimestamp)
        && Objects.equals(serverTransactionId, that.serverTransactionId)
        && Objects.equals(recordSequence, that.recordSequence)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(mod, that.mod)
        && modType == that.modType
        && numberOfRecordsInTransaction == that.numberOfRecordsInTransaction
        && Objects.equals(transactionTag, that.transactionTag)
        && Objects.equals(shard, that.shard)
        && isRetryRecord == that.isRetryRecord;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        commitTimestamp,
        serverTransactionId,
        recordSequence,
        tableName,
        mod,
        modType,
        numberOfRecordsInTransaction,
        transactionTag,
        shard,
        isRetryRecord);
  }

  @Override
  public String toString() {
    return "TrimmedShardedDataChangeRecord{"
        + "commitTimestamp="
        + commitTimestamp
        + ", serverTransactionId='"
        + serverTransactionId
        + '\''
        + ", recordSequence='"
        + recordSequence
        + '\''
        + ", tableName='"
        + tableName
        + '\''
        + ", mod="
        + mod
        + ", modType="
        + modType
        + ", numberOfRecordsInTransaction="
        + numberOfRecordsInTransaction
        + ", transactionTag="
        + transactionTag
        + ", shard="
        + shard
        + ", isRetryRecord="
        + isRetryRecord
        + '}';
  }
}
