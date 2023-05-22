/*
 * Copyright (C) 2023 Google LLC
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
package com.google.cloud.teleport.v2.templates.common;

import com.google.cloud.Timestamp;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;

/**
 * Trimmed version of the Apache Beam DataChangeRecord class that only contains the field we need in
 * this pipeline.
 */
@SuppressWarnings("initialization.fields.uninitialized") // Avro requires the default constructor
@DefaultCoder(value = AvroCoder.class)
public class TrimmedDataChangeRecord extends java.lang.Object implements Serializable {
  private Timestamp commitTimestamp;
  private String serverTransactionId;
  private String recordSequence;
  private String tableName;
  private List<Mod> mods;
  private ModType modType;
  private long numberOfRecordsInTransaction;
  private String transactionTag;

  public TrimmedDataChangeRecord(
      com.google.cloud.Timestamp commitTimestamp,
      String serverTransactionId,
      String recordSequence,
      String tableName,
      List<Mod> mods,
      ModType modType,
      long numberOfRecordsInTransaction,
      String transactionTag) {
    this.commitTimestamp = commitTimestamp;
    this.serverTransactionId = serverTransactionId;
    this.recordSequence = recordSequence;
    this.tableName = tableName;
    this.mods = mods;
    this.modType = modType;
    this.numberOfRecordsInTransaction = numberOfRecordsInTransaction;
    this.transactionTag = transactionTag;
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

  public List<Mod> getMods() {
    return mods;
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

  @Override
  public boolean equals(@javax.annotation.Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TrimmedDataChangeRecord)) {
      return false;
    }
    TrimmedDataChangeRecord that = (TrimmedDataChangeRecord) o;
    return Objects.equals(commitTimestamp, that.commitTimestamp)
        && Objects.equals(serverTransactionId, that.serverTransactionId)
        && Objects.equals(recordSequence, that.recordSequence)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(mods, that.mods)
        && modType == that.modType
        && numberOfRecordsInTransaction == that.numberOfRecordsInTransaction
        && Objects.equals(transactionTag, that.transactionTag);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        commitTimestamp,
        serverTransactionId,
        recordSequence,
        tableName,
        mods,
        modType,
        numberOfRecordsInTransaction,
        transactionTag);
  }

  @Override
  public String toString() {
    return "TrimmedDataChangeRecord{"
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
        + ", mods="
        + mods
        + ", modType="
        + modType
        + ", numberOfRecordsInTransaction="
        + numberOfRecordsInTransaction
        + ", transactionTag="
        + transactionTag
        + '}';
  }
}
