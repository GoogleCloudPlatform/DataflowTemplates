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
package com.google.cloud.teleport.v2.transforms;

import com.google.auto.value.AutoValue;
import com.google.cloud.Timestamp;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.TypeCode;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ValueCaptureType;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link WriteDataChangeRecordsToAvro} class is a {@link PTransform} that takes in {@link
 * PCollection} of Spanner data change records. The transform converts and writes these records to
 * GCS in Avro file format.
 */
@AutoValue
public class WriteDataChangeRecordsToAvro {
  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(WriteDataChangeRecordsToAvro.class);

  /**
   * The {@link DataChangeRecordToAvroFn} takes DataChangeRecord and convert it to
   * com.google.cloud.teleport.v2.DataChangeRecord.
   */
  public static class DataChangeRecordToAvroFn
      extends SimpleFunction<DataChangeRecord, com.google.cloud.teleport.v2.DataChangeRecord> {
    @Override
    public com.google.cloud.teleport.v2.DataChangeRecord apply(DataChangeRecord record) {
      return dataChangeRecordToAvro(record);
    }
  }

  private static long timestampToMicros(Timestamp ts) {
    return TimeUnit.SECONDS.toMicros(ts.getSeconds())
        + TimeUnit.NANOSECONDS.toMicros(ts.getNanos());
  }

  public static com.google.cloud.teleport.v2.DataChangeRecord dataChangeRecordToAvro(
      DataChangeRecord record) {
    String partitionToken = record.getPartitionToken();
    long commitTimestampMicros = timestampToMicros(record.getCommitTimestamp());
    String serverTransactionId = record.getServerTransactionId();
    boolean isLastRecordInTransaction = record.isLastRecordInTransactionInPartition();
    String recordSequence = record.getRecordSequence();
    String tableName = record.getTableName();
    List<com.google.cloud.teleport.v2.ColumnType> columnTypes =
        record.getRowType().stream()
            .map(
                columnType ->
                    new com.google.cloud.teleport.v2.ColumnType(
                        columnType.getName(),
                        mapTypeCodeToAvro(columnType.getType()),
                        columnType.isPrimaryKey(),
                        columnType.getOrdinalPosition()))
            .collect(Collectors.toList());

    List<com.google.cloud.teleport.v2.Mod> mods =
        record.getMods().stream()
            .map(
                mod ->
                    new com.google.cloud.teleport.v2.Mod(
                        mod.getKeysJson(),
                        mod.getOldValuesJson() != null ? mod.getOldValuesJson() : "",
                        mod.getNewValuesJson() != null ? mod.getNewValuesJson() : ""))
            .collect(Collectors.toList());

    com.google.cloud.teleport.v2.ModType modType = mapModTypeToModTypeAvro(record.getModType());
    com.google.cloud.teleport.v2.ValueCaptureType captureType =
        mapValueCaptureTypeToAvro(record.getValueCaptureType());
    long numberOfRecordsInTransaction = record.getNumberOfRecordsInTransaction();
    long numberOfPartitionsInTransaction = record.getNumberOfPartitionsInTransaction();

    com.google.cloud.teleport.v2.ChangeStreamRecordMetadata metadata =
        record.getMetadata() == null
            ? null
            : new com.google.cloud.teleport.v2.ChangeStreamRecordMetadata(
                record.getMetadata().getPartitionToken(),
                timestampToMicros(record.getMetadata().getRecordTimestamp()),
                timestampToMicros(record.getMetadata().getPartitionStartTimestamp()),
                timestampToMicros(record.getMetadata().getPartitionEndTimestamp()),
                timestampToMicros(record.getMetadata().getPartitionCreatedAt()),
                record.getMetadata().getPartitionScheduledAt() == null
                    ? 0
                    : timestampToMicros(record.getMetadata().getPartitionScheduledAt()),
                record.getMetadata().getPartitionRunningAt() == null
                    ? 0
                    : timestampToMicros(record.getMetadata().getPartitionRunningAt()),
                timestampToMicros(record.getMetadata().getQueryStartedAt()),
                timestampToMicros(record.getMetadata().getRecordStreamStartedAt()),
                timestampToMicros(record.getMetadata().getRecordStreamEndedAt()),
                timestampToMicros(record.getMetadata().getRecordReadAt()),
                record.getMetadata().getTotalStreamTimeMillis(),
                record.getMetadata().getNumberOfRecordsRead());

    // Add ChangeStreamMetadata
    return new com.google.cloud.teleport.v2.DataChangeRecord(
        partitionToken,
        commitTimestampMicros,
        serverTransactionId,
        isLastRecordInTransaction,
        recordSequence,
        tableName,
        columnTypes,
        mods,
        modType,
        captureType,
        numberOfRecordsInTransaction,
        numberOfPartitionsInTransaction,
        metadata);
  }

  private static com.google.cloud.teleport.v2.ModType mapModTypeToModTypeAvro(ModType modType) {
    switch (modType) {
      case INSERT:
        return com.google.cloud.teleport.v2.ModType.INSERT;
      case UPDATE:
        return com.google.cloud.teleport.v2.ModType.UPDATE;
      default:
        return com.google.cloud.teleport.v2.ModType.DELETE;
    }
  }

  private static com.google.cloud.teleport.v2.ValueCaptureType mapValueCaptureTypeToAvro(
      ValueCaptureType valueCaptureType) {
    switch (valueCaptureType) {
      case OLD_AND_NEW_VALUES:
        return com.google.cloud.teleport.v2.ValueCaptureType.OLD_AND_NEW_VALUES;
      case NEW_ROW:
        return com.google.cloud.teleport.v2.ValueCaptureType.NEW_ROW;
      default:
        return com.google.cloud.teleport.v2.ValueCaptureType.NEW_VALUES;
    }
  }

  private static com.google.cloud.teleport.v2.TypeCode mapTypeCodeToAvro(TypeCode typeCode) {
    switch (typeCode.getCode()) {
      case "{\"code\":\"BOOL\"}":
        return com.google.cloud.teleport.v2.TypeCode.BOOL;
      case "{\"code\":\"INT64\"}":
        return com.google.cloud.teleport.v2.TypeCode.INT64;
      case "{\"code\":\"FLOAT64\"}":
        return com.google.cloud.teleport.v2.TypeCode.FLOAT64;
      case "{\"code\":\"TIMESTAMP\"}":
        return com.google.cloud.teleport.v2.TypeCode.TIMESTAMP;
      case "{\"code\":\"DATE\"}":
        return com.google.cloud.teleport.v2.TypeCode.DATE;
      case "{\"code\":\"STRING\"}":
        return com.google.cloud.teleport.v2.TypeCode.STRING;
      case "{\"code\":\"BYTES\"}":
        return com.google.cloud.teleport.v2.TypeCode.BYTES;
      case "{\"code\":\"ARRAY\"}":
        return com.google.cloud.teleport.v2.TypeCode.ARRAY;
      case "{\"code\":\"STRUCT\"}":
        return com.google.cloud.teleport.v2.TypeCode.STRUCT;
      case "{\"code\":\"NUMERIC\"}":
        return com.google.cloud.teleport.v2.TypeCode.NUMERIC;
      case "{\"code\":\"JSON\"}":
        return com.google.cloud.teleport.v2.TypeCode.JSON;
      default:
        return com.google.cloud.teleport.v2.TypeCode.TYPE_CODE_UNSPECIFIED;
    }
  }
}
