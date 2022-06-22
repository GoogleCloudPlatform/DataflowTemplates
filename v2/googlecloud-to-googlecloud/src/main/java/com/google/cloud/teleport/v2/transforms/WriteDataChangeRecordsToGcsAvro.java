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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.cloud.Timestamp;
import com.google.cloud.teleport.v2.io.WindowedFilenamePolicy;
import com.google.cloud.teleport.v2.utils.WriteToGCSUtility;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.TypeCode;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ValueCaptureType;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link WriteDataChangeRecordToGcsAvro} class is a {@link PTransform} that takes in {@link
 * PCollection} of Spanner data change records. The transform converts and writes these records to
 * GCS in avro file format.
 */
@AutoValue
public abstract class WriteDataChangeRecordsToGcsAvro
    extends PTransform<PCollection<DataChangeRecord>, PDone> {
  @VisibleForTesting protected static final String DEFAULT_OUTPUT_FILE_PREFIX = "output";
  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(WriteDataChangeRecordsToGcsAvro.class);

  public static WriteToGcsBuilder newBuilder() {
    return new AutoValue_WriteDataChangeRecordsToGcsAvro.Builder();
  }

  public abstract String gcsOutputDirectory();

  public abstract String outputFilenamePrefix();

  public abstract String tempLocation();

  public abstract Integer numShards();

  @Override
  public PDone expand(PCollection<DataChangeRecord> dataChangeRecords) {
    return dataChangeRecords
        /*
         * Writing as avro file using {@link AvroIO}.
         *
         * The {@link WindowedFilenamePolicy} class specifies the file path for writing the file.
         * The {@link withNumShards} option specifies the number of shards passed by the user.
         * The {@link withTempDirectory} option sets the base directory used to generate temporary files.
         */
        .apply("Transform to Avro", MapElements.via(new DataChangeRecordToAvroFn()))
        .apply(
            "Writing as Avro",
            AvroIO.write(com.google.cloud.teleport.v2.DataChangeRecord.class)
                .to(
                    new WindowedFilenamePolicy(
                        gcsOutputDirectory(),
                        outputFilenamePrefix(),
                        WriteToGCSUtility.SHARD_TEMPLATE,
                        WriteToGCSUtility.FILE_SUFFIX_MAP.get(WriteToGCSUtility.FileFormat.AVRO)))
                .withTempDirectory(
                    FileBasedSink.convertToFileResourceIfPossible(tempLocation())
                        .getCurrentDirectory())
                .withWindowedWrites()
                .withNumShards(numShards()));
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
      ValueCaptureType modType) {
    return com.google.cloud.teleport.v2.ValueCaptureType.OLD_AND_NEW_VALUES;
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

  static class DataChangeRecordToAvroFn
      extends SimpleFunction<DataChangeRecord, com.google.cloud.teleport.v2.DataChangeRecord> {
    @Override
    public com.google.cloud.teleport.v2.DataChangeRecord apply(DataChangeRecord record) {
      return dataChangeRecordToAvro(record);
    }
  }

  /**
   * The {@link WriteToGcsAvroOptions} interface provides the custom execution options passed by the
   * executor at the command-line.
   */
  public interface WriteToGcsAvroOptions extends PipelineOptions {
    @Description("The directory to output files to. Must end with a slash.")
    String getGcsOutputDirectory();

    void setGcsOutputDirectory(String gcsOutputDirectory);

    @Description(
        "The filename prefix of the files to write to. Default file prefix is set to \"output\".")
    @Default.String("output")
    String getOutputFilenamePrefix();

    void setOutputFilenamePrefix(String outputFilenamePrefix);

    @Description(
        "The maximum number of output shards produced when writing. Default number is runner"
            + " defined.")
    @Default.Integer(20)
    Integer getNumShards();

    void setNumShards(Integer numShards);
  }

  /** Builder for {@link WriteDataChangeRecordsToGcsAvro}. */
  @AutoValue.Builder
  public abstract static class WriteToGcsBuilder {
    abstract WriteToGcsBuilder setGcsOutputDirectory(String gcsOutputDirectory);

    abstract String gcsOutputDirectory();

    abstract WriteToGcsBuilder setTempLocation(String tempLocation);

    abstract String tempLocation();

    abstract WriteToGcsBuilder setOutputFilenamePrefix(String outputFilenamePrefix);

    abstract WriteToGcsBuilder setNumShards(Integer numShards);

    abstract WriteDataChangeRecordsToGcsAvro autoBuild();

    public WriteToGcsBuilder withGcsOutputDirectory(String gcsOutputDirectory) {
      checkArgument(
          gcsOutputDirectory != null,
          "withGcsOutputDirectory(gcsOutputDirectory) called with null input.");
      return setGcsOutputDirectory(gcsOutputDirectory);
    }

    public WriteToGcsBuilder withTempLocation(String tempLocation) {
      checkArgument(tempLocation != null, "withTempLocation(tempLocation) called with null input.");
      return setTempLocation(tempLocation);
    }

    public WriteToGcsBuilder withOutputFilenamePrefix(String outputFilenamePrefix) {
      if (outputFilenamePrefix == null) {
        LOG.info("Defaulting output filename prefix to: {}", DEFAULT_OUTPUT_FILE_PREFIX);
        outputFilenamePrefix = DEFAULT_OUTPUT_FILE_PREFIX;
      }
      return setOutputFilenamePrefix(outputFilenamePrefix);
    }

    public WriteDataChangeRecordsToGcsAvro build() {
      checkNotNull(gcsOutputDirectory(), "Provide output directory to write to. ");
      checkNotNull(tempLocation(), "Temporary directory needs to be provided. ");
      return autoBuild();
    }
  }
}
