/*
 * Copyright (C) 2021 Google LLC
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

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.cloud.teleport.v2.utils.BigQueryToGcsDirectoryNaming;
import com.google.cloud.teleport.v2.utils.BigQueryToGcsFileNaming;
import com.google.cloud.teleport.v2.utils.Schemas;
import com.google.cloud.teleport.v2.values.BigQueryTable;
import com.google.cloud.teleport.v2.values.BigQueryTablePartition;
import com.google.cloud.teleport.v2.values.DataplexCompression;
import com.google.common.annotations.VisibleForTesting;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.Sink;
import org.apache.beam.sdk.io.FileIO.Write;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * Reads data from a BigQuery table, writes it to Cloud Storage, and outputs {@code
 * <BigQueryTablePartition, String>} pairs where the key is the exported BigQuery partition and the
 * value is the path of the corresponding file in Storage.
 *
 * <p>If the table is not partitioned, the partition key in the output will be @{@code null}.
 *
 * <p>See {@link FileFormat} for the list of supported output formats.
 */
public class BigQueryTableToGcsTransform
    extends PTransform<PBegin, PCollection<KV<BigQueryTablePartition, String>>> {

  private static final String PARTITION_COLUMN_RENAME_SUFFIX = "_pkey";

  private final BigQueryTable table;
  private final FileFormat outputFileFormat;
  private final DataplexCompression outputFileCompression;
  private final String targetRootPath;
  private final boolean enforceSamePartitionKey;
  private transient BigQueryServices testServices;

  public BigQueryTableToGcsTransform(
      BigQueryTable table,
      String targetRootPath,
      FileFormat outputFileFormat,
      DataplexCompression outputFileCompression,
      boolean enforceSamePartitionKey) {
    this.table = table;
    this.targetRootPath = targetRootPath;
    this.outputFileFormat = outputFileFormat;
    this.outputFileCompression = outputFileCompression;
    this.enforceSamePartitionKey = enforceSamePartitionKey;
  }

  @Override
  public PCollection<KV<BigQueryTablePartition, String>> expand(PBegin begin) {
    Schema targetFileSchema = table.getSchema();
    if (table.isPartitioned() && enforceSamePartitionKey) {
      // Apart from renaming the field in the schema we don't need to anything else (e.g. replace
      // the field in the actual GenericRecord being processed) because writers write fields
      // to the file based on their numeric position, not their name.
      targetFileSchema =
          Schemas.renameAvroField(
              targetFileSchema,
              table.getPartitioningColumn(),
              table.getPartitioningColumn() + PARTITION_COLUMN_RENAME_SUFFIX);
    }

    Sink<GenericRecord> sink;
    switch (outputFileFormat) {
      case PARQUET:
        sink =
            ParquetIO.sink(targetFileSchema)
                .withCompressionCodec(outputFileCompression.getParquetCodec());
        break;
      case AVRO:
        sink =
            AvroIO.<GenericRecord>sink(targetFileSchema)
                .withCodec(outputFileCompression.getAvroCodec());
        break;
      default:
        throw new UnsupportedOperationException(
            "Output format is not implemented: " + outputFileFormat);
    }

    BigQueryToGcsDirectoryNaming dn = new BigQueryToGcsDirectoryNaming(enforceSamePartitionKey);

    if (!table.isPartitioned()) {
      return transformTable(begin, sink, dn);
    }
    if (table.getPartitions() == null || table.getPartitions().isEmpty()) {
      throw new IllegalStateException(
          String.format(
              "Expected at least 1 partition for a partitioned table %s, but got none.",
              table.getTableName()));
    }

    List<PCollection<KV<BigQueryTablePartition, String>>> collections = new ArrayList<>();
    table.getPartitions().forEach(p -> collections.add(transformPartition(begin, sink, p, dn)));
    return PCollectionList.of(collections)
        .apply(tableNodeName("FlattenPartitionResults"), Flatten.pCollections());
  }

  private PCollection<KV<BigQueryTablePartition, String>> transformTable(
      PBegin begin, Sink<GenericRecord> sink, BigQueryToGcsDirectoryNaming directoryNaming) {
    String targetPath =
        String.format(
            "%s/%s", targetRootPath, directoryNaming.getTableDirectory(table.getTableName()));

    return begin
        .apply(tableNodeName("Read"), getDefaultRead().from(table.toTableReference()))
        .apply(
            tableNodeName("Write"),
            getDefaultWrite()
                .via(sink)
                .withNaming(
                    new BigQueryToGcsFileNaming(outputFileFormat.fileSuffix, table.getTableName()))
                .to(targetPath))
        .getPerDestinationOutputFilenames()
        .apply(
            tableNodeName("MapFileNames"),
            MapElements.into(TypeDescriptors.strings())
                .via((SerializableFunction<KV<Void, String>, String>) KV::getValue))
        .apply(
            tableNodeName("AttachPartitionKeys"),
            WithKeys.<BigQueryTablePartition, String>of((BigQueryTablePartition) null)
                .withKeyType(TypeDescriptor.of(BigQueryTablePartition.class)));
  }

  private PCollection<KV<BigQueryTablePartition, String>> transformPartition(
      PBegin begin,
      Sink<GenericRecord> sink,
      BigQueryTablePartition partition,
      BigQueryToGcsDirectoryNaming directoryNaming) {

    String sql =
        String.format(
            "select * from [%s.%s.%s$%s]",
            table.getProject(),
            table.getDataset(),
            table.getTableName(),
            partition.getPartitionName());

    String targetPath =
        String.format(
            "%s/%s",
            targetRootPath,
            directoryNaming.getPartitionDirectory(
                table.getTableName(), partition.getPartitionName(), table.getPartitioningColumn()));

    return begin
        .apply(partitionNodeName("Read", partition), getDefaultRead().fromQuery(sql))
        .apply(
            partitionNodeName("Write", partition),
            getDefaultWrite()
                .via(sink)
                .withNaming(
                    new BigQueryToGcsFileNaming(
                        outputFileFormat.fileSuffix,
                        table.getTableName(),
                        partition.getPartitionName()))
                .to(targetPath))
        .getPerDestinationOutputFilenames()
        .apply(
            partitionNodeName("MapFileNames", partition),
            MapElements.into(TypeDescriptors.strings())
                .via((SerializableFunction<KV<Void, String>, String>) KV::getValue))
        .apply(partitionNodeName("AttachPartitionKeys", partition), WithKeys.of(partition));
  }

  private TypedRead<GenericRecord> getDefaultRead() {
    TypedRead<GenericRecord> read =
        BigQueryIO.read(this::genericRecordWithFixedDates)
            .withTemplateCompatibility()
            // Performance hit due to validation is too big. When exporting a table with thousands
            // of partitions launching the job takes more than 12 minutes (Flex template timeout).
            .withoutValidation()
            // TODO: Switch to DIRECT_READ when the BigQueryIO bug is fixed.
            // There is probably a bug in BigQueryIO that causes "IllegalMutationException:
            // PTransform BigQueryIO.TypedRead/ParDo(Anonymous)/ParMultiDo(Anonymous) mutated
            // value ... after it was output" when using read() + DIRECT_READ + other conditions.
            .withMethod(TypedRead.Method.EXPORT)
            .withCoder(AvroCoder.of(table.getSchema()));

    return testServices == null ? read : read.withTestServices(testServices);
  }

  /**
   * When Beam's BigQueryIO reads from BQ it derives the Avro schema by itself, where it maps BQ's
   * `DATE` type to Avro's `string` type, so the GenericRecords outputed by the BigQueryIO contain
   * `string` fields for the `DATE` columns. The Avro schema obtained from the BQ directly -- {@code
   * table.getSchema()} has the `DATE` columns mapped to type Avro's `int` with logical type `date`.
   * To fix this mismatch this cmethod converts the `string` dates fields to `int` with logical type
   * `date` fields.
   *
   * <p>Note that for the TIMESTAMP type both Beam's BigQueryIO and BQ API map it to `long` so there
   * is no mismatch.
   */
  private GenericRecord genericRecordWithFixedDates(SchemaAndRecord schemaAndRecord) {
    GenericRecord input = schemaAndRecord.getRecord();
    GenericRecord output = new GenericData.Record(table.getSchema());
    for (TableFieldSchema fieldSchema : schemaAndRecord.getTableSchema().getFields()) {
      if ("DATE".equals(fieldSchema.getType())) {
        Object value = input.get(fieldSchema.getName());
        if (!(value instanceof CharSequence)) {
          throw new IllegalStateException(
              "The class of input value of type DATE is " + value.getClass());
        }
        output.put(fieldSchema.getName(), (int) LocalDate.parse((CharSequence) value).toEpochDay());
      } else {
        output.put(fieldSchema.getName(), input.get(fieldSchema.getName()));
      }
    }
    return output;
  }

  private Write<Void, GenericRecord> getDefaultWrite() {
    return FileIO.<GenericRecord>write()
        .withNumShards(1); // Must be 1 as we can only have 1 file per partition.
  }

  private String tableNodeName(String prefix) {
    return String.format("%s-T%s", prefix, table.getTableName());
  }

  private String partitionNodeName(String prefix, BigQueryTablePartition partition) {
    return String.format("%s-P%s", prefix, partition.getPartitionName());
  }

  @VisibleForTesting
  public BigQueryTableToGcsTransform withTestServices(BigQueryServices services) {
    this.testServices = services;
    return this;
  }

  /** Possible output file formats supported by {@link BigQueryTableToGcsTransform}. */
  public enum FileFormat {
    PARQUET(".parquet"),
    AVRO(".avro"),
    ORC(".orc");

    private final String fileSuffix;

    FileFormat(String fileSuffix) {
      this.fileSuffix = fileSuffix;
    }

    public String getFileSuffix() {
      return fileSuffix;
    }
  }

  /** Possible write disposition supported by {@link BigQueryTableToGcsTransform}. */
  public enum WriteDisposition {
    OVERWRITE("OVERWRITE"),
    SKIP("SKIP"),
    FAIL("FAIL");

    private final String writeDisposition;

    WriteDisposition(String writeDisposition) {
      this.writeDisposition = writeDisposition;
    }

    public String getWriteDisposition() {
      return writeDisposition;
    }
  }
}
