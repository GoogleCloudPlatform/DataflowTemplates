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

import static com.google.common.collect.ImmutableList.toImmutableList;

import com.google.cloud.teleport.v2.io.AvroSinkWithJodaDatesConversion;
import com.google.cloud.teleport.v2.utils.DataplexJdbcIngestionNaming;
import com.google.cloud.teleport.v2.utils.DataplexJdbcPartitionUtils;
import com.google.cloud.teleport.v2.utils.DataplexJdbcPartitionUtils.PartitioningSchema;
import com.google.cloud.teleport.v2.utils.FileFormat.FileFormatOptions;
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.cloud.teleport.v2.values.DataplexPartitionMetadata;
import com.google.common.collect.ImmutableList;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.Sink;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} that partitions a collection of {@link GenericRecord} by datetime field and
 * writes the partitions to GCS. The transform outputs a collection of {@link
 * DataplexPartitionMetadata} for each partition.
 */
public class GenericRecordsToGcsPartitioned
    extends PTransform<PCollection<GenericRecord>, PCollection<DataplexPartitionMetadata>> {

  /* Logger for class. */
  private static final Logger LOG = LoggerFactory.getLogger(GenericRecordsToGcsPartitioned.class);

  private final String gcsPath;

  private final String serializedAvroSchema;

  @Nullable private final String partitionColumnName;

  @Nullable private final PartitioningSchema partitioningSchema;

  private final FileFormatOptions outputFileFormat;

  public GenericRecordsToGcsPartitioned(
      String gcsPath,
      String serializedAvroSchema,
      @Nullable String partitionColumnName,
      @Nullable PartitioningSchema partitioningSchema,
      FileFormatOptions outputFileFormat) {
    this.gcsPath = gcsPath;
    this.serializedAvroSchema = serializedAvroSchema;
    this.partitionColumnName = partitionColumnName;
    this.partitioningSchema = partitioningSchema;
    this.outputFileFormat = outputFileFormat;
  }

  @Override
  public PCollection<DataplexPartitionMetadata> expand(PCollection<GenericRecord> input) {
    Schema schema = SchemaUtils.parseAvroSchema(serializedAvroSchema);
    Sink<GenericRecord> sink;
    switch (outputFileFormat) {
      case PARQUET:
        sink = ParquetIO.sink(schema);
        break;
      case AVRO:
        sink = new AvroSinkWithJodaDatesConversion<>(schema);
        break;
      default:
        throw new UnsupportedOperationException(
            "Output format is not implemented: " + outputFileFormat);
    }

    if (partitionColumnName == null || partitioningSchema == null) {
      LOG.info(
          "PartitionColumnName or/and PartitioningSchema not provided. "
              + "Writing to GCS without partition");
      return input
          .apply(
              "Write to Storage with No Partition",
              FileIO.<GenericRecord>write()
                  .via(sink)
                  .to(gcsPath)
                  .withNaming(new DataplexJdbcIngestionNaming(outputFileFormat.getFileSuffix()))
                  .withNumShards(1)) // must be 1 as we can only have 1 file
          .getPerDestinationOutputFilenames()
          .apply(
              "MapFileNames",
              MapElements.into(TypeDescriptors.strings())
                  .via((SerializableFunction<KV<Void, String>, String>) KV::getValue))
          .apply(
              MapElements.via(
                  new SimpleFunction<String, DataplexPartitionMetadata>() {
                    @Override
                    public DataplexPartitionMetadata apply(String path) {
                      return DataplexPartitionMetadata.builder()
                          .setValues(ImmutableList.of("1"))
                          .setLocation(withoutFileName(path))
                          .build();
                    }
                  }));
    }

    ZoneId zoneId = DataplexJdbcPartitionUtils.getZoneId(schema, partitionColumnName);

    return input
        // partition and write files to GCS
        .apply(
            FileIO.<List<KV<String, Integer>>, GenericRecord>writeDynamic()
                // group values by the partition
                .by(
                    (GenericRecord r) ->
                        partitioningSchema.toPartition(
                            Instant.ofEpochMilli(
                                    DataplexJdbcPartitionUtils.partitionColumnValueToMillis(
                                        r.get(partitionColumnName)))
                                .atZone(zoneId)))
                // set the coder for the partition -- List<KV<String,String>>
                .withDestinationCoder(
                    ListCoder.of(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())))
                .via(sink)
                .to(gcsPath)
                .withNumShards(1) // must be 1 as we can only have 1 file per Dataplex partition
                // derive filenames from the partition and output file format
                .withNaming(
                    p ->
                        new DataplexJdbcIngestionNaming(
                            DataplexJdbcPartitionUtils.partitionToPath(p),
                            outputFileFormat.getFileSuffix())))
        // convert the WriteFilesResult<List<KV<String, Integer>>> to Dataplex partition metadata
        .getPerDestinationOutputFilenames()
        .apply(
            MapElements.via(
                new SimpleFunction<
                    KV<List<KV<String, Integer>>, String>, DataplexPartitionMetadata>() {
                  @Override
                  public DataplexPartitionMetadata apply(
                      KV<List<KV<String, Integer>>, String> partitionAndPath) {
                    if (partitionAndPath.getKey() == null) {
                      throw new IllegalStateException(
                          "Partition is null for path " + partitionAndPath.getValue());
                    }
                    if (partitionAndPath.getValue() == null) {
                      throw new IllegalStateException(
                          "Path is null for partition " + partitionAndPath.getKey());
                    }
                    return DataplexPartitionMetadata.builder()
                        .setValues(
                            partitionAndPath.getKey().stream()
                                .map(e -> String.valueOf(e.getValue()))
                                .collect(toImmutableList()))
                        .setLocation(withoutFileName(partitionAndPath.getValue()))
                        .build();
                  }
                }));
  }

  private static String withoutFileName(String gcsPath) {
    return gcsPath.substring(0, gcsPath.lastIndexOf('/'));
  }
}
