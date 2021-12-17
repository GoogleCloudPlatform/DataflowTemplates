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
import com.google.cloud.teleport.v2.utils.SchemaUtils;
import com.google.cloud.teleport.v2.values.PartitionMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.Sink;
import org.apache.beam.sdk.io.FileIO.Write;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.ReadableInstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} that partitions a collection of {@link GenericRecord} by datetime field and
 * writes the partitions to GCS. THe transform outputs a collection of {@link PartitionMetadata} for
 * each partition.
 *
 * <p>Three levels of partitioning granularity are supported by providing {@link
 * PartitioningSchema}.
 *
 * <p>Avro logical type "timestamp-millis" is supported for partitioning, see: <a
 * href="https://avro.apache.org/docs/current/spec.html#Logical+Types">Logical types</a>.
 */
public class GenericRecordsToGcsPartitioned
    extends PTransform<PCollection<GenericRecord>, PCollection<PartitionMetadata>> {

  /* Logger for class.*/
  private static final Logger LOG = LoggerFactory.getLogger(GenericRecordsToGcsPartitioned.class);

  private static final ImmutableMap<LogicalType, ZoneId> AVRO_DATE_TIME_LOGICAL_TYPES =
      ImmutableMap.of(
          LogicalTypes.timestampMillis(), ZoneOffset.UTC
          // TODO(olegsa) add "local-timestamp-millis" to ZoneId.systemDefault() mapping when Avro
          //  version is updated
          );

  /** The granularity of partitioning. */
  public enum PartitioningSchema {
    MONTHLY("month", ZonedDateTime::getMonthValue),
    DAILY("day", ZonedDateTime::getDayOfMonth),
    HOURLY("hour", ZonedDateTime::getHour);

    private final String label;
    private final Function<ZonedDateTime, Integer> dateTimeToPartition;

    PartitioningSchema(String label, Function<ZonedDateTime, Integer> dateTimeToPartition) {
      this.label = label;
      this.dateTimeToPartition = dateTimeToPartition;
    }

    private List<KV<String, Integer>> toPartition(ZonedDateTime dateTime) {
      ImmutableList.Builder<KV<String, Integer>> result = ImmutableList.builder();
      result.add(KV.of("year", dateTime.getYear()));
      for (PartitioningSchema schema : PartitioningSchema.values()) {
        result.add(KV.of(schema.label, schema.dateTimeToPartition.apply(dateTime)));
        if (this == schema) {
          break;
        }
      }
      return result.build();
    }
  }

  private final String gcsPath;

  private final String serializedAvroSchema;

  @Nullable private final String partitionColumnName;

  @Nullable private final PartitioningSchema partitioningSchema;

  private final OutputFileFormat outputFileFormat;

  public GenericRecordsToGcsPartitioned(
      String gcsPath,
      String serializedAvroSchema,
      @Nullable String partitionColumnName,
      @Nullable PartitioningSchema partitioningSchema,
      OutputFileFormat outputFileFormat) {
    this.gcsPath = gcsPath;
    this.serializedAvroSchema = serializedAvroSchema;
    this.partitionColumnName = partitionColumnName;
    this.partitioningSchema = partitioningSchema;
    this.outputFileFormat = outputFileFormat;
  }

  @Override
  public PCollection<PartitionMetadata> expand(PCollection<GenericRecord> input) {
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
                  .withSuffix(outputFileFormat.fileSuffix)
                  .via(sink)
                  .to(gcsPath))
          // Dummy conversion to Dataplex partition metadata
          // TODO(weiwenxu) Change after Dataplex metadata update is enabled
          .getPerDestinationOutputFilenames()
          .apply(
              "MapFileNames",
              MapElements.into(TypeDescriptors.strings())
                  .via((SerializableFunction<KV<Void, String>, String>) KV::getValue))
          .apply(
              MapElements.via(
                  new SimpleFunction<String, PartitionMetadata>() {
                    @Override
                    public PartitionMetadata apply(String path) {
                      return PartitionMetadata.builder()
                          .setValues(ImmutableList.of("1"))
                          .setLocation(withoutFileName(path))
                          .build();
                    }
                  }));
    }

    ZoneId zoneId = getZoneId(schema);

    return input
        // partition and write files to GCS
        .apply(
            FileIO.<List<KV<String, Integer>>, GenericRecord>writeDynamic()
                // group values by the partition
                .by(
                    (GenericRecord r) ->
                        partitioningSchema.toPartition(
                            Instant.ofEpochMilli(
                                    partitionColumnValueToMillis(r.get(partitionColumnName)))
                                .atZone(zoneId)))
                // set the coder for the partition -- List<KV<String,String>>
                .withDestinationCoder(
                    ListCoder.of(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of())))
                .via(sink)
                .to(gcsPath)
                .withNumShards(1) // must be 1 as we can only have 1 file per Dataplex partition
                // derive filenames from the partition and output file format
                .withNaming(
                    p -> Write.defaultNaming(partitionToPath(p), outputFileFormat.getFileSuffix())))
        // convert the WriteFilesResult<List<KV<String, Integer>>> to Dataplex partition metadata
        .getPerDestinationOutputFilenames()
        .apply(
            MapElements.via(
                new SimpleFunction<KV<List<KV<String, Integer>>, String>, PartitionMetadata>() {
                  @Override
                  public PartitionMetadata apply(
                      KV<List<KV<String, Integer>>, String> partitionAndPath) {
                    if (partitionAndPath.getKey() == null) {
                      throw new IllegalStateException(
                          "Partition is null for path " + partitionAndPath.getValue());
                    }
                    if (partitionAndPath.getValue() == null) {
                      throw new IllegalStateException(
                          "Path is null for partition " + partitionAndPath.getKey());
                    }
                    return PartitionMetadata.builder()
                        .setValues(
                            partitionAndPath.getKey().stream()
                                .map(e -> String.valueOf(e.getValue()))
                                .collect(toImmutableList()))
                        .setLocation(withoutFileName(partitionAndPath.getValue()))
                        .build();
                  }
                }));
  }

  private ZoneId getZoneId(Schema schema) {
    Schema partitionFieldType = schema.getField(partitionColumnName).schema();
    // check if the partition field is nullable, inspired by {@code Schema.isNullable()} of Avro 1.9
    if (schema.getType() == Schema.Type.UNION) {
      partitionFieldType =
          partitionFieldType.getTypes().stream()
              .filter(t -> t.getType() != Schema.Type.NULL)
              .findFirst()
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          String.format(
                              "Partition field %s is of unsupported type: %s",
                              partitionColumnName, schema.getField(partitionColumnName).schema())));
    }

    // get zone according to the logical-type if there is no logical-type assume UTC time-zone
    ZoneId zoneId =
        AVRO_DATE_TIME_LOGICAL_TYPES.getOrDefault(
            partitionFieldType.getLogicalType(), ZoneOffset.UTC);
    if (zoneId == null) {
      throw new IllegalArgumentException(
          String.format(
              "Partition field `%s` is of an unsupported type: %s, supported types are `long` types"
                  + " with logical types: %s",
              partitionColumnName,
              partitionFieldType,
              AVRO_DATE_TIME_LOGICAL_TYPES.keySet().stream()
                  .map(LogicalType::getName)
                  .collect(Collectors.joining(", "))));
    }
    return zoneId;
  }

  /**
   * This method is used to address the static initialization in
   * org.apache.beam.sdk.schemas.utils.AvroUtils static initialization.
   *
   * <p>A usage of AvroUtils changes how Avro treats `timestamp-millis` "globally", and so if
   * AvroUtils is used, even in a unrelated classes, the `timestamp-millis` is returned as Joda
   * timestamps, and if AvroUtils is not used `timestamp-millis` is returned as long. This method
   * handles both cases and returns long millis.
   */
  private static long partitionColumnValueToMillis(Object value) {
    if (value instanceof Long) {
      return (Long) value;
    } else if (value instanceof ReadableInstant) {
      return ((ReadableInstant) value).getMillis();
    } else {
      throw new IllegalArgumentException(
          "The partition column value is an instance of unsupported class: " + value.getClass());
    }
  }

  private static String partitionToPath(List<KV<String, Integer>> partition) {
    StringBuilder result = new StringBuilder(64);
    for (KV<String, Integer> element : partition) {
      result.append(element.getKey()).append('=').append(element.getValue()).append('/');
    }
    return result.toString();
  }

  private static String withoutFileName(String gcsPath) {
    return gcsPath.substring(0, gcsPath.lastIndexOf('/'));
  }

  /** Supported output file formats. */
  public enum OutputFileFormat {
    PARQUET(".parquet"),
    AVRO(".avro"),
    ORC(".orc");

    private final String fileSuffix;

    OutputFileFormat(String fileSuffix) {
      this.fileSuffix = fileSuffix;
    }

    public String getFileSuffix() {
      return fileSuffix;
    }
  }
}
