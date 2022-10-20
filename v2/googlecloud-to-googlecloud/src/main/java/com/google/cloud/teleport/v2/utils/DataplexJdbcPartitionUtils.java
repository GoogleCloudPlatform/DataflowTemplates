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
package com.google.cloud.teleport.v2.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.beam.sdk.values.KV;
import org.joda.time.ReadableInstant;

/**
 * Utility functions for Partitions in DataplexJdbcIngestion template.
 *
 * <p>Avro logical type "timestamp-millis" is supported for partitioning, see: <a
 * href="https://avro.apache.org/docs/current/spec.html#Logical+Types">Logical types</a>.
 */
public class DataplexJdbcPartitionUtils {

  private static final ImmutableMap<LogicalType, ZoneId> AVRO_DATE_TIME_LOGICAL_TYPES =
      ImmutableMap.of(
          LogicalTypes.timestampMillis(), ZoneOffset.UTC
          // TODO(olegsa) add "local-timestamp-millis" to ZoneId.systemDefault() mapping when Avro
          //  version is updated
          );

  /**
   * The granularity of partitioning.
   *
   * <p>Three levels of partitioning granularity are supported by providing {@link
   * PartitioningSchema}.
   */
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

    public List<KV<String, Integer>> toPartition(ZonedDateTime dateTime) {
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

    public List<String> getKeyNames() {
      ImmutableList.Builder<String> result = ImmutableList.builder();
      result.add("year");
      for (PartitioningSchema schema : PartitioningSchema.values()) {
        result.add(schema.label);
        if (this == schema) {
          break;
        }
      }
      return result.build();
    }
  }

  public static ZoneId getZoneId(Schema schema, String partitionColumnName) {
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
  public static long partitionColumnValueToMillis(Object value) {
    if (value instanceof Long) {
      return (Long) value;
    } else if (value instanceof ReadableInstant) {
      return ((ReadableInstant) value).getMillis();
    } else {
      throw new IllegalArgumentException(
          "The partition column value is an instance of unsupported class: " + value.getClass());
    }
  }

  /** Generates path from partition value. */
  public static String partitionToPath(List<KV<String, Integer>> partition) {
    StringBuilder result = new StringBuilder(64);
    for (KV<String, Integer> element : partition) {
      result.append(element.getKey()).append('=').append(element.getValue()).append('/');
    }
    return result.toString();
  }
}
